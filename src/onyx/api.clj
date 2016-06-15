(ns onyx.api
  (:require [clojure.core.async :refer [chan >!! <!! close! alts!! timeout go promise-chan]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info warn fatal error]]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.system :as system]
            [onyx.extensions :as extensions]
            [onyx.static.validation :as validator]
            [onyx.static.planning :as planning]
            [onyx.static.default-vals :refer [arg-or-default]])
  (:import [java.util UUID]
           [java.security MessageDigest]))

(defn ^{:no-doc true} saturation [catalog]
  (let [rets
        (reduce #(+ %1 (or (:onyx/max-peers %2)
                           Double/POSITIVE_INFINITY))
                0
                catalog)]
    (if (zero? rets)
      Double/POSITIVE_INFINITY
      rets)))

(defn ^{:no-doc true} task-saturation [catalog tasks]
  (into
   {}
   (map
    (fn [task]
      {(:id task)
       (or (:onyx/max-peers (planning/find-task catalog (:name task)))
           Double/POSITIVE_INFINITY)})
    tasks)))

(defn ^{:no-doc true} min-required-peers [catalog tasks]
  (into
   {}
   (map
    (fn [task]
      {(:id task)
       (or (:onyx/min-peers (planning/find-task catalog (:name task))) 1)})
    tasks)))

(defn ^{:no-doc true} required-tags [catalog tasks]
  (reduce
   (fn [result [catalog-entry task]]
     (if-let [tags (:onyx/required-tags catalog-entry)]
       (assoc result (:id task) tags)
       result))
   {}
   (map vector catalog tasks)))

(defn ^{:no-doc true} flux-policies [catalog tasks]
  (->> tasks
       (map (fn [task]
              (vector (:id task)
                      (:onyx/flux-policy (planning/find-task catalog (:name task))))))
       (filter second)
       (into {})))

(defn ^{:added "0.6.0"} map-set-workflow->workflow
  "Converts a workflow in format:
   {:a #{:b :c}
    :b #{:d}}
   to format:
   [[:a :b]
    [:a :c]
    [:b :d]]"
  [workflow]
  (vec
   (reduce-kv (fn [w k v]
                (concat w
                        (map (fn [t] [k t]) v)))
              []
              workflow)))

(defn ^{:no-doc true} add-job-percentage [config job args]
  (if (= (:onyx.peer/job-scheduler config) :onyx.job-scheduler/percentage)
    (assoc args :percentage (:percentage job))
    args))

(defn ^{:no-doc true} task-id->pct [catalog task]
  (let [task-map (planning/find-task catalog (:name task))]
    {(:id task) (:onyx/percentage task-map)}))

(defn ^{:no-doc true} add-task-percentage [args job-id tasks catalog]
  (if (= (:task-scheduler args) :onyx.task-scheduler/percentage)
    (assoc-in args
              [:task-percentages]
              (into {} (map (fn [t] (task-id->pct catalog t)) tasks)))
    args))

(defn ^{:no-doc true} add-percentages-to-log-entry
  [config job args tasks catalog job-id]
  (let [job-updated (add-job-percentage config job args)]
    (add-task-percentage job-updated job-id tasks catalog)))

(defn ^{:no-doc true} find-input-tasks [catalog tasks]
  (map :id (filter (fn [task]
                     (let [task (planning/find-task catalog (:name task))]
                       (= :input (:onyx/type task))))
                   tasks)))

(defn ^{:no-doc true} find-output-tasks [catalog tasks]
  (map :id (filter (fn [task]
                     (let [task (planning/find-task catalog (:name task))]
                       (= :output (:onyx/type task))))
                   tasks)))

(defn ^{:no-doc true} find-exempt-tasks [tasks exempt-task-names]
  (let [exempt-set (into #{} exempt-task-names)
        exempt-tasks (filter (fn [task] (some #{(:name task)} exempt-set)) tasks)]
    (map :id exempt-tasks)))

(defn ^{:no-doc true} expand-n-peers [catalog]
  (mapv
   (fn [entry]
     (if-let [n (:onyx/n-peers entry)]
       (assoc entry :onyx/min-peers n :onyx/max-peers n)
       entry))
   catalog))

(defn ^{:no-doc true} create-submit-job-entry [id config job tasks]
  (let [task-ids (map :id tasks)
        job (update job :catalog expand-n-peers)
        scheduler (:task-scheduler job)
        sat (saturation (:catalog job))
        task-saturation (task-saturation (:catalog job) tasks)
        min-reqs (min-required-peers (:catalog job) tasks)
        task-flux-policies (flux-policies (:catalog job) tasks)
        input-task-ids (find-input-tasks (:catalog job) tasks)
        output-task-ids (find-output-tasks (:catalog job) tasks)
        exempt-task-ids (find-exempt-tasks tasks (:acker/exempt-tasks job))
        required-tags (required-tags (:catalog job) tasks)
        args {:id id
              :tasks task-ids
              :task-scheduler scheduler
              :saturation sat
              :task-saturation task-saturation
              :min-required-peers min-reqs
              :flux-policies task-flux-policies
              :inputs input-task-ids
              :outputs output-task-ids
              :exempt-tasks exempt-task-ids
              :required-tags required-tags
              :acker-percentage (or (:acker/percentage job) 1)
              :acker-exclude-inputs (or (:acker/exempt-input-tasks? job) false)
              :acker-exclude-outputs (or (:acker/exempt-output-tasks? job) false)}
        args (add-percentages-to-log-entry config job args tasks (:catalog job) id)]
    (create-log-entry :submit-job args)))

(defn validate-submission [job peer-client-config]
  (try
    (validator/validate-peer-client-config peer-client-config)
    (validator/validate-job job)
    {:success? true}
    (catch Throwable t
      (if-let [data (ex-data t)]
        (cond (and (:helpful-failed? data) (:e data))
              (throw (:e data))

              (:e data) {:success? false :e (:e data)}

              (:manual? data) {:success? false}

              :else (throw t))
        (throw t)))))

(defn ^{:no-doc true} hash-job [job]
  ;; Sort the keys of the job to get a consistent hash
  ;; in case the keys are in a different order.
  (let [sorted-job (into (sorted-map) job)
        md (MessageDigest/getInstance "SHA-256")]
    (.update md (.getBytes (pr-str sorted-job) "UTF-8"))
    (let [digest (.digest md)]
      (apply str (map #(format "%x" %) digest)))))

(defn ^{:no-doc true} serialize-job-to-zookeeper [client job-id job tasks entry]
  (extensions/write-chunk (:log client) :catalog (:catalog job) job-id)
  (extensions/write-chunk (:log client) :workflow (:workflow job) job-id)
  (extensions/write-chunk (:log client) :flow-conditions (:flow-conditions job) job-id)
  (extensions/write-chunk (:log client) :lifecycles (:lifecycles job) job-id)
  (extensions/write-chunk (:log client) :windows (:windows job) job-id)
  (extensions/write-chunk (:log client) :triggers (:triggers job) job-id)
  (extensions/write-chunk (:log client) :job-metadata (:metadata job) job-id)

  (doseq [task tasks]
    (extensions/write-chunk (:log client) :task task job-id))
  (extensions/write-log-entry (:log client) entry)
  (component/stop client)
  {:success? true
   :job-id job-id})

(defn ^{:added "0.6.0"} submit-job
  "Takes a peer configuration, job map, and optional monitoring config,
   sending the job to the cluster for eventual execution. Returns a map
   with :success? indicating if the job was submitted to ZooKeeper. The job map
   may contain a :metadata key, among other keys described in the user
   guide. The :metadata key may optionally supply a :job-id value. Repeated
   submissions of a job with the same :job-id will be treated as an idempotent
   action. If a job has been submitted more than once, the original task IDs
   associated with the catalog will be returned, and the job will not run again,
   even if it has been killed or completed. It is undefined behavior to submit
   two jobs with the same :job-id metadata whose :workflow, :catalog,
   :flow-conditions,etc are not equal."
  ([peer-client-config job]
   (submit-job peer-client-config job {:monitoring :no-op}))
  ([peer-client-config job monitoring-config]
   (let [result (validate-submission job peer-client-config)]
     (if (:success? result)
       (let [job-hash (hash-job job)
             job (-> job 
                     (update-in [:metadata :job-id] #(or % (UUID/randomUUID)))
                     (assoc-in [:metadata :job-hash] job-hash))
             id (get-in job [:metadata :job-id])
             tasks (planning/discover-tasks (:catalog job) (:workflow job))
             entry (create-submit-job-entry id peer-client-config job tasks)
             client (component/start (system/onyx-client peer-client-config monitoring-config))
             status (extensions/write-chunk (:log client) :job-hash job-hash id)]
         (if status
           (serialize-job-to-zookeeper client id job tasks entry)
           (let [written-hash (extensions/read-chunk (:log client) :job-hash id)]
             (if (= written-hash job-hash)
               (serialize-job-to-zookeeper client id job tasks entry)
               {:cause :incorrect-job-hash
                :success? false}))))
       result))))

(defn ^{:added "0.6.0"} kill-job
  "Kills a currently executing job, given its job ID. All peers executing
   tasks for this job cleanly stop executing and volunteer to work on other jobs.
   Task lifecycle APIs for closing tasks are invoked. This job is never again scheduled
   for execution."
  ([peer-client-config job-id]
   (kill-job peer-client-config job-id {:monitoring :no-op}))
  ([peer-client-config job-id monitoring-config]
   (validator/validate-peer-client-config peer-client-config)
   (when (nil? job-id)
     (throw (ex-info "Invalid job id" {:job-id job-id})))
   (let [client (component/start (system/onyx-client peer-client-config monitoring-config))
         entry (create-log-entry :kill-job {:job (validator/coerce-uuid job-id)})]
     (extensions/write-log-entry (:log client) entry)
     (component/stop client)
     true)))

(defn ^{:added "0.6.0"} subscribe-to-log
  "Sends all events from the log to the provided core.async channel.
   Starts at the origin of the log and plays forward monotonically.

   Returns a map with keys :replica and :env. :replica contains the origin
   replica. :env contains an Component with a :log connection to ZooKeeper,
   convenient for directly querying the znodes. :env can be shutdown with
   the onyx.api/shutdown-env function"
  ([peer-client-config ch]
   (subscribe-to-log peer-client-config ch {:monitoring :no-op}))
  ([peer-client-config ch monitoring-config]
   (validator/validate-peer-client-config peer-client-config)
   (let [env (component/start (system/onyx-client peer-client-config monitoring-config))]
     {:replica (extensions/subscribe-to-log (:log env) ch)
      :env env})))

(defn ^{:added "0.6.0"} gc
  "Invokes the garbage collector on Onyx. Compresses all local replicas
   for peers, decreasing memory usage. Also deletes old log entries from
   ZooKeeper, freeing up disk space.

   Local replicas clear out all data about completed and killed jobs -
   as if they never existed."
  ([peer-client-config]
   (gc peer-client-config {:monitoring :no-op}))
  ([peer-client-config monitoring-config]
   (validator/validate-peer-client-config peer-client-config)
   (let [id (java.util.UUID/randomUUID)
         client (component/start (system/onyx-client peer-client-config monitoring-config))
         entry (create-log-entry :gc {:id id})
         ch (chan 1000)]
     (extensions/write-log-entry (:log client) entry)

     (loop [replica (extensions/subscribe-to-log (:log client) ch)]
       (let [entry (<!! ch)
             new-replica (extensions/apply-log-entry entry replica)]
         (if (and (= (:fn entry) :gc) (= (:id (:args entry)) id))
           (let [diff (extensions/replica-diff entry replica new-replica)]
             (extensions/fire-side-effects! entry replica new-replica diff {:id id :type :client :log (:log client)}))
           (recur new-replica))))
     (component/stop client)
     true)))

(defn ^{:added "0.7.4"} await-job-completion
  "Blocks until job-id has had all of its tasks completed or the job is killed.
   Returns true if the job completed successfully, false if the job was killed."
  ([peer-client-config job-id]
   (await-job-completion peer-client-config job-id {:monitoring-config {:monitoring :no-op}}))
  ([peer-client-config job-id {:keys [monitoring-config timeout-ms]}]
   (validator/validate-peer-client-config peer-client-config)
   (let [job-id (validator/coerce-uuid job-id)
         client (component/start (system/onyx-client peer-client-config monitoring-config))
         ch (chan 100)
         tmt (if timeout-ms (timeout timeout-ms) (chan))]
     (loop [replica (extensions/subscribe-to-log (:log client) ch)]
       (let [[v c] (alts!! [(go (extensions/apply-log-entry (<!! ch) replica))
                            tmt]
                           :priority true)]
         (cond (some #{job-id} (:completed-jobs v))
               (do (component/stop client)
                   true)
               (some #{job-id} (:killed-jobs v))
               (do (component/stop client)
                   false)
               (= c tmt)
               (do (component/stop client)
                   :timeout)
               :else
               (recur v)))))))

(defn ^{:added "0.6.0"} start-peers
  "Launches n virtual peers. Each peer may be stopped by passing it to the shutdown-peer function."
  [n peer-group]
  (validator/validate-java-version)
  (mapv
   (fn [_]
     (let [group-ch (:group-ch (:peer-group-manager peer-group))
           peer-owner-id (java.util.UUID/randomUUID)]
       (>!! group-ch [:add-peer peer-owner-id])
       {:group-ch group-ch :peer-owner-id peer-owner-id}))
   (range n)))

(defn ^{:added "0.6.0"} shutdown-peer
  "Shuts down the virtual peer, which releases all of its resources
   and removes it from the execution of any tasks. This peer will
   no longer volunteer for tasks. Returns nil."
  [peer]
  (>!! (:group-ch peer) [:remove-peer (:peer-owner-id peer)]))

(defn ^{:added "0.8.1"} shutdown-peers
  "Like shutdown-peer, but takes a sequence of peers as an argument,
   shutting each down in order. Returns nil."
  [peers]
  (doseq [p peers]
    (shutdown-peer p)))

(defn ^{:added "0.6.0"} start-env
  "Starts a development environment using an in-memory implementation of ZooKeeper."
  ([env-config]
   (start-env env-config {:monitoring :no-op}))
  ([env-config monitoring-config]
   (validator/validate-env-config env-config)
   (component/start (system/onyx-development-env env-config monitoring-config))))

(defn ^{:added "0.6.0"} shutdown-env
  "Shuts down the given development environment."
  [env]
  (component/stop env))

(defn ^{:added "0.6.0"} start-peer-group
  "Starts a set of shared resources that are used across all virtual peers on this machine.
   Optionally takes a monitoring configuration map. See the User Guide for details."
  ([peer-config]
   (start-peer-group peer-config {:monitoring :no-op}))
  ([peer-config monitoring-config]
   (validator/validate-java-version)
   (validator/validate-peer-config peer-config)
   (component/start
    (system/onyx-peer-group peer-config monitoring-config))))

(defn ^{:added "0.6.0"} shutdown-peer-group
  "Shuts down the given peer-group"
  [peer-group]
  (component/stop peer-group))
