(ns onyx.api
  (:require [clojure.string :refer [split]]
            [clojure.core.async :refer [chan alts!! >!! <!! close! alts!! timeout go]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [warn fatal error]]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.system :as system]
            [onyx.extensions :as extensions]
            [onyx.static.validation :as validator]
            [onyx.static.planning :as planning]))

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

(defn ^{:no-doc true} create-submit-job-entry [id config job tasks]
  (let [task-ids (map :id tasks)
        scheduler (:task-scheduler job)
        sat (saturation (:catalog job))
        task-saturation (task-saturation (:catalog job) tasks)
        min-reqs (min-required-peers (:catalog job) tasks)
        task-flux-policies (flux-policies (:catalog job) tasks)
        input-task-ids (find-input-tasks (:catalog job) tasks)
        output-task-ids (find-output-tasks (:catalog job) tasks)
        exempt-task-ids (find-exempt-tasks tasks (:acker/exempt-tasks job))
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
              :acker-percentage (or (:acker/percentage job) 1)
              :acker-exclude-inputs (or (:acker/exempt-input-tasks? job) false)
              :acker-exclude-outputs (or (:acker/exempt-output-tasks? job) false)}
        args (add-percentages-to-log-entry config job args tasks (:catalog job) id)]
    (create-log-entry :submit-job args)))

(defn ^{:added "0.6.0"} submit-job
  "Takes a peer configuration, job map, and optional monitoring config,
   sending the job to the cluster for eventual execution."
  ([peer-config job]
     (submit-job peer-config job {:monitoring :no-op}))
  ([peer-config job monitoring-config]
     (try (validator/validate-peer-config peer-config)
          (validator/validate-job (assoc job :workflow (:workflow job)))
          (validator/validate-flow-conditions (:flow-conditions job) (:workflow job))
          (validator/validate-lifecycles (:lifecycles job) (:catalog job))
          (catch Throwable t
            (error t)
            (throw t)))
     (let [id (java.util.UUID/randomUUID)
           tasks (planning/discover-tasks (:catalog job) (:workflow job))
           entry (create-submit-job-entry id peer-config job tasks)
           client (component/start (system/onyx-client peer-config monitoring-config))]
       (extensions/write-chunk (:log client) :catalog (:catalog job) id)
       (extensions/write-chunk (:log client) :workflow (:workflow job) id)
       (extensions/write-chunk (:log client) :flow-conditions (:flow-conditions job) id)
       (extensions/write-chunk (:log client) :lifecycles (:lifecycles job) id)

       (doseq [task tasks]
         (extensions/write-chunk (:log client) :task task id))

       (extensions/write-log-entry (:log client) entry)
       (component/stop client)
       {:job-id id
        :task-ids tasks})))

(defn ^{:added "0.6.0"} kill-job
  "Kills a currently executing job, given it's job ID. All peers executing
   tasks for this job cleanly stop executing and volunteer to work on other jobs.
   Task lifecycle APIs for closing tasks are invoked. This job is never again scheduled
   for execution."
  ([peer-config job-id]
     (kill-job peer-config job-id {:monitoring :no-op}))
  ([peer-config job-id monitoring-config]
     (when (nil? job-id)
       (throw (ex-info "Invalid job id" {:job-id job-id})))
     (let [client (component/start (system/onyx-client peer-config monitoring-config))
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
  ([peer-config ch]
     (subscribe-to-log peer-config ch {:monitoring :no-op}))
  ([peer-config ch monitoring-config]
     (let [env (component/start (system/onyx-client peer-config monitoring-config))]
       {:replica (extensions/subscribe-to-log (:log env) ch)
        :env env})))

(defn ^{:added "0.6.0"} gc
  "Invokes the garbage collector on Onyx. Compresses all local replicas
   for peers, decreasing memory usage. Also deletes old log entries from
   ZooKeeper, freeing up disk space.

   Local replicas clear out all data about completed and killed jobs -
   as if they never existed."
  ([peer-config]
   (gc peer-config {:monitoring :no-op}))
  ([peer-config monitoring-config]
   (let [id (java.util.UUID/randomUUID)
           client (component/start (system/onyx-client peer-config monitoring-config))
           entry (create-log-entry :gc {:id id})
           ch (chan 1000)]
       (extensions/write-log-entry (:log client) entry)

       (loop [replica (extensions/subscribe-to-log (:log client) ch)]
         (let [entry (<!! ch)
               new-replica (extensions/apply-log-entry entry replica)]
           (if (and (= (:fn entry) :gc) (= (:id (:args entry)) id))
             (let [diff (extensions/replica-diff entry replica new-replica)]
               (extensions/fire-side-effects! entry replica new-replica diff {:id id :log (:log client)}))
             (recur new-replica))))
       (component/stop client)
       true)))

(defn ^{:added "0.6.0"} await-job-completion
  "Blocks until job-id has had all of its tasks completed or the job is killed.
   Returns true if the job completed successfully, false if the job was killed."
  ([peer-config job-id monitoring-config timeout-ms]
   (let [job-id (validator/coerce-uuid job-id)
         client (component/start (system/onyx-client peer-config monitoring-config))
         ch (chan 100)
         tmt (timeout timeout-ms)]
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

(defn ^{:no-doc true} peer-lifecycle [started-peer config shutdown-ch ack-ch]
  (try
    (loop [live @started-peer]
      (let [restart-ch (:restart-ch (:virtual-peer live))
            [v ch] (alts!! [shutdown-ch restart-ch] :priority true)]
        (cond (= ch shutdown-ch)
              (do (component/stop live)
                  (reset! started-peer nil)
                  (>!! ack-ch true))
              (= ch restart-ch)
              (do (component/stop live)
                  (Thread/sleep (or (:onyx.peer/retry-start-interval config) 2000))
                  (let [live (component/start live)]
                    (reset! started-peer live)
                    (recur live)))
              :else (throw (ex-info "Read from a channel with no response implementation" {})))))
    (catch Throwable e
      (fatal "Peer lifecycle threw an exception")
      (fatal e))))

(defn ^{:added "0.6.0"} start-peers
  "Launches n virtual peers. Each peer may be stopped
   by passing it to the shutdown-peer function. Optionally takes
   a 3rd argument - a monitoring configuration map. See the User Guide
   for details."
  ([n peer-group]
     (start-peers n peer-group {:monitoring :no-op}))
  ([n {:keys [config] :as peer-group} monitoring-config]
     (when-not (= (type peer-group) onyx.system.OnyxPeerGroup)
       (throw (Exception. (str "start-peers must supplied with a peer-group not a " (type peer-group)))))
     (doall
      (map
       (fn [_]
         (let [v-peer (system/onyx-peer peer-group monitoring-config)
               live (component/start v-peer)
               shutdown-ch (chan 1)
               ack-ch (chan)
               started-peer (atom live)]
           {:peer-lifecycle (future (peer-lifecycle started-peer config shutdown-ch ack-ch))
            :started-peer started-peer
            :shutdown-ch shutdown-ch
            :ack-ch ack-ch}))
       (range n)))))

(defn ^{:added "0.6.0"} shutdown-peer
  "Shuts down the virtual peer, which releases all of its resources
   and removes it from the execution of any tasks. This peer will
   not longer volunteer for tasks."
  [peer]
  (>!! (:shutdown-ch peer) true)
  (<!! (:ack-ch peer))
  (close! (:shutdown-ch peer))
  (close! (:ack-ch peer)))

(defn ^{:added "0.6.0"} start-env
  "Starts a development environment using an in-memory implementation ZooKeeper."
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
  "Starts a peer group for use in cases where an env is not started (e.g. distributed mode)"
  [peer-config]
  (validator/validate-peer-config peer-config)
  (component/start (system/onyx-peer-group peer-config)))

(defn ^{:added "0.6.0"} shutdown-peer-group
  "Shuts down the given peer-group"
  [peer-group]
  (component/stop peer-group))
