(ns onyx.api
  (:require [clojure.string :refer [split]]
            [clojure.core.async :refer [chan alts!! >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [warn fatal]]
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

(defn ^{:added "0.6.0"} map-set-workflow->workflow
  "Converts a workflow in format:
   {:a #{:b :c}
    :b #{:d}}
   to format:
   [[:a :b]
    [:b :c]
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

(defn ^{:added "0.6.0"} submit-job [config job]
  (let [id (java.util.UUID/randomUUID)
        client (component/start (system/onyx-client config))
        _  (validator/validate-job (assoc job :workflow (:workflow job)))
        _ (validator/validate-flow-conditions (:flow-conditions job) (:workflow job))
        tasks (planning/discover-tasks (:catalog job) (:workflow job))
        task-ids (map :id tasks)
        scheduler (:task-scheduler job)
        sat (saturation (:catalog job))
        task-saturation (task-saturation (:catalog job) tasks)
        input-task-ids (find-input-tasks (:catalog job) tasks)
        output-task-ids (find-output-tasks (:catalog job) tasks)
        exempt-task-ids (find-exempt-tasks tasks (:acker/exempt-tasks job))
        args {:id id :tasks task-ids :task-scheduler scheduler
              :saturation sat :task-saturation task-saturation
              :inputs input-task-ids :outputs output-task-ids
              :exempt-tasks exempt-task-ids
              :acker-percentage (or (:acker/percentage job) 1)
              :acker-exclude-inputs (or (:acker/exempt-input-tasks? job) false)
              :acker-exclude-outputs (or (:acker/exempt-output-tasks? job) false)}
        args (add-percentages-to-log-entry config job args tasks (:catalog job) id)
        entry (create-log-entry :submit-job args)]
    (extensions/write-chunk (:log client) :catalog (:catalog job) id)
    (extensions/write-chunk (:log client) :workflow (:workflow job) id)
    (extensions/write-chunk (:log client) :flow-conditions (:flow-conditions job) id)

    (doseq [task tasks]
      (extensions/write-chunk (:log client) :task task id)
      (let [task-map (planning/find-task (:catalog job) (:name task))]
        (when (:onyx/bootstrap? task-map)
          ;; TODO: reimplment bootstrapping.
          )))

    (extensions/write-log-entry (:log client) entry)
    (component/stop client)
    id))

(defn ^{:added "0.6.0"} kill-job
  "Kills a currently executing job, given it's job ID. All peers executing
   tasks for this job cleanly stop executing and volunteer to work on other jobs.
   Task lifecycle APIs for closing tasks are invoked. This job is never again scheduled
   for execution."
  [config job-id]
  (let [client (component/start (system/onyx-client config))
        entry (create-log-entry :kill-job {:job job-id})]
    (extensions/write-log-entry (:log client) entry)
    (component/stop client)
    true))

(defn ^{:added "0.6.0"} subscribe-to-log
  "Sends all events from the log to the provided core.async channel.
   Starts at the origin of the log and plays forward monotonically.

   Returns a map with keys :replica and :env. :replica contains the origin
   replica. :env contains an Component with a :log connection to ZooKeeper,
   convenient for directly querying the znodes. :env can be shutdown with
   the onyx.api/shutdown-env function"
  [config ch]
  (let [env (component/start (system/onyx-client config))]
    {:replica (extensions/subscribe-to-log (:log env) ch)
     :env env}))

(defn ^{:added "0.6.0"} gc
  "Invokes the garbage collector on Onyx. Compresses all local replicas
   for peers, decreasing memory usage. Also deletes old log entries from
   ZooKeeper, freeing up disk space.

   Local replicas clear out all data about completed and killed jobs -
   as if they never existed. "
  [config]
  (let [id (java.util.UUID/randomUUID)
        client (component/start (system/onyx-client config))
        entry (create-log-entry :gc {:id id})
        ch (chan 1000)]
    (extensions/write-log-entry (:log client) entry)
    
    (loop [replica (extensions/subscribe-to-log (:log client) ch)]
      (let [position (<!! ch)
            entry (extensions/read-log-entry (:log client) position)
            new-replica (extensions/apply-log-entry entry replica)]
        (if (and (= (:fn entry) :gc) (= (:id (:args entry)) id))
          (let [diff (extensions/replica-diff entry replica new-replica)]
            (extensions/fire-side-effects! entry replica new-replica diff {:id id :log (:log client)}))
          (recur new-replica))))
    (component/stop client)
    true))

(defn ^{:added "0.6.0"} await-job-completion
  "Blocks until job-id has had all of its tasks completed."
  [config job-id]
  (let [client (component/start (system/onyx-client config))
        ch (chan 100)]
    (loop [replica (extensions/subscribe-to-log (:log client) ch)]
      (let [position (<!! ch)
            entry (extensions/read-log-entry (:log client) position)
            new-replica (extensions/apply-log-entry entry replica)
            tasks (get (:tasks new-replica) job-id)
            complete-tasks (get (:completions new-replica) job-id)]
        (if (or (nil? tasks) (not= (into #{} tasks) (into #{} complete-tasks)))
          (recur new-replica)
          (do (component/stop client)
              true))))))

(defn ^{:no-doc true} peer-lifecycle [started-peer config shutdown-ch ack-ch]
  (try
    (loop [live started-peer]
      (let [restart-ch (:restart-ch (:virtual-peer live))
            [v ch] (alts!! [shutdown-ch restart-ch] :priority? true)]
        (cond (= ch shutdown-ch)
              (do (component/stop live)
                  (>!! ack-ch true))
              (= ch restart-ch)
              (do (component/stop live)
                  (Thread/sleep (or (:onyx.peer/retry-start-interval config) 2000))
                  (recur (component/start live)))
              :else (throw (ex-info "Read from a channel with no response implementation" {})))))
    (catch Exception e
      (fatal "Peer lifecycle threw an exception")
      (fatal e))))

(defn ^{:added "0.6.0"} start-peers
  "Launches n virtual peers. Each peer may be stopped
   by passing it to the shutdown-peer function."
  [n config]
  (doall
   (map
    (fn [_]
      (let [v-peer (system/onyx-peer config)
            live (component/start v-peer)
            shutdown-ch (chan 1)
            ack-ch (chan)]
        {:peer (future (peer-lifecycle live config shutdown-ch ack-ch))
         :shutdown-ch shutdown-ch
         :ack-ch ack-ch}))
    (range n))))

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
  [env-config]
  (component/start (system/onyx-development-env env-config)))

(defn ^{:added "0.6.0"} shutdown-env
  "Shuts down the given development environment."
  [env]
  (component/stop env))

