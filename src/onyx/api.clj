(ns onyx.api
  (:require [clojure.string :refer [split]]
            [clojure.core.async :refer [chan alts!! >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [clj-http.client :refer [post]]
            [taoensso.timbre :refer [warn fatal]]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.system :as system]
            [onyx.extensions :as extensions]
            [onyx.validation :as validator]
            [onyx.planning :as planning]))

(defn saturation [catalog]
  (let [rets
        (reduce #(+ %1 (or (:onyx/max-peers %2)
                           Double/POSITIVE_INFINITY))
                0
                catalog)]
    (if (zero? rets)
      Double/POSITIVE_INFINITY
      rets)))

(defn task-saturation [catalog tasks]
  (into
   {}
   (map
    (fn [task]
      {(:id task)
       (or (:onyx/max-peers (planning/find-task catalog (:name task)))
           Double/POSITIVE_INFINITY)})
    tasks)))

(defn map-set-workflow->workflow
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

(defn unpack-map-workflow
  ([workflow] (vec (unpack-map-workflow workflow [])))
  ([workflow result]
     (let [roots (keys workflow)]
       (if roots
         (concat result
                 (mapcat
                  (fn [k]
                    (let [child (get workflow k)]
                      (if (map? child)
                        (concat (map (fn [x] [k x]) (keys child))
                                (unpack-map-workflow child result))
                        [[k child]])))
                  roots))
         result))))

(defn add-job-percentage [config job args]
  (if (= (:onyx.peer/job-scheduler config) :onyx.job-scheduler/percentage)
    (assoc args :percentage (:percentage job))
    args))

(defn task-id->pct [catalog task]
  (let [task-map (planning/find-task catalog (:name task))]
    {(:id task) (:onyx/percentage task-map)}))

(defn add-task-percentage [args job-id tasks catalog]
  (if (= (:task-scheduler args) :onyx.task-scheduler/percentage)
    (assoc-in args
              [:task-percentages]
              (into {} (map (fn [t] (task-id->pct catalog t)) tasks)))
    args))

(defn add-percentages-to-log-entry [config job args tasks catalog job-id]
  (let [job-updated (add-job-percentage config job args)]
    (add-task-percentage job-updated job-id tasks catalog)))

(defn submit-job [config job]
  (let [id (java.util.UUID/randomUUID)
        client (component/start (system/onyx-client config))
        normalized-workflow (if (map? (:workflow job))
                              (unpack-map-workflow (:workflow job))
                              (:workflow job))
        _  (validator/validate-job (assoc job :workflow normalized-workflow))
        tasks (planning/discover-tasks (:catalog job) normalized-workflow)
        task-ids (map :id tasks)
        scheduler (:task-scheduler job)
        sat (saturation (:catalog job))
        task-saturation (task-saturation (:catalog job) tasks)
        args {:id id :tasks task-ids :task-scheduler scheduler
              :saturation sat :task-saturation task-saturation}
        args (add-percentages-to-log-entry config job args tasks (:catalog job) id)
        entry (create-log-entry :submit-job args)]
    (extensions/write-chunk (:log client) :catalog (:catalog job) id)
    (extensions/write-chunk (:log client) :workflow normalized-workflow id)

    (doseq [task tasks]
      (extensions/write-chunk (:log client) :task task id)
      (let [task-map (planning/find-task (:catalog job) (:name task))]
        (when (:onyx/bootstrap? task-map)
          (extensions/bootstrap-queue (:queue client) task))))

    (extensions/write-log-entry (:log client) entry)
    (component/stop client)
    id))

(defn kill-job
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

(defn subscribe-to-log
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

(defn gc
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

(defn await-job-completion
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

(defn peer-lifecycle [started-peer config shutdown-ch ack-ch]
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

(defn start-peers!
  "Launches n virtual peers. Each peer may be stopped
   by passing it to the shutdown-peer function."
  ([n config]
     (start-peers! n config system/onyx-peer))
  ([n config peer-f]
     (doall
      (map
       (fn [_]
         (let [v-peer (peer-f config)
               live (component/start v-peer)
               shutdown-ch (chan 1)
               ack-ch (chan)]
           {:peer (future (peer-lifecycle live config shutdown-ch ack-ch))
            :shutdown-ch shutdown-ch
            :ack-ch ack-ch}))
       (range n)))))

(defn shutdown-peer
  "Spins down the virtual peer"
  [peer]
  (>!! (:shutdown-ch peer) true)
  (<!! (:ack-ch peer)))

(defn start-env
  "Spins up a development environment, using in-memory ZooKeeper and HornetQ"
  [env-config]
  (component/start (system/onyx-development-env env-config)))

(defn shutdown-env
  "Spins down the given development environment"
  [env]
  (component/stop env))

