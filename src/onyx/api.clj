(ns onyx.api
  (:require [clojure.string :refer [split]]
            [clojure.core.async :refer [chan alts!! >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [clj-http.client :refer [post]]
            [taoensso.timbre :refer [warn]]
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
        args {:id id :tasks task-ids :task-scheduler scheduler :saturation sat}
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

(defn await-job-completion
  "Blocks until job-id has had all of its tasks completed."
  [config job-id]
  (let [client (component/start (system/onyx-client config))
        ch (chan 100)]
    (extensions/subscribe-to-log (:log client) 0 ch)
    (loop [replica {:job-scheduler (:onyx.peer/job-scheduler config)}]
      (let [position (<!! ch)
            entry (extensions/read-log-entry (:log client) position)
            new-replica (extensions/apply-log-entry entry replica)
            tasks (get (:tasks new-replica) job-id)
            complete-tasks (get (:completions new-replica) job-id)]
        (if (or (nil? tasks) (not= (into #{} tasks) (into #{} complete-tasks)))
          (recur new-replica)
          (do (component/stop client)
              true))))))

(defn peer-lifecycle [started-peer shutdown-ch ack-ch]
  (loop [live started-peer]
    (let [restart-ch (:ch (:restart-chan live))
          [ch] (alts!! [shutdown-ch restart-ch] :priority? true)]
      (cond (= ch shutdown-ch)
            (do (component/stop live)
                (>!! ack-ch true))
            (= ch restart-ch)
            (do (component/stop live)
                (recur (component/start live)))
            :else (throw (ex-info "Read from a channel with no response implementation" {}))))))

(defn start-peers!
  "Launches n virtual peers. Each peer may be stopped
   by invoking the fn returned by :shutdown-fn."
  [n config]
  (doall
   (map
    (fn [_]
      (let [v-peer (system/onyx-peer config)
            live (component/start v-peer)
            shutdown-ch (chan 1)
            ack-ch (chan)]
        {:peer (future (peer-lifecycle live shutdown-ch ack-ch))
         :shutdown-ch shutdown-ch
         :ack-ch ack-ch}))
    (range n))))

(defn shutdown-peer
  "Spins down the virtual peer"
  [peer]
  (>!! (:shutdown-ch peer) true)
  (<!! (:ack-ch peer)))

(defn shutdown-dev-env
  "Spins down the given development environment"
  [env]
  (component/stop env))

