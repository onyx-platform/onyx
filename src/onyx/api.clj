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

(defn await-job-completion [config job-id]
  (let [client (component/start (system/onyx-client config))
        ch (chan 100)]
    (extensions/subscribe-to-log (:log client) 0 ch)
    (loop [replica {:job-scheduler (:onyx.peer/job-scheduler config)}]
      (let [position (<!! ch)
            entry (extensions/read-log-entry (:log client) position)
            new-replica (extensions/apply-log-entry entry replica)
            tasks (get (:tasks replica) job-id)
            complete-tasks (get (:completions replica) job-id)]
        (when-not (= (into #{} tasks) (into #{} complete-tasks))
          (recur new-replica))))))

(defn start-peers!
  "Launches n virtual peers. Each peer may be stopped
   by invoking the fn returned by :shutdown-fn."
  [n config]
  (doall
   (map
    (fn [_]
      (let [stop-ch (chan (clojure.core.async/sliding-buffer 1))
            v-peer (system/onyx-peer config)]
        {:runner (future
                   (let [live (component/start v-peer)]
                     (let [ack-ch (<!! stop-ch)]
                       (component/stop live)
                       (>!! ack-ch true)
                       (close! ack-ch))))
         :shutdown-fn (fn []
                        (let [ack-ch (chan)]
                          (>!! stop-ch ack-ch)
                          (<!! ack-ch)))}))
    (range n))))

