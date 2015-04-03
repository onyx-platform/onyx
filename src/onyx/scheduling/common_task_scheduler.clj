(ns onyx.scheduling.common-task-scheduler
  (:require [clojure.core.async :refer [chan go >! <! close! >!!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [com.stuartsierra.component :as component]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]
            [taoensso.timbre]))

(defn active-tasks-only
  "Filters out tasks that are currently being sealed."
  [replica tasks]
  (filter #(nil? (get-in replica [:sealing-task %])) tasks))

(defn incomplete-tasks [replica job tasks]
  (let [tasks (get-in replica [:tasks job])
        completed (get-in replica [:completions job])]
    (filter identity (second (diff completed tasks)))))

(defmulti select-task
  (fn [replica job peer-id]
    (get-in replica [:task-schedulers job])))

(defmethod select-task :default
  [replica job peer-id]
  (throw (ex-info 
           (format "Task scheduler %s not recognized. Check that you have not supplied a job scheduler instead."
                   (get-in replica [:task-schedulers job]))
           {:replica replica})))

(defmulti drop-peers
  (fn [replica job n]
    (get-in replica [:task-schedulers job])))

(defmethod drop-peers :default
  [replica job n]
  (let [scheduler (get-in replica [:task-schedulers job])]
    (throw (ex-info 
             (format "Task scheduler %s not recognized. Check that you have not supplied a job scheduler instead." 
                     scheduler)
             {:replica replica}))))

(defmulti reallocate-from-task?
  (fn [scheduler old new job state]
    scheduler))

(defmethod reallocate-from-task? :default
  [scheduler old new job state]
  false)
