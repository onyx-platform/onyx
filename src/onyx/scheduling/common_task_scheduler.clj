(ns onyx.scheduling.common-task-scheduler
  (:require [clojure.core.async :refer [chan go >! <! close! >!!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [com.stuartsierra.component :as component]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]
            [taoensso.timbre]))

(defn preallocated-grouped-task? [replica job task]
  (and (#{:continue :kill} (get-in replica [:flux-policies job task]))
       (> (count (get-in replica [:allocations job task])) 0)))

(defmulti task-distribute-peer-count
  (fn [replica job-id n]
    (get-in replica [:task-schedulers job-id])))

(defmulti task-constraints
  (fn [replica jobs peer->vm task->node no-op-node job-id]
    (get-in replica [:task-schedulers job-id])))

(defmethod task-distribute-peer-count :default
  [replica job n]
  (throw (ex-info (format "Task scheduler %s not recognized" (get-in replica [:task-schedulers job]))
                  {:task-scheduler (get-in replica [:task-schedulers job])
                   :replica replica
                   :job job})))

(defmethod task-constraints :default
  [replica jobs peer->vm task->node no-op-node job-id]
  [])
