(ns onyx.log.volunteer-for-task
  (:require [clojure.core.async :refer [chan go >! <! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :volunteer-for-task
  [{:keys [args]} replica]
  (if (= (:job-scheduler replica) :onyx.job-scheduler/greedy)
    (let [job (first (:jobs replica))]
      (-> replica
          (update-in [:allocations job] conj (:id args))
          (update-in [:allocations job] vec)))
    replica))

(defmethod extensions/replica-diff :volunteer-for-task
  [{:keys [args]} old new]
  {:job (:id args)})

(defmethod extensions/reactions :volunteer-for-task
  [{:keys [args]} old new diff peer-args])

(defmethod extensions/fire-side-effects! :volunteer-for-task
  [entry old new diff state]
  state)

