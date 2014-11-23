(ns onyx.log.commands.submit-job
  (:require [clojure.core.async :refer [chan go >! <! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :submit-job
  [{:keys [args]} replica]
  (-> replica
      (update-in [:jobs] conj (:id args))
      (update-in [:jobs] vec)
      (assoc-in [:task-schedulers (:id args)] (:task-scheduler args))
      (assoc-in [:tasks (:id args)] (:tasks args))))

(defmethod extensions/replica-diff :submit-job
  [{:keys [args]} old new]
  {:job (:id args)})

(defmethod extensions/reactions :submit-job
  [entry old new diff peer-args]
  (if (and (= (:job-scheduler old) :onyx.job-scheduler/greedy)
           (not (seq (:jobs old))))
    [{:fn :volunteer-for-task
      :args {:id (:id peer-args)}}]
    []))

(defmethod extensions/fire-side-effects! :submit-job
  [entry old new diff state]
  state)

