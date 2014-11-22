(ns onyx.log.volunteer-for-task
  (:require [clojure.core.async :refer [chan go >! <! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.extensions :as extensions]))

(defmulti update-replica-using-scheduler
  (fn [{:keys [args]} replica]
    (:job-scheduler replica)))

(defmethod update-replica-using-scheduler :onyx.job-scheduler/greedy
  [{:keys [args]} replica]
  (let [job (first (:jobs replica))
        tasks (get-in replica [:tasks job])]
    (-> replica
        (update-in [:allocations job (first tasks)] conj (:id args))
        (update-in [:allocations job (first tasks)] vec))))

(defmethod update-replica-using-scheduler :onyx.job-scheduler/round-robin
  [{:keys [args]} replica]
  (let [prev (or (:last-allocated replica) (first (:jobs replica)))
        job (second (drop-while (partial not= prev) (cycle (:jobs replica))))
        tasks (get-in replica [:tasks job])]
    (-> replica
        (update-in [:allocations job (first tasks)] conj (:id args))
        (update-in [:allocations job (first tasks)] vec)
        (assoc-in [:last-allocated] job))))

(defmethod update-replica-using-scheduler :default
  [_ replica]
  (throw (ex-info "This peer has no :job-scheduler set in its replica"
                  {:replica replica})))

(defmethod extensions/apply-log-entry :volunteer-for-task
  [entry replica]
  (update-replica-using-scheduler entry replica))

(defmethod extensions/replica-diff :volunteer-for-task
  [{:keys [args]} old new]
  {:job (:id args)})

(defmethod extensions/reactions :volunteer-for-task
  [{:keys [args]} old new diff peer-args])

(defmethod extensions/fire-side-effects! :volunteer-for-task
  [entry old new diff state]
  state)

