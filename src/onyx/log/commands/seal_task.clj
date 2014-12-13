(ns onyx.log.commands.seal-task
  (:require [clojure.core.async :refer [>!!]]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]))

(defn should-seal? [replica args]
  (let [n-peers (count (get-in replica [:allocations (:job args) (:task args)]))
        status (common/task-status replica (:job args) (:task args))]
    (boolean
     (and (>= (or (:waiting status) 0) (dec n-peers))
          (= (or (:active status) 0) 1)
          (= (get-in replica [:peer-state (:id args)]) :active)))))

(defmethod extensions/apply-log-entry :seal-task
  [{:keys [args]} replica]
  (if-not (should-seal? replica args)
    (assoc-in replica [:peer-state (:id args)] :waiting)
    replica))

(defmethod extensions/replica-diff :seal-task
  [{:keys [args]} old new]
  nil)

(defmethod extensions/reactions :seal-task
  [{:keys [args]} old new diff peer-args]
  [])

(defmethod extensions/fire-side-effects! :seal-task
  [{:keys [args]} old new diff state]
  (when (= (:id args) (:id state))
    (>!! (:seal-response-ch state) (should-seal? new args)))
  state)

