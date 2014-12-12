(ns onyx.log.commands.seal-task
  (:require [clojure.core.async :refer [>!!]]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :seal-task
  [entry replica]
  replica)

(defmethod extensions/replica-diff :seal-task
  [{:keys [args]} old new]
  nil)

(defmethod extensions/reactions :seal-task
  [{:keys [args]} old new diff peer-args]
  [])

(defmethod extensions/fire-side-effects! :seal-task
  [{:keys [args]} old new diff state]
  (let [n-peers (count (get-in new [:allocations (:job args) (:task args)]))
        status (common/task-status new (:job args) (:task args))]
    (when (= (:id args) (:id state))
      (>!! (:seal-response-ch state)
           (and (>= (:waiting status) (dec n-peers))
                (zero? (:sealing status))
                (= (get-in new [:peer-states (:id args)]) :active))))))

