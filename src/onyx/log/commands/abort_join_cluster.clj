(ns onyx.log.abort-join-cluster
  (:require [clojure.core.async :refer [chan go >! <! >!! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :abort-join-cluster
  [{:keys [args message-id]} replica]
  (-> replica
      (update-in [:prepared] dissoc (:id args))
      (update-in [:accepted] (:id args))))

(defmethod extensions/replica-diff :abort-join-cluster
  [entry old new]
  (let [prepared (keys (first (diff (:prepared old) (:prepared new))))
        accepted (keys (first (diff (:accepted old) (:accepted new))))]
    (assert (<= (count prepared) 1))
    (assert (<= (count accepted) 1))
    (when (or (seq prepared) (seq accepted))
      {:aborted (or (first prepared) (first accepted))})))

(defmethod extensions/fire-side-effects! :abort-join-cluster
  [{:keys [args]} old new diff state]
  state)

(defmethod extensions/reactions :abort-join-cluster
  [entry old new diff peer-args]
  [])

