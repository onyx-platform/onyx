(ns onyx.log.commands.backpressure-off
  (:require [taoensso.timbre :as timbre :refer [info error]]
            [clojure.core.async :refer [>!!]]
            [clojure.data :refer [diff]]
            [onyx.log.commands.common :as common]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :backpressure-off
  [{:keys [args]} replica]
  (if (= :backpressure (get-in replica [:peer-state (:peer args)]))
    (assoc-in replica [:peer-state (:peer args)] :active) 
    replica))

(defmethod extensions/replica-diff :backpressure-off
  [{:keys [args]} old new]
  (second (diff (:peer-state old) (:peer-state new))))

(defmethod extensions/reactions :backpressure-off
  [{:keys [args]} old new diff peer-args]
  [])

(defmethod extensions/fire-side-effects! :backpressure-off
  [{:keys [args]} old new diff {:keys [monitoring] :as state}]
  (if (= (:peer args) (:id state))
    (do (extensions/emit monitoring {:event :peer-backpressure-off :id (:id state)})
        state)
    state))
