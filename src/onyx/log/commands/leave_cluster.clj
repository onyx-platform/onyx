(ns onyx.log.leave-cluster
  (:require [clojure.core.async :refer [chan go >! <! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :leave-cluster
  [kw args]
  (fn [replica message-id]
    (-> replica
        (update-in [:peers] remove #(= (:id args)))
        (update-in [:prepared] dissoc (:id args))
        (update-in [:accepted] dissoc (:id args)))))

(defmethod extensions/replica-diff :leave-cluster
  [kw old new args]
  {:died (:id args)})

(defmethod extensions/reactions :leave-cluster
  [kw old new diff args])

(defmethod extensions/fire-side-effects! :leave-cluster
  [kw old new diff args state])

