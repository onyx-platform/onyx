(ns onyx.coordinator.log.datomic
  (:require [onyx.coordinator.extensions :as extensions]))

(defmethod extensions/mark-peer-born :datomic
  [log peer])

(defmethod extensions/mark-peer-dead :datomic
  [log peer])

(defmethod extensions/mark-offered :datomic
  [log])

(defmethod extensions/plan-job :datomic
  [log job])

(defmethod extensions/ack :datomic
  [log task])

(defmethod extensions/evict :datomic
  [log task])

(defmethod extensions/complete :datomic
  [log task])

(defmethod extensions/next-task :datomic
  [log])

