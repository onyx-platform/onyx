(ns ^:no-doc onyx.coordinator.impl
  (:require [com.stuartsierra.component :as component]
            [taoensso.timbre]
            [onyx.extensions :as extensions]
            [onyx.sync.zookeeper])
  (:import [onyx.sync.zookeeper ZooKeeper]))

(defn create-job-datom [catalog workflow tasks])

(defn create-task-datom [task])

(defn find-incomplete-tasks [db job-id])

(defn find-incomplete-concurrent-tasks [db job-id])

(defn find-active-task-ids [db job-id tasks])

(defn peer-count [db task])

(defn sort-tasks-by-phase [db tasks])

(defn sort-tasks-by-peer-count [db tasks])

(defn next-inactive-task [db job-id])

(defn next-active-task [db job-id])

(defn all-active-jobs-ids [db])

(defn last-offered-job [db])

(defn job-candidate-seq [job-seq last-offered])

(defn to-task [db eid])

(defn select-nodes [ent])

(defn node->task [db basis node])

(defn n-active-peers [db task-id])

(defn n-sealing-peers [db task-id])

(defn n-peers [db task-id])

(defn node-basis [db basis node])

(defmethod extensions/mark-peer-born ZooKeeper
  [sync peer-node]
  (let [peer-data (extensions/read-place sync peer-node)
        peer-state-path (extensions/create-at sync :peer-state (:id peer-data))
        state {:id (:id peer-data) :state :idle}]
    (extensions/write-place peer-state-path state)))

(defmethod extensions/mark-peer-dead ZooKeeper
  [sync pulse-node]
  (let [peer-state (extensions/read-place sync pulse-node)
        peer-state-path (extensions/read-place-at sync :peer-state (:id peer-state))
        state {:id (:id peer-state) :state :dead}]
    (extensions/write-place peer-state-path state)))

(defmethod extensions/plan-job ZooKeeper
  [sync catalog workflow tasks])

(defmethod extensions/next-tasks ZooKeeper
  [sync])

(defmethod extensions/nodes ZooKeeper
  [sync peer])

(defmethod extensions/node-basis ZooKeeper
  [sync basis node])

(defmethod extensions/node->task ZooKeeper
  [sync basis node])

(defmethod extensions/idle-peers ZooKeeper
  [sync])

(defmethod extensions/seal-resource? ZooKeeper
  [sync exhaust-place])

(defmethod extensions/mark-offered ZooKeeper
  [sync task peer nodes])

(defmethod extensions/ack ZooKeeper
  [sync ack-place])

(defmethod extensions/revoke-offer ZooKeeper
  [sync ack-place])

(defmethod extensions/complete ZooKeeper
  [sync complete-place])

