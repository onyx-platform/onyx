(ns ^:no-doc onyx.coordinator.impl
  (:require [com.stuartsierra.component :as component]
            [taoensso.timbre]
            [onyx.extensions :as extensions]
            [onyx.sync.zookeeper])
  (:import [onyx.sync.zookeeper ZooKeeper]))

(defn serialize-task [task]
  {:task/id (java.util.UUID/randomUUID)
   :task/name (:name task)
   :task/phase (:phase task)
   :task/consumption (:consumption task)
   :task/complete? false
   :task/ingress-queues (:ingress-queues task)
   :task/egress-queues (or (vals (:egress-queues task)) [])})

(defn task-complete? [sync task-node]
  (extensions/place-exists? sync (str task-node ".complete")))

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
        next-state-path (extensions/create-at sync :peer-state (:id peer-state))
        state {:id (:id peer-state) :state :dead}]
    (extensions/write-place next-state-path state)))

(defmethod extensions/plan-job ZooKeeper
  [sync catalog workflow tasks]
  (let [job-id (extensions/create sync :job)
        workflow-path (extensions/create-at sync :workflow job-id)
        catalog-path (extensions/create-at sync :catalog job-id)]

    (extensions/write-place sync workflow-path workflow)
    (extensions/write-place sync catalog-path catalog)

    (doseq [task tasks]
      (let [place (extensions/create-at sync :task job-id)]
        (extensions/write-place sync place (serialize-task task))))))

(defmethod extensions/mark-offered ZooKeeper
  [sync task-node peer-node nodes]
  (let [peer-data (extensions/read-place sync peer-node)
        peer-state (extensions/deref-place-at sync :peer-state (:id peer-data))
        complete? (task-complete? sync task-node)]
    (when (and (= (:state peer-state) :idle) (not complete?))
      (let [next-path (extensions/create-at sync :peer-state (:id peer-state))
            state {:id (:id peer-data) :state :acking :task-node task-node :nodes nodes}]
        (extensions/write-place sync next-path state)))))

(defmethod extensions/ack ZooKeeper
  [sync ack-place]
  (let [ack-data (extensions/read-place sync ack-place)
        peer-state (extensions/deref-place-at sync :peer-state (:id ack-data))
        complete? (task-complete? sync (:task-node peer-state))]
    (when (and (= (:state peer-state) :acking) (not complete?))
      (let [next-path (extensions/create-at sync :peer-state (:id peer-state))
            state (assoc peer-state :state :active)]
        (extensions/write-place sync next-path state)))))

(defmethod extensions/revoke-offer ZooKeeper
  [sync ack-place]
  (let [ack-data (extensions/read-place sync ack-place)
        peer-state (extensions/deref-place-at sync :peer-state (:id ack-data))]
    (when (= (:state peer-state) :acking)
      (let [next-path (extensions/create-at sync :peer-state (:id peer-state))
            state {:id (:id peer-state) :state :dead}]
        (extensions/write-place sync next-path state)))))

(defmethod extensions/nodes ZooKeeper
  [sync peer-node]
  (let [peer-data (extensions/read-place sync peer-node)
        peer-state (extensions/deref-place-at sync :peer-state (:id peer-data))]
    (:nodes peer-state)))

(defmethod extensions/next-tasks ZooKeeper
  [sync])

(defmethod extensions/node->task ZooKeeper
  [sync basis node])

(defmethod extensions/idle-peers ZooKeeper
  [sync])

(defmethod extensions/seal-resource? ZooKeeper
  [sync exhaust-place])

(defmethod extensions/complete ZooKeeper
  [sync complete-place])

