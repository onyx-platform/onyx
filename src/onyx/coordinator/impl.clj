(ns ^:no-doc onyx.coordinator.impl
  (:require [com.stuartsierra.component :as component]
            [taoensso.timbre]
            [onyx.extensions :as extensions]
            [onyx.sync.zookeeper])
  (:import [onyx.sync.zookeeper ZooKeeper]))

(def complete-marker ".complete")

(defn serialize-task [task]
  {:task/id (:id task)
   :task/name (:name task)
   :task/phase (:phase task)
   :task/consumption (:consumption task)
   :task/ingress-queues (:ingress-queues task)
   :task/egress-queues (or (vals (:egress-queues task)) [])})

(defn task-complete? [sync task-node]
  (extensions/place-exists? sync (str task-node complete-marker)))

(defn complete-task [sync task-node]
  (extensions/create-node sync (str task-node complete-marker)))

(defn n-active-peers [sync task-node]
  (let [peers (extensions/bucket sync :peer-state)]
    (count
     (filter
      (fn [peer]
        (let [state (extensions/dereference sync peer)]
          (and (= (:task-node state) task-node)
               (= (:state state) :active))))
      peers))))

(defn n-sealing-peers [sync task-node]
  (let [peers (extensions/bucket sync :peer-state)]
    (count
     (filter
      (fn [peer]
        (let [state (extensions/dereference sync peer)]
          (and (= (:task-node state) task-node)
               (= (:state state) :sealing))))
      peers))))

(defn n-peers [sync task-node]
  (+ (n-active-peers sync task-node)
     (n-sealing-peers sync task-node)))

(defmethod extensions/mark-peer-born ZooKeeper
  [sync peer-node]
  (let [peer-data (extensions/read-place sync peer-node)
        peer-state-path (extensions/create-at sync :peer-state (:id peer-data))
        state {:id (:id peer-data) :state :idle}]
    (extensions/write-place sync peer-state-path state)))

(defmethod extensions/mark-peer-dead ZooKeeper
  [sync pulse-node]
  (let [peer-state (extensions/read-place sync pulse-node)
        state-path (extensions/resolve-node sync :peer-state (:id peer-state))
        peer-state (extensions/dereference sync state-path)
        next-state-path (extensions/create-at sync :peer-state (:id peer-state))
        state {:id (:id peer-state) :state :dead}]
    (extensions/write-place sync next-state-path state)))

(defmethod extensions/plan-job ZooKeeper
  [sync catalog workflow tasks]
  (let [job-id (extensions/create sync :job)
        workflow-path (extensions/create-at sync :workflow job-id)
        catalog-path (extensions/create-at sync :catalog job-id)]

    (extensions/write-place sync workflow-path workflow)
    (extensions/write-place sync catalog-path catalog)

    (doseq [task tasks]
      (let [place (extensions/create-at sync :task job-id)]
        (extensions/write-place sync place (serialize-task task))))

    job-id))

(defmethod extensions/mark-offered ZooKeeper
  [sync task-node peer-node nodes]
  (let [peer-data (extensions/read-place sync peer-node)
        state-path (extensions/resolve-node sync :peer-state (:id peer-data))
        peer-state (extensions/dereference sync :peer-state state-path)
        complete? (task-complete? sync task-node)]
    (when (and (= (:state peer-state) :idle) (not complete?))
      (let [next-path (extensions/create-at sync :peer-state (:id peer-state))
            state {:id (:id peer-data) :state :acking :task-node task-node :nodes nodes}
            offer-log (extensions/create sync :job-log)
            job-id (:job-id (extensions/read-place sync task-node))]
        (extensions/write-place sync offer-log {:job-id job-id})
        (extensions/write-place sync next-path state)))))

(defmethod extensions/ack ZooKeeper
  [sync ack-place]
  (let [ack-data (extensions/read-place sync ack-place)
        state-path (extensions/resolve-node sync :peer-state (:id ack-data))
        peer-state (extensions/dereference sync :peer-state state-path)
        complete? (task-complete? sync (:task-node peer-state))]
    (when (and (= (:state peer-state) :acking) (not complete?))
      (let [next-path (extensions/create-at sync :peer-state (:id peer-state))
            state (assoc peer-state :state :active)]
        (extensions/write-place sync next-path state)))))

(defmethod extensions/revoke-offer ZooKeeper
  [sync ack-place]
  (let [ack-data (extensions/read-place sync ack-place)
        state-path (extensions/resolve-node sync :peer-state (:id ack-data))
        peer-state (extensions/dereference sync :peer-state state-path)]
    (when (= (:state peer-state) :acking)
      (let [next-path (extensions/create-at sync :peer-state (:id peer-state))
            state {:id (:id peer-state) :state :dead}]
        (extensions/write-place sync next-path state)))))

(defmethod extensions/nodes ZooKeeper
  [sync node]
  (let [node-data (extensions/read-place sync node)
        state-path (extensions/resolve-node sync :peer-state (:id node-data))
        peer-state (extensions/dereference sync :peer-state state-path)]
    (:nodes peer-state)))

(defmethod extensions/idle-peers ZooKeeper
  [sync]
  (let [peers (extensions/bucket sync :peer-state)]
    (filter
     (fn [peer]
       (let [state (extensions/dereference sync :peer-state peer)]
         (= (:state state) :idle)))
     peers)))

(defmethod extensions/seal-resource? ZooKeeper
  [sync exhaust-place]
  (let [node-data (extensions/read-place sync exhaust-place)
        state-path (extensions/resolve-node sync :peer-state (:id node-data))
        peer-state (extensions/dereference sync :peer-state state-path)
        next-path (extensions/create-at sync :peer-state (:id peer-state))
        state (assoc peer-state :state :sealing)]
    (extensions/write-place sync next-path state)

    (let [n-active (n-active-peers sync (:task-node peer-state))
          n (n-peers sync (:task-node peer-state))]
      {:seal? (or (= n 1) (zero? n-active))
       :seal-node (:node/seal (:nodes peer-state))})))

(defmethod extensions/complete ZooKeeper
  [sync complete-node]
  (let [node-data (extensions/read-place sync complete-node)
        state-path (extensions/resolve-node sync :peer-state (:id node-data))
        peer-state (extensions/dereference sync :peer-state state-path)
        complete? (task-complete? sync (:task-node node-data))
        n (n-peers sync (:task-node node-data))]
    (when (not complete?)
      (let [next-path (extensions/create-at sync :peer-state (:id node-data))
            state {:id (:id node-data) :state :idle}]
        (when (= n 1)
          (complete-task (:task-node node-data)))
        (extensions/write-place sync next-path state)
        {:n-peers n}))))

(defn incomplete-jobs-ids [sync]
  (let [job-ids (extensions/bucket sync :job)]
    (filter
     (fn [job-id]
       (let [tasks (extensions/bucket-at sync :task job-id)]
         (seq
          (filter
           (fn [task]
             (let [completion (str task complete-marker)]
               (not (extensions/place-exists-at? sync :task job-id completion))))
           tasks))))
     job-ids)
    (sort-by (fn [job-id] (extensions/creation-time sync job-id)) job-ids)))

(defn last-offered-job [sync]
  (extensions/dereference sync (extensions/bucket sync :job-log)))

(defn job-candidate-seq [job-seq last-offered]
  (if (nil? last-offered)
    job-seq
    (->> (cycle job-seq)
         (drop (inc (.indexOf job-seq last-offered)))
         (take (count job-seq)))))

(defn sort-tasks-by-phase [sync job-id tasks]
  (sort-by :task/phase (map (fn [t] (extensions/read-place sync t)) tasks)))

(defn find-incomplete-tasks [sync tasks]
  (filter (fn [task] (not (task-complete? sync task))) tasks))

(defn find-active-task-ids [sync tasks]
  (filter (fn [task] (> (n-peers sync task) 1)) tasks))

(defn next-inactive-task [sync job-id]
  (let [task-nodes (extensions/bucket-at sync :task job-id)
        tasks (map #(extensions/read-place-at sync :task job-id %) task-nodes)
        incomplete-tasks (find-incomplete-tasks sync task-nodes)
        sorted-tasks (sort-tasks-by-phase sync job-id incomplete-tasks)
        active-tasks (find-active-task-ids sync sorted-tasks)]
    (filter (fn [t] (not (contains? active-tasks (:task/id t)))) sorted-tasks)))

(defn find-incomplete-concurrent-tasks [sync job-id]
  (let [tasks (extensions/bucket-at sync :task job-id)
        active (find-active-task-ids sync tasks)]
    (filter (fn [task]
              (let [task-data (extensions/read-place sync task)]
                (= (:task/consumption task-data) :concurrent)))
            active)))

(defn sort-tasks-by-peer-count [sync tasks]
  (sort-by (partial n-peers sync) tasks))

(defn next-active-task [sync job-id]
  (let [incomplete-tasks (find-incomplete-concurrent-tasks sync job-id)]
    (sort-tasks-by-peer-count sync incomplete-tasks)))

(defmethod extensions/next-tasks ZooKeeper
  [sync]
  (let [job-seq (job-candidate-seq (incomplete-jobs-ids sync)
                                   (last-offered-job sync))
        inactive-candidates (mapcat (partial next-inactive-task sync) job-seq)
        active-candidates (mapcat (partial next-active-task sync) job-seq)
        tasks (concat (filter identity inactive-candidates)
                      (filter identity active-candidates))]
    (map serialize-task tasks)))

