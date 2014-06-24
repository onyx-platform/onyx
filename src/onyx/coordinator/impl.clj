(ns ^:no-doc onyx.coordinator.impl
    (:require [com.stuartsierra.component :as component]
              [taoensso.timbre]
              [onyx.extensions :as extensions]
              [onyx.sync.zookeeper])
    (:import [onyx.sync.zookeeper ZooKeeper]))

(def complete-marker ".complete")

(defn serialize-task [task job-id catalog-node workflow-node]
  {:task/id (:id task)
   :task/job-id job-id
   :task/catalog-node catalog-node
   :task/workflow-node workflow-node
   :task/name (:name task)
   :task/phase (:phase task)
   :task/consumption (:consumption task)
   :task/ingress-queues (:ingress-queues task)
   :task/egress-queues (or (vals (:egress-queues task)) [])})

(defn task-complete? [sync task-node]
  (extensions/place-exists? sync (str task-node complete-marker)))

(defn completed-task? [task-node]
  (.endsWith task-node complete-marker))

(defn complete-task [sync task-node]
  (extensions/create-node sync (str task-node complete-marker)))

(defn n-active-peers [sync task-node]
  (let [peers (extensions/bucket sync :peer-state)]
    (count
     (filter
      (fn [peer]
        (let [state (:content (extensions/dereference sync peer))]
          (and (= (:task-node state) task-node)
               (= (:state state) :active))))
      peers))))

(defn n-sealing-peers [sync task-node]
  (let [peers (extensions/bucket sync :peer-state)]
    (count
     (filter
      (fn [peer]
        (let [state (:content (extensions/dereference sync peer))]
          (and (= (:task-node state) task-node)
               (= (:state state) :sealing))))
      peers))))

(defn n-peers [sync task-node]
  (let [peers (extensions/bucket sync :peer-state)
        states (map :content (map (partial extensions/dereference sync) peers))]
    (count
     (filter
      (fn [state]
        (and (= (:task-node state) task-node)
             (or (= (:state state) :acking)
                 (= (:state state) :sealing)
                 (= (:state state) :active))))
      states))))

(defmethod extensions/mark-peer-born ZooKeeper
  [sync peer-node]
  (let [peer-data (extensions/read-place sync peer-node)
        state {:id (:id peer-data) :peer-node (:peer-node peer-data) :state :idle}
        state-path (extensions/resolve-node sync :peer-state (:id state))]
    (if-not (seq (extensions/children sync state-path))
      (:node (extensions/create-at sync :peer-state (:id peer-data) state))
      (throw (ex-info "Tried to add a duplicate peer" {:peer-node peer-node})))))

(defmethod extensions/mark-peer-dead ZooKeeper
  [sync peer-node]
  (let [node-data (extensions/read-place sync peer-node)
        state-path (extensions/resolve-node sync :peer-state (:id node-data))
        peer-state (:content (extensions/dereference sync state-path))
        state (assoc peer-state :state :dead)]
    (:node (extensions/create-at sync :peer-state (:id peer-state) state))))

(defmethod extensions/plan-job ZooKeeper
  [sync job-id tasks catalog workflow]
  (let [catalog-node (:node (extensions/create-at sync :catalog job-id catalog))
        workflow-node (:node (extensions/create-at sync :workflow job-id workflow))]

    (doseq [task tasks]
      (let [data (serialize-task task job-id catalog-node workflow-node)
            place (:node (extensions/create-at sync :task job-id data))]))
    
    (extensions/create sync :job job-id)  
    job-id))

(defmethod extensions/mark-offered ZooKeeper
  [sync task-node state-path nodes]
  (let [peer-state (extensions/read-place sync state-path)
        complete? (task-complete? sync task-node)]
    (when (and (= (:state peer-state) :idle) (not complete?))
      (let [state (merge peer-state {:state :acking :task-node task-node :nodes nodes})
            next-path (:node (extensions/create-at sync :peer-state (:id peer-state) state))
            task (extensions/read-place sync task-node)
            job-log-record {:job (:task/job-id task) :task (:task/id task)}]
        (:node (extensions/create sync :job-log job-log-record))))))

(defmethod extensions/ack ZooKeeper
  [sync ack-place]
  (let [ack-data (extensions/read-place sync ack-place)
        peer-path (extensions/resolve-node sync :peer-state (:id ack-data))
        peer-state (:content (extensions/dereference sync peer-path))
        complete? (task-complete? sync (:task-node ack-data))]
    ;; Serialize this.
    (if (and (= (:state peer-state) :acking)
             (= (:task-node ack-data) (:task-node peer-state))
             (not complete?))
      (let [state (assoc peer-state :state :active)]
        (:node (extensions/create-at sync :peer-state (:id peer-state) state)))
      (let [err-val {:complete? complete? :state (:state peer-state)
                     :ack-task (:task-node ack-data) :peer-task (:task-node peer-state)}]
        (throw (ex-info "Failed to acknowledge task" err-val))))))

(defmethod extensions/revoke-offer ZooKeeper
  [sync ack-place]
  (let [ack-data (extensions/read-place sync ack-place)
        state-path (extensions/resolve-node sync :peer-state (:id ack-data))
        peer-state (:content (extensions/dereference sync state-path))]
    ;; Serialize this.
    (when (and (= (:state peer-state) :acking)
               (= (:task-node ack-data) (:task-node peer-state)))
      (let [state (assoc (dissoc peer-state :task-node :nodes) :state :dead)]
        (:node (extensions/create-at sync :peer-state (:id peer-state) state))))))

(defmethod extensions/idle-peers ZooKeeper
  [sync]
  (->> (extensions/bucket sync :peer-state)
       (map (partial extensions/dereference sync))
       (filter #(= (:state (:content %)) :idle))))

(defmethod extensions/seal-resource? ZooKeeper
  [sync exhaust-place]
  (let [node-data (extensions/read-place sync exhaust-place)
        state-path (extensions/resolve-node sync :peer-state (:id node-data))
        peer-state (:content (extensions/dereference sync state-path))
        state (assoc peer-state :state :sealing)]
    (extensions/create-at sync :peer-state (:id peer-state) state)
    (let [n-active (n-active-peers sync (:task-node peer-state))
          n (n-peers sync (:task-node peer-state))]
      {:seal? (or (= n 1) (zero? n-active))
       :seal-node (:node/seal (:nodes peer-state))})))

(defmethod extensions/complete ZooKeeper
  [sync complete-node]
  (let [node-data (extensions/read-place sync complete-node)
        state-path (extensions/resolve-node sync :peer-state (:id node-data))
        peer-state (:content (extensions/dereference sync state-path))
        complete? (task-complete? sync (:task-node peer-state))
        n (n-peers sync (:task-node peer-state))]
    (if (and (not complete?)
             (= (:task-node node-data) (:task-node peer-state))
             (or (= (:state peer-state) :active)
                 (= (:state peer-state) :sealing)))
      (let [state (assoc (dissoc peer-state :task-node :nodes) :state :idle)]
        (extensions/create-at sync :peer-state (:id node-data) state)
        (when (= n 1)
          (complete-task sync (:task-node peer-state)))
        {:n-peers n :peer-state peer-state})
      (throw (ex-info "Failed to complete task" {:complete? complete? :n n})))))

(defn incomplete-job-ids [sync]
  (let [job-nodes (extensions/bucket sync :job)
        incompletes
        (filter
         (fn [job-node]
           (let [tasks (extensions/bucket-at sync :task job-node)]
             (seq
              (filter
               (fn [task]
                 (let [completion (str task complete-marker)]
                   (not (extensions/place-exists? sync completion))))
               tasks))))
         job-nodes)]
    (sort-by (fn [job-node] (extensions/creation-time sync job-node)) incompletes)))

(defn last-offered-job [sync]
  (:content (extensions/dereference sync (extensions/resolve-node sync :job-log))))

(defn job-candidate-seq [sync job-seq last-offered]
  (if (nil? last-offered)
    job-seq
    (->> (cycle job-seq)
         (drop (inc (.indexOf job-seq (extensions/resolve-node sync :job (:job last-offered)))))
         (take (count job-seq)))))

(defn sort-tasks-by-phase [sync tasks]
  (sort-by (fn [t] (:task/phase (extensions/read-place sync t))) tasks))

(defn find-incomplete-tasks [sync tasks]
  (filter (fn [task]
            (and (not (completed-task? task))
                 (not (task-complete? sync task))))
          tasks))

(defn find-active-task-ids [sync tasks]
  (filter (fn [task] (>= (n-peers sync task) 1)) tasks))

(defn next-inactive-task [sync job-node]
  (let [task-path (extensions/resolve-node sync :task job-node)
        task-nodes (extensions/children sync task-path)
        incomplete-tasks (find-incomplete-tasks sync task-nodes)
        sorted-task-nodes (sort-tasks-by-phase sync incomplete-tasks)
        active-task-ids (find-active-task-ids sync sorted-task-nodes)]
    (filter (fn [t] (not (some #{t} active-task-ids))) sorted-task-nodes)))

(defn find-incomplete-concurrent-tasks [sync job-node]
  (let [task-path (extensions/resolve-node sync :task job-node)
        task-nodes (extensions/children sync task-path)
        incomplete-tasks (find-incomplete-tasks sync task-nodes)]
    (filter (fn [task]
              (let [task-data (extensions/read-place sync task)]
                (= (:task/consumption task-data) :concurrent)))
            incomplete-tasks)))

(defn sort-tasks-by-peer-count [sync tasks]
  (sort-by (partial n-peers sync) tasks))

(defn next-active-task [sync job-node]
  (let [incomplete-tasks (find-incomplete-concurrent-tasks sync job-node)]
    (sort-tasks-by-peer-count sync incomplete-tasks)))

(defmethod extensions/next-tasks ZooKeeper
  [sync]
  (let [job-seq (job-candidate-seq sync (incomplete-job-ids sync) (last-offered-job sync))
        inactive-candidates (mapcat (partial next-inactive-task sync) job-seq)
        active-candidates (mapcat (partial next-active-task sync) job-seq)]
    (concat (filter identity inactive-candidates)
            (filter identity active-candidates))))

