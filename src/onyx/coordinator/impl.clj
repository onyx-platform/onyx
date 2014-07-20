(ns ^:no-doc onyx.coordinator.impl
    (:require [com.stuartsierra.component :as component]
              [taoensso.timbre :as timbre]
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
  (extensions/node-exists? sync (str task-node complete-marker)))

(defn completed-task? [task-node]
  (.endsWith task-node complete-marker))

(defn complete-task [sync task-node]
  (extensions/create-node sync (str task-node complete-marker)))

(defn n-peers [sync task-node states]
  (let [peers (extensions/bucket sync :peer-state)]
    (count
     (filter
      (fn [peer]
        (let [state (:content (extensions/dereference sync peer))]
          (and (= (:task-node state) task-node)
               (some #{(:state state)} states))))
      peers))))


;;; Exception needs to go away, just pass if its already there.
;;; Make it legal to go from :idle -> :idle, not a problem since
;;; writes are serial.
(defmethod extensions/mark-peer-born ZooKeeper
  [sync peer-node]
  (let [peer-data (extensions/read-node sync peer-node)
        state {:id (:id peer-data) :peer-node (:peer-node peer-data) :state :idle}
        state-path (extensions/resolve-node sync :peer-state (:id state))]
    (if-not (seq (extensions/children sync state-path))
      (:node (extensions/create-at sync :peer-state (:id peer-data) state))
      (throw (ex-info "Tried to add a duplicate peer" {:peer-node peer-node})))))

;;; Make it legal to go from :dead -> :dead.
(defmethod extensions/mark-peer-dead ZooKeeper
  [sync peer-node]
  (let [node-data (extensions/read-node sync peer-node)
        state-path (extensions/resolve-node sync :peer-state (:id node-data))
        peer-state (:content (extensions/dereference sync state-path))
        state (assoc peer-state :state :dead)]
    (:node (extensions/create-at sync :peer-state (:id peer-state) state))))


;;; Ensure "tasks" always has the same order.
;;; Ensure that node creation is idempotent.
(defmethod extensions/plan-job ZooKeeper
  [sync job-id tasks catalog workflow]
  (let [catalog-node (:node (extensions/create-at sync :catalog job-id catalog))
        workflow-node (:node (extensions/create-at sync :workflow job-id workflow))]

    (doseq [task tasks]
      (let [data (serialize-task task job-id catalog-node workflow-node)]
        (extensions/create-at sync :task job-id data)))
    
    (extensions/create sync :job job-id)  
    job-id))

;;; job-log-record can't be written twice
;;; Allow :acking -> :acking
(defmethod extensions/mark-offered ZooKeeper
  [sync task-node peer-id nodes]
  (let [peer-state-path (extensions/resolve-node sync :peer-state peer-id)
        peer-head (extensions/dereference sync peer-state-path)
        peer-state (:content peer-head)
        complete? (task-complete? sync task-node)]
    (when (and (= (:state peer-state) :idle) (not complete?))
      (let [state (merge peer-state {:state :acking :task-node task-node :nodes nodes})
            next-path (:node (extensions/create-at sync :peer-state (:id peer-state) state))
            task (extensions/read-node sync task-node)
            job-log-record {:job (:task/job-id task) :task (:task/id task)}]
        (:node (extensions/create sync :job-log job-log-record))))))

;;; Allow :active -> :active peer state transition
;;; Exception can stay, it will just be thrown even though the peer acked
;;; on the last available coordinator.
(defmethod extensions/ack ZooKeeper
  [sync ack-node]
  (let [ack-data (extensions/read-node sync ack-node)
        peer-path (extensions/resolve-node sync :peer-state (:id ack-data))
        peer (extensions/dereference sync peer-path)
        peer-node (:node peer)
        peer-state (:content peer)
        complete? (task-complete? sync (:task-node ack-data))]
    (if (and (= (:state peer-state) :acking)
             (= (:task-node ack-data) (:task-node peer-state))
             (not complete?))
      (let [state (assoc peer-state :state :active)]
        (extensions/create-at sync :peer-state (:id peer-state) state))
      (let [err-val {:complete? complete? :state (:state peer-state)
                     :ack-task (:task-node ack-data) :peer-task (:task-node peer-state)
                     :peer-node peer-node}]
        (throw (ex-info "Failed to acknowledge task" err-val))))))

;;; Allow :revoked -> :revoked
(defmethod extensions/revoke-offer ZooKeeper
  [sync ack-node]
  (let [ack-data (extensions/read-node sync ack-node)
        state-path (extensions/resolve-node sync :peer-state (:id ack-data))
        peer-state (:content (extensions/dereference sync state-path))]
    (when (and (= (:state peer-state) :acking)
               (= (:task-node ack-data) (:task-node peer-state)))
      (let [state (assoc peer-state :state :revoked)]
        (:node (extensions/create-at sync :peer-state (:id peer-state) state))))))

(defmethod extensions/idle-peers ZooKeeper
  [sync]
  (->> (extensions/bucket sync :peer-state)
       (map (partial extensions/dereference sync))
       (filter #(= (:state (:content %)) :idle))))

;;; Allow :sealing -> :sealing
(defmethod extensions/seal-resource? ZooKeeper
  [sync exhaust-node]
  (let [node-data (extensions/read-node sync exhaust-node)
        state-path (extensions/resolve-node sync :peer-state (:id node-data))
        peer-state (:content (extensions/dereference sync state-path))
        state (assoc peer-state :state :sealing)]
    (extensions/create-at sync :peer-state (:id peer-state) state)
    (let [n-active (n-peers sync (:task-node peer-state) #{:active})
          n (n-peers sync (:task-node peer-state) #{:acking :active :sealing})]
      ;; Peer may have died before this event is executed, hence the 0.
      {:seal? (or (<= n 1) (zero? n-active))
       :seal-node (:node/seal (:nodes peer-state))
       :exhaust-node (:node/exhaust (:nodes peer-state))})))

;;; Result should be computed the same as the first time, so its okay
;;; to rewrite the cooldown node, and even add another listener.
(defmethod extensions/complete ZooKeeper
  [sync complete-node cooldown-down cb]
  (let [node-data (extensions/read-node sync complete-node)
        state-path (extensions/resolve-node sync :peer-state (:id node-data))
        peer-state (:content (extensions/dereference sync state-path))
        complete? (task-complete? sync (:task-node peer-state))
        n (n-peers sync (:task-node peer-state) #{:acking :active})]
    (if (or (= (:state peer-state) :active)
            (= (:state peer-state) :sealing)
            (= (:state peer-state) :dead))
      (let [state (assoc peer-state :state :idle)]
        (when-not (= (:state peer-state) :dead)
          (extensions/create-at sync :peer-state (:id node-data) state))

        ;; Peer may have died just after completion, n may again be 0
        (if (and (<= n 1) (not complete?))
          (do (complete-task sync (:task-node peer-state))
              (extensions/write-node sync cooldown-down {:completed? true}))
          (do (extensions/on-change sync complete-node cb)
              (extensions/write-node sync cooldown-down {:completed? false})))
        
        {:n-peers n :peer-state peer-state})
      (do (extensions/write-node sync cooldown-down {:completed? true})
          {:n-peers n :peer-state peer-state}))))

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
                   (not (extensions/node-exists? sync completion))))
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
  (sort-by (fn [t] (:task/phase (extensions/read-node sync t))) tasks))

(defn find-incomplete-tasks [sync tasks]
  (filter (fn [task]
            (and (not (completed-task? task))
                 (not (task-complete? sync task))))
          tasks))

(defn find-active-task-ids [sync tasks]
  (filter (fn [task] (>= (n-peers sync task #{:active :acking :sealing}) 1)) tasks))

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
              (let [task-data (extensions/read-node sync task)]
                (= (:task/consumption task-data) :concurrent)))
            incomplete-tasks)))

(defn sort-tasks-by-peer-count [sync tasks]
  (sort-by #(n-peers sync % #{:active :acking :sealing}) (sort tasks)))

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

