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

(defn n-peers [sync task-node]
  (let [peers (extensions/bucket sync :peer-state)
        counts
        (reduce
         (fn [all peer]
           (let [content (:content (extensions/dereference sync peer))]
             (if (= task-node (:task-node content))
               (assoc all (:state content) (inc (get all (:state content) 0)))
               all)))
         {}
         peers)]
    ;; Avoid NPE on addition
    (-> counts
        (assoc :acking (get counts :acking 0))
        (assoc :active (get counts :active 0))
        (assoc :waiting (get counts :waiting 0))
        (assoc :sealing (get counts :sealing 0)))))

(defn release-waiting-nodes! [sync task-node]
  (doseq [peer (extensions/bucket sync :peer-state)]
    (let [content (:content (extensions/dereference sync peer))]
      (when (and (= task-node (:task-node content))
                 (= (:state content) :waiting))
        (let [status (assoc content :state :idle)]
          (extensions/create-at sync :peer-state (:id content) status))))))

(defmethod extensions/mark-peer-born ZooKeeper
  [sync peer-node]
  (let [peer-data (extensions/read-node sync peer-node)
        state {:id (:id peer-data) :peer-node (:peer-node peer-data) :state :idle}
        state-path (extensions/resolve-node sync :peer-state (:id state))]
    (if-not (seq (extensions/children sync state-path))
      (:node (extensions/create-at sync :peer-state (:id peer-data) state))
      (throw (ex-info "Tried to add a duplicate peer" {:peer-node peer-node})))))

(defmethod extensions/mark-peer-dead ZooKeeper
  [sync peer-node]
  (let [node-data (extensions/read-node sync peer-node)
        state-path (extensions/resolve-node sync :peer-state (:id node-data))
        peer-state (:content (extensions/dereference sync state-path))
        state (assoc peer-state :state :dead)]
    (when (and (:task-node peer-state) (= (:state peer-state) :sealing))
      (release-waiting-nodes! sync (:task-node peer-state)))
    (:node (extensions/create-at sync :peer-state (:id peer-state) state))))

(defmethod extensions/plan-job ZooKeeper
  [sync job-id tasks catalog workflow]
  (let [catalog-node (:node (extensions/create-at sync :catalog job-id catalog))
        workflow-node (:node (extensions/create-at sync :workflow job-id workflow))]

    (doseq [task tasks]
      (let [data (serialize-task task job-id catalog-node workflow-node)]
        (extensions/create-at sync :task job-id data)))
    
    (extensions/create sync :job job-id)  
    job-id))

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

(defmethod extensions/seal-resource? ZooKeeper
  [sync exhaust-node]
  (let [node-data (extensions/read-node sync exhaust-node)
        peers (n-peers sync (:task-node node-data))
        n (+ (:acking peers) (:active peers) (:waiting peers) (:sealing peers))
        state-path (extensions/resolve-node sync :peer-state (:id node-data))
        peer-state (:content (extensions/dereference sync state-path))
        same-task? (= (:task-node peer-state) (:task-node node-data))
        complete? (task-complete? sync (:task-node node-data))
        state (if (and (>= (:waiting peers) (dec n))
                       (zero? (:sealing peers))
                       (= (:state peer-state) :active))
                (assoc peer-state :state :sealing)
                (assoc peer-state :state :waiting))]

    (when (and same-task?
               (and (not (and (= (:state peer-state) :sealing)
                              (= (:state state) :waiting)))
                    (not (= (:state peer-state) (:state state))))
               (not (and (= (:state state) :sealing) complete?)))
      (extensions/create-at sync :peer-state (:id node-data) state))

    {:seal? (boolean (and (or (= (:state peer-state) :sealing)
                              (= (:state state) :sealing))
                          same-task?
                          (not complete?)))
     :sealed? complete?
     :seal-node (:node/seal (:nodes node-data))
     :exhaust-node (:node/exhaust (:nodes node-data))}))
  
(defmethod extensions/complete ZooKeeper
  [sync complete-node cooldown-node cb]
  (let [node-data (extensions/read-node sync complete-node)
        state-path (extensions/resolve-node sync :peer-state (:id node-data))
        peer-state (:content (extensions/dereference sync state-path))
        complete? (task-complete? sync (:task-node peer-state))
        peers (n-peers sync (:task-node peer-state))
        n (+ (:acking peers) (:active peers))]

    (if (and (some #{(:state peer-state)} #{:sealing :dead})
             (= (:task-node peer-state) (:task-node node-data)))
      (let [state (assoc peer-state :state :idle)]
        (when (= (:state peer-state) :sealing)
          (extensions/create-at sync :peer-state (:id node-data) state))

        (when (and (zero? n) (not complete?))
          (release-waiting-nodes! sync (:task-node peer-state))
          (complete-task sync (:task-node peer-state))
          (extensions/write-node sync cooldown-node {:completed? true}))
        
        {:n-peers n :peer-state peer-state})
      (do (extensions/write-node sync cooldown-node {:completed? true})
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
  (let [states (extensions/bucket sync :peer-state)]
    (filter
     (fn [task]
       (let [peers (n-peers sync task)]
         (>= (+ (:acking peers) (:active peers) (:sealing peers)) 1)))
     tasks)))

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
  (sort-by
   (fn [task]
     (let [peers (n-peers sync task)]
       (+ (:acking peers) (:active peers) (:sealing peers))))
   (sort tasks)))

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

