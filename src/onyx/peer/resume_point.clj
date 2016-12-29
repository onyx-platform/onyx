(ns onyx.peer.resume-point
  (:require [onyx.extensions :as extensions]
            [onyx.peer.window-state :as ws]
            [onyx.windowing.window-compile :as wc]))

(defn coordinates->windows-resume-point
  [{:keys [onyx.core/windows onyx.core/task-id 
           onyx.core/job-id onyx.core/resume-point
           onyx.core/tenancy-id] :as event}
   latest-coordinates]
  (if latest-coordinates
    (reduce (fn [m {:keys [window/id]}]
              (assoc m id (merge latest-coordinates 
                                 {:tenancy-id tenancy-id
                                  :job-id job-id
                                  :task-id task-id
                                  :slot-migration :direct})))
            {}
            windows)
    (:windows resume-point)))

(defn read-checkpoint
  [{:keys [onyx.core/log] :as event} checkpoint-type 
   {:keys [tenancy-id job-id task-id replica-version epoch] :as coordinates}
   slot-id]
  (if coordinates
    (extensions/read-checkpoint log tenancy-id job-id replica-version epoch
                                task-id slot-id checkpoint-type)))

(defn resume-point->coordinates [resume-point]
  (select-keys resume-point 
               [:tenancy-id :job-id :task-id :replica-version :epoch]))

(defn fetch-windows [{:keys [onyx.core/slot-id] :as event} resume-point task-id]
  (let [unique-fetches (->> resume-point 
                            (vals)
                            (map resume-point->coordinates)
                            (distinct))]
    (reduce (fn [m resume]
              (assoc m resume (read-checkpoint event :state resume slot-id)))    
            {}
            unique-fetches)))

(defn lookup-fetched-state [mapping window-id slot-id fetched]
  (if mapping
    (let [{:keys [slot-migration]} mapping
          ;; TODO, use slot-id mappings
          _ (assert (= slot-migration :direct))]
      (get-in fetched [(resume-point->coordinates mapping) window-id]))))

(defn recover-windows
  [{:keys [onyx.core/windows onyx.core/triggers onyx.core/task-id onyx.core/slot-id
           onyx.core/job-id onyx.core/task-map onyx.core/tenancy-id] :as event}
  recover-coordinates]
  (let [resume-mapping (coordinates->windows-resume-point event recover-coordinates)
        fetched (fetch-windows event resume-mapping task-id)] 
    (mapv (fn [{:keys [window/id] :as window}] 
            (let [window-state (lookup-fetched-state (get resume-mapping id) id slot-id fetched)]
              (cond-> (wc/resolve-window-state window triggers task-map)
                window-state (ws/recover-state window-state))))
          windows)))

(defn coordinates->input-resume-point 
  [{:keys [onyx.core/windows onyx.core/task-id 
           onyx.core/job-id onyx.core/resume-point
           onyx.core/tenancy-id] :as event}
   latest-coordinates]
  (if latest-coordinates
    (merge latest-coordinates 
           {:tenancy-id tenancy-id
            :job-id job-id
            :task-id task-id
            :slot-migration :direct})
    (:input resume-point)))

(defn recover-input [event recover-coordinates]
  (if-let [resume-mapping (coordinates->input-resume-point event recover-coordinates)]
    (let [{:keys [slot-migration]} resume-mapping
          ;; TODO, use slot-id mappings
          _ (assert (= slot-migration :direct))
          {:keys [onyx.core/slot-id]} event]
      (read-checkpoint event :input resume-mapping slot-id))))
