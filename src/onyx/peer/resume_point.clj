(ns onyx.peer.resume-point
  (:require [onyx.compression.nippy :refer [checkpoint-compress checkpoint-decompress]]
            [onyx.extensions :as extensions]
            [onyx.state.protocol.db :as db]
            [onyx.peer.window-state :as ws]
            [onyx.checkpoint :as checkpoint]
            [onyx.state.serializers.checkpoint :as cpenc]
            [taoensso.timbre :refer [debug info error warn trace fatal]]
            [onyx.windowing.window-compile :as wc]))

(defn coordinates->input-resume-point
  [{:keys [onyx.core/task-id onyx.core/job-id onyx.core/resume-point onyx.core/tenancy-id] :as event}
   latest-coordinates]
  (if latest-coordinates
    (merge latest-coordinates
           {:tenancy-id tenancy-id
            :job-id job-id
            :task-id task-id
            :slot-migration :direct})
    (:input resume-point)))

(defn coordinates->output-resume-point
  [{:keys [onyx.core/task-id onyx.core/job-id onyx.core/resume-point onyx.core/tenancy-id] :as event}
   latest-coordinates]
  (if latest-coordinates
    (merge latest-coordinates
           {:tenancy-id tenancy-id
            :job-id job-id
            :task-id task-id
            :slot-migration :direct})
    (:output resume-point)))

(defn coordinates->windows-resume-point
  [{:keys [onyx.core/windows onyx.core/task-id
           onyx.core/job-id onyx.core/resume-point
           onyx.core/tenancy-id] :as event}
   latest-coordinates]
  (if latest-coordinates
    (reduce (fn [m {:keys [window/id]}]
              (assoc m id (merge latest-coordinates
                                 {:mode :resume
                                  :tenancy-id tenancy-id
                                  :job-id job-id
                                  :task-id task-id
                                  :window-id id
                                  :slot-migration :direct})))
            {}
            windows)
    (:windows resume-point)))

(defn read-checkpoint
  [{:keys [onyx.core/storage onyx.core/monitoring] :as event} checkpoint-type
   {:keys [tenancy-id job-id task-id replica-version epoch] :as coordinates}
   slot-id]
  (if coordinates
    (let [bs (checkpoint/read-checkpoint storage tenancy-id job-id replica-version epoch
                                         task-id slot-id checkpoint-type)]
      (.addAndGet ^java.util.concurrent.atomic.AtomicLong (:checkpoint-read-bytes monitoring) (count bs))
      bs)))

(defn resume-point->coordinates [resume-point]
  (select-keys resume-point [:tenancy-id :job-id :task-id
                             :replica-version :epoch]))

(defn windows-to-fetch [{:keys [onyx.core/slot-id] :as event} resume-point task-id]
  (->> resume-point
       (vals)
       (remove #(= :initialize (:mode %)))
       (group-by resume-point->coordinates)))

(defn state-reindex [old-state-indices new-state-indices]
  (into {} 
        (map (fn [k]
               (let [old (get old-state-indices k)
                     new (get new-state-indices k)] 
                 (if (and old new)
                   [old new]
                   (throw (ex-info "Missing resume point mapping an expected window." 
                                   {:state-index new}))))) 
             (keys new-state-indices))))

(defn recover-windows
  [{:keys [onyx.core/windows onyx.core/triggers onyx.core/task-id onyx.core/slot-id onyx.core/task-map] :as event}
   state-store
   recover-coordinates]
  (let [state-indices (ws/state-indices event)
        resume-mapping (coordinates->windows-resume-point event recover-coordinates)
        aggregated-mappings (windows-to-fetch event resume-mapping task-id)]
    (run! (fn [[coordinates mappings]]
            (let [bs (read-checkpoint event :windows coordinates slot-id)
                  decoder (cpenc/new-decoder bs)
                  schema-version (cpenc/get-schema-version decoder)
                  metadata-bs (cpenc/get-metadata decoder)
                  _ (when-not (= schema-version cpenc/current-schema-version)
                      (throw (ex-info "Incompatible schema for state checkpoint. Please rebuild the state as this migration is not supported."
                                      {:current cpenc/current-schema-version
                                       :retrieved schema-version})))
                  metadata (checkpoint-decompress metadata-bs)
                  reindex (state-reindex (:state-indexes metadata) state-indices)]
              (db/restore! state-store decoder reindex)))
          aggregated-mappings)
    (mapv (fn [{:keys [window/id] :as window}]
            (wc/build-window-executor window triggers state-store state-indices task-map))
          windows)))

(defn recover-output [event recover-coordinates]
  (if-let [resume-mapping (coordinates->output-resume-point event recover-coordinates)]
    (let [{:keys [slot-migration]} resume-mapping
          ;; TODO, support slot-id mappings
          _ (assert (= slot-migration :direct))
          {:keys [onyx.core/slot-id]} event]
      (checkpoint-decompress (read-checkpoint event :output resume-mapping slot-id)))))

(defn recover-input [event recover-coordinates]
  (if-let [resume-mapping (coordinates->input-resume-point event recover-coordinates)]
    (let [{:keys [slot-migration]} resume-mapping
          ;; TODO, support slot-id mappings
          _ (assert (= slot-migration :direct))
          {:keys [onyx.core/slot-id]} event]
      (checkpoint-decompress (read-checkpoint event :input resume-mapping slot-id)))))
