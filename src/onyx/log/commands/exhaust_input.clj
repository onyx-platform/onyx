(ns onyx.log.commands.exhaust-input
  (:require [clojure.core.async :refer [>!!]]
            [clojure.set :refer [union]]
            [schema.core :as s]
            [onyx.schema :refer [Replica LogEntry Reactions ReplicaDiff State]]
            [onyx.scheduling.common-job-scheduler :refer [reconfigure-cluster-workload]]
            [onyx.extensions :as extensions]
            [taoensso.timbre :refer [info]]
            [onyx.log.commands.common :as common]))

(defn required-exhausted-task-slots [replica job]
  (let [input-tasks (get-in replica [:input-tasks job])] 
    (->> (get-in replica [:task-slot-ids job])
         (filter (fn [[task-id _]] 
                   (get input-tasks task-id)))
         (mapcat (fn [[task-id v]]
                   (map (fn [slot-id]
                          [task-id slot-id])
                        (vals v)))))))

(defn all-inputs-exhausted? [replica job]
  (println "All inputs exhausted?"
           (:exhausted-inputs replica)
           (let [all (required-exhausted-task-slots replica job)
                 exhausted (get-in replica [:exhausted-inputs job])]
             (and (= (set all) (set (keys exhausted)))
                  ;; All have to have sent out an exhaust input on the same replica
                  ;; Otherwise a rewind may have occurred, invalidating the exhaustion
                  (= #{(get-in replica [:allocation-version job])} (set (vals exhausted)))))        
           )
  (let [all (required-exhausted-task-slots replica job)
        exhausted (get-in replica [:exhausted-inputs job])]
    (and (= (set all) (set (keys exhausted)))
         ;; All have to have sent out an exhaust input on the same replica
         ;; Otherwise a rewind may have occurred, invalidating the exhaustion
         (= #{(get-in replica [:allocation-version job])} (set (vals exhausted))))))

(s/defmethod extensions/apply-log-entry :exhaust-input :- Replica
  [{:keys [args]} :- LogEntry replica]
  (println "Apply log entry" args)
  (let [job (:job-id args)] 
    (if (some #{job} (:jobs replica)) 
      (let [new-replica (update-in replica [:exhausted-inputs job] assoc [(:task-id args) (:slot-id args)] (:replica-version args))]
        (if (all-inputs-exhausted? new-replica job)
          (let [peers (reduce into [] (vals (get-in replica [:allocations job])))]
            (-> new-replica
                (update-in [:exhausted-inputs] dissoc job)
                (update-in [:sealed-outputs] dissoc job)
                (update-in [:jobs] (fn [coll] (remove (partial = job) coll)))
                (update-in [:jobs] vec)
                (update-in [:completed-jobs] conj job)
                (update-in [:completed-jobs] vec)
                (update-in [:coordinators] dissoc job)
                (update-in [:task-metadata] dissoc job)
                (update-in [:task-slot-ids] dissoc job)
                (update-in [:allocations] dissoc job)
                (update-in [:peer-state] merge (into {} (map (fn [p] {p :idle}) peers)))
                (reconfigure-cluster-workload)))
          new-replica))
      replica)))

(s/defmethod extensions/replica-diff :exhaust-input :- ReplicaDiff
  [{:keys [args]} old new]
  {:job-completed? (not= (get-in old [:allocations (:job args)])
                         (get-in new [:allocations (:job args)]))
   :job (:job args) 
   :task (:task args)})

(s/defmethod extensions/reactions [:exhaust-input :peer] :- Reactions
  [{:keys [args]} old new diff peer-args]
  [])

(s/defmethod extensions/fire-side-effects! [:exhaust-input :peer] :- State
  [{:keys [args message-id]} old new diff state]
  (common/start-new-lifecycle old new diff state :job-completed))
