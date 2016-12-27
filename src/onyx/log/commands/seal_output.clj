(ns onyx.log.commands.seal-output
  (:require [clojure.core.async :refer [>!!]]
            [clojure.set :refer [union]]
            [schema.core :as s]
            [onyx.log.curator :as zk]
            [onyx.schema :refer [Replica LogEntry Reactions ReplicaDiff State]]
            [onyx.scheduling.common-job-scheduler :refer [reconfigure-cluster-workload]]
            [onyx.extensions :as extensions :refer [write-checkpoint-coordinate 
                                                    assume-checkpoint-coordinate]]
            [taoensso.timbre :refer [info]]
            [onyx.log.commands.common :as common])
  (:import [org.apache.zookeeper KeeperException$BadVersionException]))

(defn required-sealed-output-slots [replica job]
  (let [output-tasks (get-in replica [:output-tasks job])] 
    (->> (get-in replica [:task-slot-ids job])
         (filter (fn [[task-id _]] 
                   (get output-tasks task-id)))
         (mapcat (fn [[task-id v]]
                   (map (fn [slot-id]
                          [task-id slot-id])
                        (vals v)))))))

(defn job-completed-coordinates [replica job]
  (let [all (required-sealed-output-slots replica job)
        sealed (get-in replica [:sealed-outputs job])
        replica-versions (map first (vals sealed))
        epochs (set (map second (vals sealed)))]
    (when (and (= (set all) (set (keys sealed)))
               ;; All have to have sent out an seal output on the same replica
               ;; Otherwise a rewind may have occurred, invalidating the seal
               (= #{(get-in replica [:allocation-version job])} (set replica-versions)))
      (assert (= 1 (count (set (map second (vals sealed))))) 
              ["Sealing outputs did not agree on completion epoch" sealed])
      [(first replica-versions) (first epochs)])))

(s/defmethod extensions/apply-log-entry :seal-output :- Replica
  [{:keys [args]} :- LogEntry replica]
  (let [job (:job-id args)] 
    (if (some #{job} (:jobs replica)) 
      (let [new-replica (update-in replica 
                                   [:sealed-outputs job] 
                                   assoc 
                                   [(:task-id args) (:slot-id args)] 
                                   [(:replica-version args) (:epoch args)])]
        (if-let [coordinates (job-completed-coordinates new-replica job)]
          (update-in new-replica [:completed-job-coordinates] assoc job coordinates)
          new-replica))
      replica)))

(s/defmethod extensions/replica-diff :seal-output :- ReplicaDiff
  [{:keys [args]} old new]
  (let [completed-coordinates (get-in new [:completed-job-coordinates (:job-id args)])] 
    {:job-sealed? (boolean completed-coordinates)
     :completed-job-coordinates completed-coordinates
     :job (:job-id args) 
     :task (:task-id args)}))

(s/defmethod extensions/reactions [:seal-output :peer] :- Reactions
  [{:keys [args message-id]} old new diff peer-args]
  ;; emit additional complete job log entry to clean up the job
  ;; which will occur after the fire-side-effects! call writes the final
  ;; state coordinates
  (if (and (:job-sealed? diff)
           (= (get-in new [:coordinators (:job-id args)]) (:id peer-args)))
    [{:fn :complete-job
      :args {:job-id (:job-id args)}}]
    []))

(s/defmethod extensions/fire-side-effects! [:seal-output :peer] :- State
  [{:keys [args message-id]} old new diff state]
  (let [job-id (:job-id args)] 
    (when (and (:job-sealed? diff)
               (= (get-in new [:coordinators job-id]) (:id state)))
      (loop []
        (when-not (try 
                   (let [{:keys [log opts]} state
                         tenancy-id (:onyx/tenancy-id opts) 
                         coords (get-in new [:completed-job-coordinates job-id])
                         ver (assume-checkpoint-coordinate log tenancy-id job-id)]
                     (write-checkpoint-coordinate log tenancy-id job-id coords ver)
                     true)
                   (catch KeeperException$BadVersionException bve
                     (info "Failed coordinates write, retrying." bve)
                     false))
          (recur)))))
  state)
