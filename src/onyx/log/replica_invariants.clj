(ns onyx.log.replica-invariants
  (:require [onyx.log.commands.common :as common]
            [taoensso.timbre :refer [info]]))

(defn allocations-invariant 
  [{:keys [peers allocations] 
    :as replica}]
  (let [peer-allocations (common/allocations->peers allocations)]
    (empty? (reduce dissoc peer-allocations peers))))

(defn slot-id-invariant
  [{:keys [task-slot-ids peers] 
    :as replica}]
  (let [slot-id-peers (mapcat keys (vals (reduce merge (vals task-slot-ids))))]
    (and (= slot-id-peers (distinct slot-id-peers))
         (empty? (remove (set peers) slot-id-peers)))))

(defn all-groups-invariant
  [{:keys [groups prepared accepted pairs] 
    :as replica}]
  (let [all-groups (set (mapcat keys [prepared accepted pairs]))]
    (or (= 1 (count groups)) ;; back to a single group so nothing in pairs
        (empty? (remove all-groups groups)))))

(defn peer-site-invariant
  [{:keys [peer-sites peers prepared accepted pairs] 
    :as replica}]
  (let [all-peers (set (concat peers (mapcat keys [prepared accepted pairs])))]
    (empty? (reduce dissoc peer-sites all-peers))))

(defn all-tasks-have-non-zero-peers
  [replica]
  (every? (fn [[job allocation]]
            (or (every? empty? (vals allocation))
                (every? not-empty (vals allocation)))) 
          (:allocations replica)))

(defn all-jobs-have-coordinator
  [replica]
  (every? (fn [[job job-allocations]]
            ;; if no peers allocated, no coordinator should be allocated
            (or (and (empty? (apply concat (vals job-allocations)))
                     (nil? (get-in replica [:coordinators job])))
                ;; if peers are allocated, a coordinator should be allocated
                (get-in replica [:coordinators job]))) 
          (:allocations replica)))

(defn no-extra-coordinators
  [replica]
  (= (set (keys (:coordinators replica)))
     (set (keys (:allocations replica)))))

(defn short-identifiers-correct [replica]
  (not (some 
        (fn [[{:keys [src-peer-id job-id dst-task-id]} short-id]]
          (or (nil? (get-in replica [:allocations job-id dst-task-id]))
              (nil? (some #{src-peer-id} (:peers replica)))
              (some #{job-id} (:completed-jobs replica))
              (some #{job-id} (:killed-jobs replica))))
        (:message-short-ids replica))))

(defn all-coordinators-exist 
  [replica]
  (every? (fn [coord]
            (or (nil? coord)
                (some #{coord} (:peers replica))))
          (vals (:coordinators replica))))

(defn version-invariant
  [{:keys [version] 
    :as replica}]
  (integer? version))

(defn active-job-invariant
  [{:keys [task-slot-ids peers allocations peer-sites prepared accepted pairs jobs] 
    :as replica}]
  (let [replica-job-ids (mapcat (fn [k] 
                                  (keys (replica k))) 
                                [:allocations :task-metadata :in->out])]
    (empty? (remove (set jobs) replica-job-ids))))

(defn group-index-keys-never-nil [replica]
  (every? (comp not nil?) (keys (:groups-index replica))))

(defn group-index-vals-never-nil [replica]
  (let [peers (reduce into [] (vals (:groups-index replica)))]
    (every? (comp not nil?) peers)))

(defn all-peers-are-group-indexed [replica]
  (let [peers (reduce into [] (vals (:groups-index replica)))]
    (= (into (set (:peers replica))
             (apply concat (vals (:orphaned-peers replica))))
       (set peers))))

(defn all-peers-are-reverse-group-indexed [replica]
  (every?
   (fn [x] (not (nil? (get-in replica [:groups-reverse-index x]))))
   (into (set (:peers replica))
         (set (apply concat (vals (:orphaned-peers replica)))))))
