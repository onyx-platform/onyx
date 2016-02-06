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

(defn all-peers-invariant
  [{:keys [peers peer-state peer-sites prepared accepted pairs] 
    :as replica}]
  (let [all-peers (set (mapcat keys [peer-state peer-sites prepared accepted pairs]))]
    (or (= 1 (count peers)) ;; back to a single peer so nothing in pairs
        (empty? (remove all-peers peers)))))

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

(defn active-job-invariant
  [{:keys [task-slot-ids peers allocations peer-state peer-sites prepared accepted pairs jobs] 
    :as replica}]
  (let [replica-job-ids (mapcat (fn [k] 
                                  (keys (replica k))) 
                                [:allocations :task-metadata :exhausted-inputs :exhausted-outputs])]
    (empty? (remove (set jobs) replica-job-ids))))
