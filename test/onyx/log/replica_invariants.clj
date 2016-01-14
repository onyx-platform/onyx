(ns onyx.log.replica-invariants
  (:require [onyx.log.commands.common :as common]
            [taoensso.timbre :refer [info]]
            [clojure.test :refer :all]))

(defn job-id-invariants 
  [{:keys [task-slot-ids peers allocations peer-state peer-sites prepared accepted pairs jobs] 
    :as replica}]
  (let [replica-job-ids (mapcat (fn [k] 
                                  (keys (replica k))) 
                                [:allocations :task-slot-ids 
                                 :ackers :task-metadata])]
    (testing "all job ids are valid active jobs"
      (is (empty? (remove (set jobs) replica-job-ids))))))

(defn peer-id-invariants 
  [{:keys [task-slot-ids peers allocations peer-state peer-sites prepared accepted pairs jobs] 
    :as replica}]
  (let [peer-allocations (common/allocations->peers allocations)
        slot-id-peers (mapcat keys (vals (reduce merge (vals task-slot-ids))))]
    (do
      (mapv (fn [coll] 
              (is (empty? (remove (set peers) coll))))
            (map keys [peer-state peer-sites prepared accepted pairs])))

    (testing "slot ids only allocated once" 
      (is (= slot-id-peers (distinct slot-id-peers))))
    (is (empty? (remove (set peers) slot-id-peers)))
    (is (empty? (reduce dissoc peer-allocations peers)))))

(defn standard-invariants [replica]
  (job-id-invariants replica)
  (peer-id-invariants replica))
