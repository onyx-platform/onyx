(ns onyx.log.generative-group-test
  (:require [onyx.log.generators :as log-gen]
            [onyx.extensions :as extensions]
            [onyx.api :as api]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test :refer :all]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]))

(deftest varied-joining
  (checking
    "Checking peers from different groups join up"
    (times 50)
    [n-groups (gen/resize 10 gen/s-pos-int)
     n-vpeers (gen/resize 20 gen/s-pos-int)
     {:keys [replica log peer-choices]}
     (log-gen/apply-entries-gen
       (gen/return
         {:replica {:job-scheduler :onyx.job-scheduler/greedy
                    :messaging {:onyx.messaging/impl :dummy-messenger}}
          :message-id 0
          :entries (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids n-groups n-vpeers))
          :log []
          :peer-choices []}))]
    (let [total-vpeers (* n-groups n-vpeers)]
      (is (= n-groups (count (:groups replica))))
      (is (= total-vpeers (count (:peers replica))))
      (is (zero? (count (:orphaned-peers replica))))
      (is (zero? (count (:aborted replica))))
      (is (zero? (count (keys (:prepared replica)))))
      (is (zero? (count (keys (:accepted replica)))))
      (is (= (repeat n-groups n-vpeers)
             (map count (vals (:groups-index replica)))))
      (is (= total-vpeers (count (keys (:groups-reverse-index replica))))))))

(deftest group-leaves
  (checking
   "Checking vpeers leave when group leaves"
   (times 50)
   [n-groups (gen/resize 10 gen/s-pos-int)
    n-vpeers (gen/resize 10 gen/s-pos-int)
    {:keys [replica log peer-choices]}
    (log-gen/apply-entries-gen
     (gen/return
      {:replica {:job-scheduler :onyx.job-scheduler/greedy
                 :messaging {:onyx.messaging/impl :dummy-messenger}}
       :message-id 0
       :entries (-> (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids n-groups n-vpeers))
                    (assoc :leave {:predicate (fn [replica entry]
                                                (some #{:g1} (:groups replica)))
                                   :queue [{:fn :group-leave-cluster
                                            :args {:id :g1}}]}))
       :log []
       :peer-choices []}))]
   (let [total-vpeers (* (dec n-groups) n-vpeers)]
     (is (= (dec n-groups) (count (:groups replica))))
     (is (= total-vpeers (count (:peers replica))))
     (is (zero? (count (:aborted replica))))
     (is (zero? (count (:orphaned-peers replica))))
     (is (zero? (count (keys (:prepared replica)))))
     (is (zero? (count (keys (:accepted replica)))))
     (is (= (repeat (dec n-groups) n-vpeers)
            (map count (vals (:groups-index replica)))))
     (is (= total-vpeers (count (keys (:groups-reverse-index replica))))))))

(deftest multiple-group-leaves
  (checking
   "Checking vpeers leave when multiple groups leave"
   (times 50)
   [n-groups (gen/fmap inc (gen/resize 10 gen/s-pos-int))
    n-vpeers (gen/resize 10 gen/s-pos-int)
    {:keys [replica log peer-choices]}
    (log-gen/apply-entries-gen
     (gen/return
      {:replica {:job-scheduler :onyx.job-scheduler/greedy
                 :messaging {:onyx.messaging/impl :dummy-messenger}}
       :message-id 0
       :entries (-> (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids n-groups n-vpeers))
                    (assoc :leave-1 {:predicate (fn [replica entry]
                                                  (some #{:g1} (:groups replica)))
                                     :queue [{:fn :group-leave-cluster
                                              :args {:id :g1}}]})
                    (assoc :leave-2 {:predicate (fn [replica entry]
                                                  (some #{:g2} (:groups replica)))
                                     :queue [{:fn :group-leave-cluster
                                              :args {:id :g2}}]}))
       :log []
       :peer-choices []}))]
   (let [total-vpeers (* (- n-groups 2) n-vpeers)]
     (is (= (- n-groups 2) (count (:groups replica))))
     (is (= total-vpeers (count (:peers replica))))
     (is (zero? (count (:aborted replica))))
     (is (zero? (count (:orphaned-peers replica))))
     (is (zero? (count (keys (:prepared replica)))))
     (is (zero? (count (keys (:accepted replica)))))
     (is (= (repeat (- n-groups 2) n-vpeers)
            (map count (vals (:groups-index replica)))))
     (is (= total-vpeers (count (keys (:groups-reverse-index replica))))))))

(deftest all-groups-leave
  (checking
   "Checking clean slate when all groups join and leave"
   (times 50)
   [n-groups (gen/resize 10 gen/s-pos-int)
    n-vpeers (gen/resize 10 gen/s-pos-int)
    {:keys [replica log peer-choices]}
    (let [gs
          (for [g (range 1 (inc n-groups))]
            (let [group-id (keyword (str "g" g))]
              [(keyword (str "leave-" g))
               {:predicate (fn [replica entry]
                             (some #{group-id} (:groups replica)))
                :queue [{:fn :group-leave-cluster
                         :args {:id group-id}}]}]))
          join-entries (log-gen/generate-join-queues
                        (log-gen/generate-group-and-peer-ids n-groups n-vpeers))
          entries (reduce (fn [result [group entries]]
                            (assoc result group entries))
                          join-entries
                          gs)]
      (log-gen/apply-entries-gen
       (gen/return
        {:replica {:job-scheduler :onyx.job-scheduler/greedy
                   :messaging {:onyx.messaging/impl :dummy-messenger}}
         :message-id 0
         :entries entries
         :log []
         :peer-choices []})))]
   (is (zero? (count (:groups replica))))
   (is (zero? (count (:peers replica))))
   (is (zero? (count (:aborted replica))))
   (is (zero? (count (:orphaned-peers replica))))
   (is (zero? (count (keys (:prepared replica)))))
   (is (zero? (count (keys (:accepted replica)))))
   (is (empty? (:peer-state replica)))
   (is (empty? (:peer-sites replica)))
   (is (empty? (:groups-index replica)))
   (is (empty? (:groups-reverse-index replica)))))
