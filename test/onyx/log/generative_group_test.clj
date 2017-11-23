(ns onyx.log.generative-group-test
  (:require [onyx.log.generators :as log-gen]
            [onyx.extensions :as extensions]
            [onyx.api :as api]
            [onyx.log.replica :as replica]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.static.planning :as planning]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test :refer :all]
            [onyx.messaging.protocols.messenger :as m]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]))

(def onyx-id (random-uuid))

(def peer-config
  {:onyx/tenancy-id onyx-id
   :onyx.messaging/impl :aeron
   :onyx.peer/try-join-once? true})

(def base-replica 
  (merge replica/base-replica
         {:job-scheduler :onyx.job-scheduler/balanced
          :messaging {:onyx.messaging/impl :aeron}}))

(def messenger-group (m/build-messenger-group peer-config))
(def messenger (m/build-messenger peer-config messenger-group {} nil {}))

(def base-replica 
  (merge replica/base-replica
         {:job-scheduler :onyx.job-scheduler/balanced
          :messaging {:onyx.messaging/impl :aeron}}))

(deftest varied-joining
  (checking
    "Checking peers from different groups join up"
    (times 50)
    [n-groups (gen/resize 10 gen/s-pos-int)
     n-vpeers (gen/resize 20 gen/s-pos-int)
     {:keys [replica log peer-choices]}
     (log-gen/apply-entries-gen
       (gen/return
        {:replica base-replica 
         :message-id 0
         :entries (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids n-groups n-vpeers))
         :log []
         :peer-choices []}))]
    (let [total-vpeers (* n-groups n-vpeers)]
      (is (= n-groups (count (:groups replica))))
      (is (= total-vpeers (count (:peers replica))))
      (is (zero? (count (mapcat val (:orphaned-peers replica)))))
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
      {:replica base-replica 
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
     (is (zero? (count (mapcat val (:orphaned-peers replica)))))
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
      {:replica base-replica 
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
     (is (zero? (count (mapcat val (:orphaned-peers replica)))))
     (is (zero? (count (keys (:prepared replica)))))
     (is (zero? (count (keys (:accepted replica)))))
     (is (= (repeat (- n-groups 2) n-vpeers)
            (map count (vals (:groups-index replica)))))
     (is (= total-vpeers (count (keys (:groups-reverse-index replica))))))))



(deftest multiple-group-leaves-submitted-jobs
  (let [continue-job-id "grouping-job-1"
        grouping-slot-job
        {:workflow [[:a :b] [:b :c]]
         :catalog [{:onyx/name :a
                    :onyx/plugin :onyx.test-helper/dummy-input
                    :onyx/type :input
                    :onyx/medium :dummy
                    :onyx/batch-size 20}

                   {:onyx/name :b
                    :onyx/fn :mock/fn
                    :onyx/type :function
                    :onyx/group-by-kw :mock-key
                    :onyx/flux-policy :continue
                    :onyx/batch-size 20}

                   {:onyx/name :c
                    :onyx/plugin :onyx.test-helper/dummy-output
                    :onyx/type :output
                    :onyx/medium :dummy
                    :onyx/batch-size 20}]
         :task-scheduler :onyx.task-scheduler/balanced}
        job-1-rets (api/create-submit-job-entry
                    continue-job-id
                    peer-config
                    grouping-slot-job
                    (planning/discover-tasks (:catalog grouping-slot-job) (:workflow grouping-slot-job)))] 
    
    (checking
   "Checking vpeers leave when multiple groups leave"
   (times 50)
   [n-groups (gen/fmap inc (gen/resize 10 gen/s-pos-int))
    n-vpeers (gen/resize 10 gen/s-pos-int)
    {:keys [replica log peer-choices]}
    (log-gen/apply-entries-gen
     (gen/return
      {:replica base-replica 
       :message-id 0
       :entries (-> (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids n-groups n-vpeers))
                    (assoc :job-1 {:queue [job-1-rets]})
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
     (is (zero? (count (mapcat val (:orphaned-peers replica)))))
     (is (zero? (count (keys (:prepared replica)))))
     (is (zero? (count (keys (:accepted replica)))))
     (is (= (repeat (- n-groups 2) n-vpeers)
            (map count (vals (:groups-index replica)))))
     (is (= total-vpeers (count (keys (:groups-reverse-index replica)))))))))

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
        {:replica base-replica 
         :message-id 0
         :entries entries
         :log []
         :peer-choices []})))]
   (is (zero? (count (:groups replica))))
   (is (zero? (count (:peers replica))))
   (is (zero? (count (:aborted replica))))
   (is (zero? (count (mapcat val (:orphaned-peers replica)))))
   (is (zero? (count (keys (:prepared replica)))))
   (is (zero? (count (keys (:accepted replica)))))
   (is (empty? (:peer-state replica)))
   (is (empty? (:peer-sites replica)))
   (is (empty? (:groups-index replica)))
   (is (empty? (:groups-reverse-index replica)))))
