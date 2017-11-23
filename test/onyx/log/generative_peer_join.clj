(ns onyx.log.generative-peer-join
  (:require [onyx.log.generators :as log-gen]
            [onyx.extensions :as extensions]
            [onyx.api :as api]
            [onyx.static.planning :as planning]
            [onyx.test-helper :refer [job-allocation-counts]]
            [onyx.static.uuid :refer [random-uuid]]
            [clojure.set :refer [intersection]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test :refer :all]
            [taoensso.timbre :as timbre :refer [info]]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]))

(def onyx-id (random-uuid))

(def peer-config
  {:onyx/tenancy-id onyx-id
   :onyx.messaging/impl :aeron})

(def job-1-id #uuid "f55c14f0-a847-42eb-81bb-0c0390a88608")

(def job-1
  {:workflow [[:a :b] [:b :c]]
   :catalog [{:onyx/name :a
              :onyx/plugin :onyx.plugin.core-async/input
              :onyx/type :input
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Reads segments from a core.async channel"}

             {:onyx/name :b
              :onyx/fn :mock/fn
              :onyx/type :function
              :onyx/batch-size 20}

             {:onyx/name :c
              :onyx/plugin :onyx.plugin.core-async/output
              :onyx/type :output
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Writes segments to a core.async channel"}]
   :task-scheduler :onyx.task-scheduler/balanced})

(def job-2-id #uuid "5813d2ec-c486-4428-833d-e8373910ae14")

(def job-2
  {:workflow [[:d :e] [:e :f]]
   :catalog [{:onyx/name :d
              :onyx/plugin :onyx.plugin.core-async/input
              :onyx/type :input
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Reads segments from a core.async channel"}

             {:onyx/name :e
              :onyx/fn :mock/fn
              :onyx/type :function
              :onyx/batch-size 20}

             {:onyx/name :f
              :onyx/plugin :onyx.plugin.core-async/output
              :onyx/type :output
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Writes segments to a core.async channel"}]
   :task-scheduler :onyx.task-scheduler/balanced})

(def job-3-id #uuid "58d199e8-4ea4-4afd-a112-945e97235924")

(def job-3
  {:workflow [[:g :h] [:h :i]]
   :catalog [{:onyx/name :g
              :onyx/plugin :onyx.plugin.core-async/input
              :onyx/type :input
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Reads segments from a core.async channel"}

             {:onyx/name :h
              :onyx/fn :mock/fn
              :onyx/type :function
              :onyx/batch-size 20}

             {:onyx/name :i
              :onyx/plugin :onyx.plugin.core-async/output
              :onyx/type :output
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Writes segments to a core.async channel"}]
   :task-scheduler :onyx.task-scheduler/balanced})

(deftest greedy-allocation
  (checking
    "Checking greedy allocation causes all peers to be allocated to one of two jobs"
    (times 50)
    [{:keys [replica log peer-choices]}
     (log-gen/apply-entries-gen
       (gen/return
         {:replica {:job-scheduler :onyx.job-scheduler/greedy
                    :messaging {:onyx.messaging/impl :aeron}}
          :message-id 0
          :entries (assoc (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 1 8))
                          :job-1 {:queue [(api/create-submit-job-entry
                                            job-1-id
                                            peer-config
                                            job-1
                                            (planning/discover-tasks (:catalog job-1) (:workflow job-1)))]}
                          :job-2 {:queue [(api/create-submit-job-entry
                                            job-2-id
                                            peer-config
                                            job-2
                                            (planning/discover-tasks (:catalog job-2) (:workflow job-2)))]})
          :log []
          :peer-choices []}))]
    (let [allocs (vector (apply + (map count (vals (get (:allocations replica) job-1-id))))
                         (apply + (map count (vals (get (:allocations replica) job-2-id)))))]
      (is
        (or (= allocs [0 8])
            (= allocs [8 0]))))))

(deftest greedy-allocation-reallocated
  (checking
    "Checking peers reallocated to other job when killed"
    (times 50)
    [{:keys [replica log peer-choices]}
     (log-gen/apply-entries-gen
       (gen/return
         {:replica {:job-scheduler :onyx.job-scheduler/greedy
                    :messaging {:onyx.messaging/impl :aeron}}
          :message-id 0
          :entries (assoc (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 1 8))
                          :job-1 {:queue [(api/create-submit-job-entry
                                            job-1-id
                                            peer-config
                                            job-1
                                            (planning/discover-tasks (:catalog job-1) (:workflow job-1)))]}
                          :job-2 {:queue [(api/create-submit-job-entry
                                            job-2-id
                                            peer-config
                                            job-2
                                            (planning/discover-tasks (:catalog job-2) (:workflow job-2)))
                                          {:fn :kill-job :args {:job job-2-id}}]})
          :log []
          :peer-choices []}))]
    (is (= 8 (apply + (map count (vals (get (:allocations replica) job-1-id))))))
    (is (= 0 (apply + (map count (vals (get (:allocations replica) job-2-id))))))))

(deftest balanced-task-balancing
  (checking
    "Checking Balanced allocation causes peers to be evenly over tasks"
    (times 50)
    [{:keys [replica log peer-choices]}
     (log-gen/apply-entries-gen
       (gen/return
         {:replica {:job-scheduler :onyx.job-scheduler/balanced
                    :messaging {:onyx.messaging/impl :aeron}}
          :message-id 0
          :entries (assoc (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 1 6))
                          :job-1 {:queue [(api/create-submit-job-entry
                                            job-1-id
                                            peer-config
                                            job-1
                                            (planning/discover-tasks (:catalog job-1) (:workflow job-1)))]}
                          :job-2 {:queue [(api/create-submit-job-entry
                                            job-2-id
                                            peer-config
                                            job-2
                                            (planning/discover-tasks (:catalog job-2) (:workflow job-2)))]})
          :log []
          :peer-choices []}))]
    (is (= [1 1 1] (map count (vals (get (:allocations replica) job-1-id)))))
    (is (= [1 1 1] (map count (vals (get (:allocations replica) job-2-id)))))))

(deftest balanced-allocations-uneven
  (checking
    "Checking Balanced allocation causes peers to be evenly over tasks when the spread is uneven"
    (times 50)
    [{:keys [replica log peer-choices]}
     (log-gen/apply-entries-gen
       (gen/return
         {:replica {:job-scheduler :onyx.job-scheduler/balanced
                    :messaging {:onyx.messaging/impl :aeron}}
          :message-id 0
          :entries (assoc (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 1 7))
                          :job-1 {:queue [(api/create-submit-job-entry
                                            job-1-id
                                            peer-config
                                            job-1
                                            (planning/discover-tasks (:catalog job-1) (:workflow job-1)))]}
                          :job-2 {:queue [(api/create-submit-job-entry
                                            job-2-id
                                            peer-config
                                            job-2
                                            (planning/discover-tasks (:catalog job-2) (:workflow job-2)))]})
          :log []
          :peer-choices []}))]
    (let [j1-allocations (map (fn [t] (get-in replica [:allocations job-1-id t])) (get-in replica [:tasks job-1-id]))
          j2-allocations (map (fn [t] (get-in replica [:allocations job-2-id t])) (get-in replica [:tasks job-2-id]))]
      ;; Since job IDs are reused, we can't know which order they'll be in.
      (is (= #{[1 1 2] [1 1 1]}
             (set (map sort [(map count j1-allocations) (map count j2-allocations)])))))))

(deftest balanced-allocations
  (checking
    "Checking balanced allocation causes peers to be evenly split"
    (times 50)
    [{:keys [replica log peer-choices]}
     (log-gen/apply-entries-gen
       (gen/return
         {:replica {:job-scheduler :onyx.job-scheduler/balanced
                    :messaging {:onyx.messaging/impl :aeron}}
          :message-id 0
          :entries (assoc (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 1 12))
                          :job-1 {:queue [(api/create-submit-job-entry
                                            job-1-id
                                            peer-config
                                            job-1
                                            (planning/discover-tasks (:catalog job-1) (:workflow job-1)))]}
                          :job-2 {:queue [(api/create-submit-job-entry
                                            job-2-id
                                            peer-config
                                            job-2
                                            (planning/discover-tasks (:catalog job-2) (:workflow job-2)))]}
                          :job-3 {:queue [(api/create-submit-job-entry
                                            job-3-id
                                            peer-config
                                            job-3
                                            (planning/discover-tasks (:catalog job-3) (:workflow job-3)))
                                          {:fn :kill-job :args {:job job-3-id}}]})
          :log []
          :peer-choices []}))]
    (is (= [2 2 2] (map count (vals (get (:allocations replica) job-1-id)))))
    (is (= [2 2 2] (map count (vals (get (:allocations replica) job-2-id)))))
    (is (= [] (map count (vals (get (:allocations replica) job-3-id)))))))

(def job-max-peers-id #uuid "f55c14f0-a847-42eb-81bb-0c0390a88608")

(def job-max-peers
  {:workflow [[:a :b] [:b :c]]
   :percentage 100
   :catalog [{:onyx/name :a
              :onyx/plugin :onyx.plugin.core-async/input
              :onyx/type :input
              :onyx/max-peers 1
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Reads segments from a core.async channel"}

             {:onyx/name :b
              :onyx/fn :mock/fn
              :onyx/max-peers 1
              :onyx/type :function
              :onyx/batch-size 20}

             {:onyx/name :c
              :onyx/plugin :onyx.plugin.core-async/output
              :onyx/type :output
              :onyx/max-peers 1
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Writes segments to a core.async channel"}]
   :task-scheduler :onyx.task-scheduler/balanced})

(deftest all-max-peers-allocations-all-schedulers
  (checking
    "Checking job where all tasks have max-peers set"
    (times 50)
    [{:keys [replica log peer-choices]}
     (log-gen/apply-entries-gen
       (gen/fmap 
         (fn [scheduler]
           {:replica {:job-scheduler scheduler
                      :messaging {:onyx.messaging/impl :aeron}}
            :message-id 0
            :entries (assoc (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 1 6))
                            :job-1 {:queue [(api/create-submit-job-entry
                                              job-max-peers-id
                                              (assoc peer-config :onyx.peer/job-scheduler scheduler)
                                              job-max-peers
                                              (planning/discover-tasks (:catalog job-max-peers) (:workflow job-max-peers)))]})
            :log []
            :peer-choices []})
         (gen/elements [:onyx.job-scheduler/balanced :onyx.job-scheduler/greedy :onyx.job-scheduler/percentage])))]
    (is (= (sort [1 1 1]) (sort (map count (vals (get (:allocations replica) job-max-peers-id))))))))

(def job-min-peers-id #uuid "f55c14f0-a847-42eb-81bb-0c0390a88608")

(def job-min-peers
  {:workflow [[:a :b] [:b :c]]
   :percentage 100
   :catalog [{:onyx/name :a
              :onyx/plugin :onyx.plugin.core-async/input
              :onyx/type :input
              :onyx/min-peers 2
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Reads segments from a core.async channel"}

             {:onyx/name :b
              :onyx/fn :mock/fn
              :onyx/min-peers 2
              :onyx/type :function
              :onyx/batch-size 20}

             {:onyx/name :c
              :onyx/plugin :onyx.plugin.core-async/output
              :onyx/type :output
              :onyx/min-peers 2
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Writes segments to a core.async channel"}]
   :task-scheduler :onyx.task-scheduler/balanced})

(deftest all-min-peers-allocations-all-schedulers
  (checking
    "Checking job where all tasks have min-peers set"
    (times 50)
    [{:keys [replica log peer-choices]}
     (log-gen/apply-entries-gen
       (gen/fmap 
         (fn [scheduler]
           {:replica {:job-scheduler scheduler
                      :messaging {:onyx.messaging/impl :aeron}}
            :message-id 0
            :entries (assoc (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 1 6))
                            :job-1 {:queue [(api/create-submit-job-entry
                                              job-min-peers-id
                                              (assoc peer-config :onyx.peer/job-scheduler scheduler)
                                              job-min-peers
                                              (planning/discover-tasks (:catalog job-min-peers) (:workflow job-min-peers)))]})
            :log []
            :peer-choices []})
         (gen/elements [:onyx.job-scheduler/balanced :onyx.job-scheduler/greedy :onyx.job-scheduler/percentage])))]
    (is (= (sort [2 2 2]) (sort (map count (vals (get (:allocations replica) job-min-peers-id))))))))

(deftest job-percentages-balance
  (checking
    "Checking percentages allocation causes peers to be evenly split"
    (times 50)
    [{:keys [replica log peer-choices]}
     (let [percentages-peer-config (assoc peer-config
                                          :onyx.peer/job-scheduler
                                          :onyx.job-scheduler/percentage)]
       (log-gen/apply-entries-gen
         (gen/return
           {:replica {:job-scheduler :onyx.job-scheduler/percentage
                      :messaging {:onyx.messaging/impl :aeron}}
            :message-id 0
            :entries (assoc (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 1 20))
                            :job-1 {:queue [(api/create-submit-job-entry
                                              job-1-id
                                              percentages-peer-config
                                              (assoc job-1 :percentage 30)
                                              (planning/discover-tasks (:catalog job-1)
                                                                       (:workflow job-1)))]}
                            :job-2 {:queue [(api/create-submit-job-entry
                                              job-2-id
                                              percentages-peer-config
                                              (assoc job-2 :percentage 30)
                                              (planning/discover-tasks (:catalog job-2)
                                                                       (:workflow job-2)))]}
                            :job-3 {:queue [(api/create-submit-job-entry
                                              job-3-id
                                              percentages-peer-config
                                              (assoc job-3 :percentage 40)
                                              (planning/discover-tasks (:catalog job-3)
                                                                       (:workflow job-3)))
                                            {:fn :kill-job :args {:job job-3-id}}]})
            :log []
            :peer-choices []})))]
    (is (= [2 2 2] (map count (vals (get (:allocations replica) job-1-id)))))
    (is (= [2 2 2] (map count (vals (get (:allocations replica) job-2-id)))))
    (is (= [] (map count (vals (get (:allocations replica) job-3-id)))))))

(def job-1-pct-tasks
  {:workflow [[:a :b] [:b :c]]
   :catalog [{:onyx/name :a
              :onyx/plugin :onyx.plugin.core-async/input
              :onyx/type :input
              :onyx/percentage 25
              :onyx/max-peers 1
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Reads segments from a core.async channel"}

             {:onyx/name :b
              :onyx/fn :mock/fn
              :onyx/percentage 37.5
              :onyx/type :function
              :onyx/batch-size 20}

             {:onyx/name :c
              :onyx/plugin :onyx.plugin.core-async/output
              :onyx/type :output
              :onyx/percentage 37.5
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Writes segments to a core.async channel"}]
   :task-scheduler :onyx.task-scheduler/percentage})

(def job-2-pct-tasks
  {:workflow [[:d :e] [:e :f]]
   :catalog [{:onyx/name :d
              :onyx/plugin :onyx.plugin.core-async/input
              :onyx/type :input
              :onyx/percentage 25
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Reads segments from a core.async channel"}

             {:onyx/name :e
              :onyx/fn :mock/fn
              :onyx/percentage 25
              :onyx/type :function
              :onyx/batch-size 20}

             {:onyx/name :f
              :onyx/plugin :onyx.plugin.core-async/output
              :onyx/type :output
              :onyx/percentage 50
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Writes segments to a core.async channel"}]
   :task-scheduler :onyx.task-scheduler/percentage})

(def job-3-pct-tasks
  {:workflow [[:g :h] [:h :i]]
   :catalog [{:onyx/name :g
              :onyx/plugin :onyx.plugin.core-async/input
              :onyx/type :input
              :onyx/percentage 25
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Reads segments from a core.async channel"}

             {:onyx/name :h
              :onyx/fn :mock/fn
              :onyx/type :function
              :onyx/percentage 25
              :onyx/batch-size 20}

             {:onyx/name :i
              :onyx/plugin :onyx.plugin.core-async/output
              :onyx/type :output
              :onyx/percentage 50
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Writes segments to a core.async channel"}]
   :task-scheduler :onyx.task-scheduler/percentage})

(deftest percentage-task-allocations
  (checking
    "Checking percentage task allocations"
    (times 50)
    [{:keys [replica log peer-choices]}
     (log-gen/apply-entries-gen
       (gen/return
         {:replica {:job-scheduler :onyx.job-scheduler/balanced
                    :messaging {:onyx.messaging/impl :aeron}}
          :message-id 0
          :entries (assoc (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 1 16))
                          :job-1 {:queue [(api/create-submit-job-entry
                                            job-1-id
                                            peer-config
                                            job-1-pct-tasks
                                            (planning/discover-tasks (:catalog job-1-pct-tasks) (:workflow job-1-pct-tasks)))]}
                          :job-2 {:queue [(api/create-submit-job-entry
                                            job-2-id
                                            peer-config
                                            job-2-pct-tasks
                                            (planning/discover-tasks (:catalog job-2-pct-tasks) (:workflow job-2-pct-tasks)))]}
                          :job-3 {:queue [(api/create-submit-job-entry
                                            job-3-id
                                            peer-config
                                            job-3-pct-tasks
                                            (planning/discover-tasks (:catalog job-3-pct-tasks) (:workflow job-3-pct-tasks)))
                                          {:fn :kill-job :args {:job job-3-id}}]})
          :log []
          :peer-choices []}))]
    (is
      (= [1 4 3]
         (map
           (fn [t]
             (count (get-in replica [:allocations job-1-id t])))
           (get-in replica [:tasks job-1-id]))))
   (is
     (= [2 2 4]
        (map
          (fn [t]
            (count (get-in replica [:allocations job-2-id t])))
          (get-in replica [:tasks job-2-id]))))
   (is (= [] (map count (vals (get (:allocations replica) job-3-id)))))))

(deftest peer-leave-4
  (checking
    "Checking peer leave is correctly performed"
    (times 50)
    [{:keys [replica log peer-choices]}
     (log-gen/apply-entries-gen
       (gen/return
         {:replica {:job-scheduler :onyx.job-scheduler/balanced
                    :messaging {:onyx.messaging/impl :aeron}}
          :message-id 0
          :entries
          (-> (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 1 4))
              (assoc :leave {:predicate (fn [replica entry]
                                          (some #{:g1-p1} (:peers replica)))
                             :queue [{:fn :leave-cluster
                                      :args {:id :g1-p1 :group-id :g1}}]}))
          :log []
          :peer-choices []}))]
    (is (empty? (:accepted replica)))
    (is (empty? (:prepared replica)))
    (is (= 3 (count (:peers replica))))))

(deftest peer-leave
  (checking
    "Checking peer leave is correctly performed"
    (times 50)
    [{:keys [replica log peer-choices]}
     (log-gen/apply-entries-gen
       (gen/return
         {:replica {:job-scheduler :onyx.job-scheduler/balanced
                    :messaging {:onyx.messaging/impl :aeron}}
          :message-id 0
          :entries
          (-> (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 1 3))
              (assoc :leave-anytime {:queue [{:fn :leave-cluster
                                              :args {:id :g1-p1 :group-id :g1}}]}))
          :log []
          :peer-choices []}))]
    (is (empty? (:accepted replica)))
    (is (empty? (:prepared replica)))
    (is (#{2 3} (count (:peers replica))))))

(deftest peer-spurious-notify
  (checking
    "Checking a spurious notify is handled correctly"
    (times 50)
    [{:keys [replica log peer-choices]}
     (log-gen/apply-entries-gen
       (gen/return
         {:replica {:job-scheduler :onyx.job-scheduler/balanced
                    :messaging {:onyx.messaging/impl :aeron}}
          :message-id 0
          :entries
          (-> (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 9 1))
              (assoc :job-1 {:queue [(api/create-submit-job-entry
                                       job-1-id
                                       peer-config
                                       job-1
                                       (planning/discover-tasks (:catalog job-1) (:workflow job-1)))]})
              ;; TODO, generate spurious entries
              (assoc :spurious-prepare {:queue [{:fn :prepare-join-cluster
                                                 :args {:joiner :g6}}]})
              (assoc :spurious-notify {:queue [{:fn :notify-join-cluster
                                                :args {:observer :g5}}]})
              (assoc :spurious-abort {:queue [{:fn :abort-join-cluster
                                               :args {:id :g1}}]})
              (assoc :spurious-accept {:queue [{:fn :accept-join-cluster
                                                :args {:observer :g2
                                                       :subject :g8
                                                       :accepted-observer :g6
                                                       :accepted-joiner :g2}}]})
              (assoc :leave-1 {:queue [{:fn :leave-cluster :args {:id :g1-p1 :group-id :g1}}]})
              (assoc :leave-2 {:queue [{:fn :leave-cluster :args {:id :g2-p1 :group-id :g2}}]}))
          :log []
          :peer-choices []}))]
    (is (empty? (:accepted replica)))
    (is (empty? (:prepared replica)))
    ;; peers may have left before they joined, so there should be at LEAST 7 peers allocated
    ;; since there are enough peers to handle 2 peers leaving without a task being deallocated the
    ;; job must be able to go on
    (is (>= (apply + (map count (vals (get (:allocations replica) job-1-id)))) 7))))

(deftest peer-leave-still-running
  (checking
    "Checking peer leave is correctly performed"
    (times 50)
    [{:keys [replica log peer-choices]}
     (log-gen/apply-entries-gen
       (gen/return
         {:replica {:job-scheduler :onyx.job-scheduler/balanced
                    :messaging {:onyx.messaging/impl :aeron}}
          :message-id 0
          :entries
          (-> (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 1 9))
              (assoc :job-1 {:queue [(api/create-submit-job-entry
                                       job-1-id
                                       peer-config
                                       job-1
                                       (planning/discover-tasks (:catalog job-1) (:workflow job-1)))]})
              (assoc :leave-1 {:queue [{:fn :leave-cluster :args {:id :g1-p1 :group-id :g1}}]})
              (assoc :leave-2 {:queue [{:fn :leave-cluster :args {:id :g1-p2 :group-id :g1}}]}))
          :log []
          :peer-choices []}))]
    (is (empty? (:accepted replica)))
    (is (empty? (:prepared replica)))
    ;; peers may have left before they joined, so there should be at LEAST 7 peers allocated
    ;; since there are enough peers to handle 2 peers leaving without a task being deallocated the
    ;; job must be able to go on
    (is (>= (apply + (map count (vals (get (:allocations replica) job-1-id)))) 7))))

;; Reproduce onyx-test scheduler issue
(def inner-job-id #uuid "f55c14f0-a847-42eb-81bb-0c0390a88608")

(def inner-job
  {:workflow [[:a :b] [:b :c] [:c :d] [:d :e]]
   :catalog [{:onyx/name :a
              :onyx/plugin :onyx.plugin.core-async/input
              :onyx/type :input
              :onyx/min-peers 4
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Reads segments from a core.async channel"}

             {:onyx/name :b
              :onyx/fn :mock/fn
              :onyx/type :function
              :onyx/batch-size 20}

             {:onyx/name :c
              :onyx/fn :mock/fn
              :onyx/type :function
              :onyx/batch-size 20}

             {:onyx/name :d
              :onyx/fn :mock/fn
              :onyx/type :function
              :onyx/batch-size 20}

             {:onyx/name :e
              :onyx/plugin :onyx.plugin.core-async/output
              :onyx/max-peers 1
              :onyx/type :output
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Writes segments to a core.async channel"}]
   :task-scheduler :onyx.task-scheduler/balanced})

(def outer-job-id #uuid "5813d2ec-c486-4428-833d-e8373910ae14")

(def outer-job
  {:workflow [[:d :e] [:e :f]]
   :catalog [{:onyx/name :d
              :onyx/plugin :onyx.plugin.core-async/input
              :onyx/type :input
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/max-peers 1
              :onyx/doc "Reads segments from a core.async channel"}

             {:onyx/name :e
              :onyx/fn :mock/fn
              :onyx/type :function
              :onyx/batch-size 20}

             {:onyx/name :f
              :onyx/plugin :onyx.plugin.core-async/output
              :onyx/type :output
              :onyx/max-peers 1
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Writes segments to a core.async channel"}]
   :task-scheduler :onyx.task-scheduler/balanced})

(defn running? [peer-counts]
  (and (not (empty? peer-counts)) 
       (not (some zero? peer-counts))))

#_(deftest outer-inner-allocations
  (checking
    "Reproduce scheduler issue in https://github.com/littlebird/onyx-test. 
    Second job fails to run due to cjs/job-offer-n-peers :onyx.job-scheduler/balanced"
    (times 50)
    [{:keys [replica log peer-choices]}
     (log-gen/apply-entries-gen
       (gen/return
         {:replica {:job-scheduler :onyx.job-scheduler/balanced
                    :messaging {:onyx.messaging/impl :aeron}}
          :message-id 0
          :entries (assoc (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 1 11))
                          :job-1 {:queue [(api/create-submit-job-entry
                                            inner-job-id
                                            peer-config
                                            inner-job
                                            (planning/discover-tasks (:catalog inner-job) (:workflow inner-job)))]}
                          :job-2 {:queue [(api/create-submit-job-entry
                                            outer-job-id
                                            peer-config
                                            outer-job
                                            (planning/discover-tasks (:catalog outer-job) (:workflow outer-job)))]})
          :log []
          :peer-choices []}))]
    (is (running? (map count (vals (get (:allocations replica) inner-job-id)))))
    (is (running? (map count (vals (get (:allocations replica) outer-job-id)))))))


(def slot-id-job-id #uuid "f55c14f0-a847-42eb-81bb-0c0390a88608")

(def slot-id-job
  {:workflow [[:a :b] [:b :c] [:c :d] [:d :e] [:e :f] [:f :g]]
   :catalog [{:onyx/name :a
              :onyx/plugin :onyx.plugin.core-async/input
              :onyx/type :input
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Reads segments from a core.async channel"}

             {:onyx/name :b
              :onyx/fn :mock/fn
              :onyx/n-peers 1
              :onyx/type :function
              :onyx/batch-size 20}

             {:onyx/name :c
              :onyx/fn :mock/fn
              :onyx/n-peers 1
              :onyx/type :function
              :onyx/batch-size 20}

             {:onyx/name :d
              :onyx/fn :mock/fn
              :onyx/n-peers 1
              :onyx/type :function
              :onyx/batch-size 20}

             {:onyx/name :e
              :onyx/fn :mock/fn
              :onyx/n-peers 1
              :onyx/type :function
              :onyx/batch-size 20}

             {:onyx/name :f
              :onyx/fn :mock/fn
              :onyx/n-peers 1
              :onyx/type :function
              :onyx/batch-size 20}

             {:onyx/name :g
              :onyx/plugin :onyx.plugin.core-async/output
              :onyx/type :output
              :onyx/n-peers 1
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Writes segments to a core.async channel"}]
   :task-scheduler :onyx.task-scheduler/balanced})

(deftest slot-id-after-peer-leave
  (checking
    "Checking peer leave is correctly performed"
    (times 50)
    [{:keys [replica log peer-choices]}
     (log-gen/apply-entries-gen
       (gen/return
         {:replica {:job-scheduler :onyx.job-scheduler/balanced
                    :messaging {:onyx.messaging/impl :aeron}}
          :message-id 0
          :entries
          (-> (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 1 14))
              (assoc :job-1 {:queue [(api/create-submit-job-entry
                                       slot-id-job-id
                                       peer-config
                                       slot-id-job
                                       (planning/discover-tasks (:catalog slot-id-job) (:workflow slot-id-job)))]})
              (assoc :leave-1 {:predicate (fn [replica entry]
                                            (some #{:g1-p1} (:peers replica)))
                               :queue [{:fn :leave-cluster :args {:id :g1-p1 :group-id :g1}}]})
              (assoc :leave-2 {:predicate (fn [replica entry]
                                            (some #{:g1-p2} (:peers replica)))
                               :queue [{:fn :leave-cluster :args {:id :g1-p2 :group-id :g1}}]})
              (assoc :leave-3 {:predicate (fn [replica entry]
                                            (some #{:g1-p3} (:peers replica)))
                               :queue [{:fn :leave-cluster :args {:id :g1-p3 :group-id :g1}}]})
              (assoc :leave-4 {:predicate (fn [replica entry]
                                            (some #{:g1-p4} (:peers replica)))
                               :queue [{:fn :leave-cluster :args {:id :g1-p4 :group-id :g1}}]})
              (assoc :leave-5 {:predicate (fn [replica entry]
                                            (some #{:g1-p5} (:peers replica)))
                               :queue [{:fn :leave-cluster :args {:id :g1-p5 :group-id :g1}}]}))
          :log []
          :peer-choices []}))]

    ;; check that task :a has more than one peer on it so we don't get confused
    (is (not 
          (empty? 
            (filter #(> (count %) 1) 
                    (vals (val (first (:task-slot-ids replica))))))))

    (is (= #{'(0)}
           (set (map vals 
                     (filter #(= (count %) 1) 
                             (vals (val (first (:task-slot-ids replica)))))))))
    
    (is (empty? (:accepted replica)))
    (is (empty? (:prepared replica)))
    ;; peers may have left before they joined, so there should be at LEAST 7 peers allocated
    ;; since there are enough peers to handle 2 peers leaving without a task being deallocated the
    ;; job must be able to go on
    (is (>= (apply + (map count (vals (get (:allocations replica) slot-id-job-id)))) 7))))
