(ns onyx.log.generative-peer-join
  (:require [onyx.messaging.dummy-messenger :refer [dummy-messenger]]
            [onyx.log.generators :as log-gen]
            [onyx.extensions :as extensions]
            [onyx.api :as api]
            [onyx.static.planning :as planning]
            [onyx.test-helper :refer [job-allocation-counts]]
            [clojure.set :refer [intersection]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test :refer :all]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]))

(def onyx-id (java.util.UUID/randomUUID))

(def peer-config
  {:onyx/id onyx-id
   :onyx.messaging/impl :dummy-messenger})

(def messenger (dummy-messenger {}))

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
                    :messaging {:onyx.messaging/impl :dummy-messenger}}
          :message-id 0
          :entries (assoc (log-gen/generate-join-queues (log-gen/generate-peer-ids 8))
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

    (is (= #{:active} (set (vals (:peer-state replica)))))
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
                    :messaging {:onyx.messaging/impl :dummy-messenger}}
          :message-id 0
          :entries (assoc (log-gen/generate-join-queues (log-gen/generate-peer-ids 8))
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
    (is (= #{:active} (set (vals (:peer-state replica)))))
    (is (= (apply + (map count (vals (get (:allocations replica) job-1-id)))) 8))
    (is (= (apply + (map count (vals (get (:allocations replica) job-2-id)))) 0))))

(deftest balanced-task-balancing
  (checking
    "Checking Balanced allocation causes peers to be evenly over tasks"
    50
    [{:keys [replica log peer-choices]}
     (log-gen/apply-entries-gen
       (gen/return
         {:replica {:job-scheduler :onyx.job-scheduler/balanced
                    :messaging {:onyx.messaging/impl :dummy-messenger}}
          :message-id 0
          :entries (assoc (log-gen/generate-join-queues (log-gen/generate-peer-ids 6))
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
    (is (= #{:active} (set (vals (:peer-state replica)))))
    (is (= (map count (vals (get (:allocations replica) job-1-id))) [1 1 1]))
    (is (= (map count (vals (get (:allocations replica) job-2-id))) [1 1 1]))))

(deftest balanced-allocations-uneven
  (checking
    "Checking Balanced allocation causes peers to be evenly over tasks when the spread is uneven"
    (times 50)
    [{:keys [replica log peer-choices]}
     (log-gen/apply-entries-gen
       (gen/return
         {:replica {:job-scheduler :onyx.job-scheduler/balanced
                    :messaging {:onyx.messaging/impl :dummy-messenger}}
          :message-id 0
          :entries (assoc (log-gen/generate-join-queues (log-gen/generate-peer-ids 7))
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
    (is (= #{:active} (set (vals (:peer-state replica)))))
    (let [j1-allocations (map (fn [t] (get-in replica [:allocations job-1-id t])) (get-in replica [:tasks job-1-id]))
          j2-allocations (map (fn [t] (get-in replica [:allocations job-2-id t])) (get-in replica [:tasks job-2-id]))]
      ;; Since job IDs are reused, we can't know which order they'll be in.
      (is (= (set (map sort [(map count j1-allocations) (map count j2-allocations)]))
             #{[1 1 2] [1 1 1]})))))

(deftest balanced-allocations
  (checking
    "Checking balanced allocation causes peers to be evenly split"
    (times 50)
    [{:keys [replica log peer-choices]}
     (log-gen/apply-entries-gen
       (gen/return
         {:replica {:job-scheduler :onyx.job-scheduler/balanced
                    :messaging {:onyx.messaging/impl :dummy-messenger}}
          :message-id 0
          :entries (assoc (log-gen/generate-join-queues (log-gen/generate-peer-ids 12))
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
    (is (= #{:active} (set (vals (:peer-state replica)))))
    (is (= (map count (vals (get (:allocations replica) job-1-id))) [2 2 2]))
    (is (= (map count (vals (get (:allocations replica) job-2-id))) [2 2 2]))
    (is (= (map count (vals (get (:allocations replica) job-3-id))) []))))

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
                      :messaging {:onyx.messaging/impl :dummy-messenger}}
            :message-id 0
            :entries (assoc (log-gen/generate-join-queues (log-gen/generate-peer-ids 20))
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
    (let [peer-state-group (group-by val (:peer-state replica))]
      (is (= (count (:active peer-state-group)) 12))
      (is (= (count (:idle peer-state-group)) 8))
      (is (= (count (:backpressure peer-state-group)) 0)))
    (is (= (map count (vals (get (:allocations replica) job-1-id))) [2 2 2]))
    (is (= (map count (vals (get (:allocations replica) job-2-id))) [2 2 2]))
    (is (= (map count (vals (get (:allocations replica) job-3-id))) []))))

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
                    :messaging {:onyx.messaging/impl :dummy-messenger}}
          :message-id 0
          :entries (assoc (log-gen/generate-join-queues (log-gen/generate-peer-ids 16))
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
    (is (= #{:active} (set (vals (:peer-state replica)))))
    (is
      (=
       (map
         (fn [t]
           (count (get-in replica [:allocations job-1-id t])))
         (get-in replica [:tasks job-1-id]))
       [1 4 3]))
   (is
    (=
     (map
      (fn [t]
        (count (get-in replica [:allocations job-2-id t])))
      (get-in replica [:tasks job-2-id]))
     [2 2 4]))
   (is (= (map count (vals (get (:allocations replica) job-3-id))) []))))

(deftest peer-leave-4
  (checking
    "Checking peer leave is correctly performed"
    (times 50)
    [{:keys [replica log peer-choices]}
     (log-gen/apply-entries-gen
       (gen/return
         {:replica {:job-scheduler :onyx.job-scheduler/balanced
                    :messaging {:onyx.messaging/impl :dummy-messenger}}
          :message-id 0
          :entries
          (-> (log-gen/generate-join-queues (log-gen/generate-peer-ids 4))
              (assoc :leave {:predicate (fn [replica entry]
                                          (some #{:p1} (:peers replica)))
                             :queue [{:fn :leave-cluster
                                      :args {:id :p1}}]}))
          :log []
          :peer-choices []}))]
    (is (= 3 (count (:peer-state replica))))
    (is (= 3 (count (:peers replica))))))

(deftest peer-leave
  (checking
    "Checking peer leave is correctly performed"
    (times 50)
    [{:keys [replica log peer-choices]}
     (log-gen/apply-entries-gen
       (gen/return
         {:replica {:job-scheduler :onyx.job-scheduler/balanced
                    :messaging {:onyx.messaging/impl :dummy-messenger}}
          :message-id 0
          :entries
          (-> (log-gen/generate-join-queues (log-gen/generate-peer-ids 3))
              (assoc :leave-anytime {:queue [{:fn :leave-cluster
                                              :args {:id :p1}}]}))
          :log []
          :peer-choices []}))]
    (is (or (= 2 (count (:peer-state replica)))
            (= 3 (count (:peer-state replica)))))
    (is (or (= 2 (count (:peers replica)))
            (= 3 (count (:peers replica)))))))


(deftest peer-leave-still-running
  (checking
    "Checking peer leave is correctly performed"
    (times 50)
    [{:keys [replica log peer-choices]}
     (log-gen/apply-entries-gen
       (gen/return
         {:replica {:job-scheduler :onyx.job-scheduler/balanced
                    :messaging {:onyx.messaging/impl :dummy-messenger}}
          :message-id 0
          :entries
          (-> (log-gen/generate-join-queues (log-gen/generate-peer-ids 9))
              (assoc :job-1 {:queue [(api/create-submit-job-entry
                                       job-1-id
                                       peer-config
                                       job-1
                                       (planning/discover-tasks (:catalog job-1) (:workflow job-1)))]})
              (assoc :leave-1 {:queue [{:fn :leave-cluster :args {:id :p1}}]})
              (assoc :leave-2 {:queue [{:fn :leave-cluster :args {:id :p2}}]}))
          :log []
          :peer-choices []}))]
    ;; peers may have left before they joined, so there should be at LEAST 7 peers allocated
    ;; since there are enough peers to handle 2 peers leaving without a task being deallocated the
    ;; job must be able to go on
    (is (>= (apply + (map count (vals (get (:allocations replica) job-1-id)))) 7))))
