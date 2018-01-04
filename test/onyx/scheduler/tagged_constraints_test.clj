(ns onyx.scheduler.tagged-constraints-test
  (:require [clojure.test :refer :all]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.data :refer [diff]]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [onyx.scheduling.common-job-scheduler :refer [reconfigure-cluster-workload]]
            [onyx.log.generators :refer [one-group] :as log-gen]
            [onyx.test-helper :refer [job-allocation-counts get-counts]]
            [onyx.static.planning :as planning]
            [onyx.api]))

(def old-replica {:allocations {}})

(deftest diff-replicas-after-reconfigured
  (let [new-replica (one-group
                     {:jobs [:j1]
                      :allocations {}
                      :peers [:p1 :p2 :p3]
                      :peer-state {:p1 :idle :p2 :idle :p3 :idle}
                      :tasks {:j1 [:t1 :t2 :t3]}
                      :task-schedulers {:j1 :onyx.task-scheduler/balanced
                                        :j2 :onyx.task-scheduler/balanced}
                      :job-scheduler :onyx.job-scheduler/balanced
                      :required-tags {:j1 {:t1 [:datomic]
                                           :t2 [:datomic]
                                           :t3 [:datomic]}}
                      :peer-tags {:p1 [:datomic]
                                  :p2 [:datomic]
                                  :p3 [:datomic]}
                      :messaging {:onyx.messaging/impl :aeron}})
        reconfigured (reconfigure-cluster-workload new-replica old-replica)]
    (is
      (=  '({:allocations nil}
            {:allocations {:j1 {:t2 [:p1]
                                :t3 [:p2]
                                :t1 [:p3]}}
             :peer-sites {:p1 {} :p2 {} :p3 {}}
             :task-slot-ids {:j1 {:t2 {:p1 0}
                                  :t3 {:p2 0}
                                  :t1 {:p3 0}}}
             :allocation-version {:j1 nil}
             :coordinators {:j1 :p1}
             :message-short-ids {}})
         (butlast (diff new-replica reconfigured))))))

(deftest peers-allocated-with-tags-if-peers-more-than-necessary
  (is
   (= {:j1 {:t1 [:p1]
            :t2 [:p6]
            :t3 [:p2]}}
      (:allocations
       (reconfigure-cluster-workload
        (one-group
         {:jobs [:j1]
          :allocations {}
          :peers [:p1 :p2 :p3 :p4 :p5 :p6]
          :peer-state {:p1 :idle :p2 :idle :p3 :idle :p4 :idle :p5 :idle :p6 :idle}
          :tasks {:j1 [:t1 :t2 :t3]}
          :task-schedulers {:j1 :onyx.task-scheduler/balanced
                            :j2 :onyx.task-scheduler/balanced}
          :job-scheduler :onyx.job-scheduler/balanced
          :min-required-peers {:j1 {:t1 1 :t2 1 :t3 1}},
          :saturation {:j1 3}
          :task-saturation {:j1 {:t1 1 :t2 1 :t3 1}}
          :required-tags {:j1 {:t1 [:s3]
                               :t2 [:datomic]
                               :t3 [:datomic]}}
          :peer-tags {:p1 [:s3] :p2 [:datomic] :p3 [:datomic] :p4 [:s3] :p5 [:s3] :p6 [:datomic]}
          :messaging {:onyx.messaging/impl :aeron}})
        old-replica)))))

;; Following tests are broken because reconfigure-cluster-workload
;; now takes an old and a new replica

(deftest no-peers-are-allocated-missing-tags
  (is
   (= {}
      (:allocations
       (reconfigure-cluster-workload
        (one-group
         {:jobs [:j1]
          :allocations {}
          :peers [:p1 :p2 :p3]
          :peer-state {:p1 :idle :p2 :idle :p3 :idle}
          :tasks {:j1 [:t1 :t2 :t3]}
          :task-schedulers {:j1 :onyx.task-scheduler/balanced
                            :j2 :onyx.task-scheduler/balanced}
          :job-scheduler :onyx.job-scheduler/balanced
          :required-tags {:j1 {:t1 [:datomic]
                               :t2 [:datomic]
                               :t3 [:datomic]}}
          :messaging {:onyx.messaging/impl :aeron}})
        old-replica)))))


(deftest peers-allocated-with-tags
  (is
   (= {:j1 {:t1 [:p3]
            :t2 [:p1]
            :t3 [:p2]}}
      (:allocations
       (reconfigure-cluster-workload
        (one-group
         {:jobs [:j1]
          :allocations {}
          :peers [:p1 :p2 :p3]
          :tasks {:j1 [:t1 :t2 :t3]}
          :task-schedulers {:j1 :onyx.task-scheduler/balanced
                            :j2 :onyx.task-scheduler/balanced}
          :job-scheduler :onyx.job-scheduler/balanced
          :required-tags {:j1 {:t1 [:datomic]
                               :t2 [:datomic]
                               :t3 [:datomic]}}
          :peer-tags {:p1 [:datomic] :p2 [:datomic] :p3 [:datomic]}
          :messaging {:onyx.messaging/impl :aeron}})
        old-replica)))))

(deftest only-tagged-peers-allocated
  (is
   (= {:j1 {:t1 [:p4]
            :t2 [:p1]
            :t3 [:p2]}}
      (:allocations
       (reconfigure-cluster-workload
        (one-group
         {:jobs [:j1]
          :allocations {}
          :peers [:p1 :p2 :p3 :p4]
          :tasks {:j1 [:t1 :t2 :t3]}
          :task-schedulers {:j1 :onyx.task-scheduler/balanced
                            :j2 :onyx.task-scheduler/balanced}
          :job-scheduler :onyx.job-scheduler/balanced
          :required-tags {:j1 {:t1 [:datomic]
                               :t2 [:datomic]
                               :t3 [:datomic]}}
          :peer-tags {:p1 [:datomic]
                      :p2 [:datomic]
                      :p3 []
                      :p4 [:datomic]}
          :messaging {:onyx.messaging/impl :aeron}})
        old-replica)))))

(deftest one-task-tagged
  (is
   (= {:j1 {:t1 [:p1]
            :t2 [:p3]
            :t3 [:p2]}}
      (:allocations
       (reconfigure-cluster-workload
        (one-group
         {:jobs [:j1]
          :allocations {}
          :peers [:p1 :p2 :p3]
          :tasks {:j1 [:t1 :t2 :t3]}
          :task-schedulers {:j1 :onyx.task-scheduler/balanced
                            :j2 :onyx.task-scheduler/balanced}
          :job-scheduler :onyx.job-scheduler/balanced
          :required-tags {:j1 {:t1 [:datomic]}}
          :peer-tags {:p1 [:datomic]}
          :messaging {:onyx.messaging/impl :aeron}})
        old-replica)))))

(deftest one-task-tagged-max-peers
  (is
   (= {:j1 {:t1 [:p1]
            :t2 [:p3 :p4]
            :t3 [:p2]}}
      (:allocations
       (reconfigure-cluster-workload
        (one-group
         {:jobs [:j1]
          :allocations {}
          :peers [:p1 :p2 :p3 :p4]
          :tasks {:j1 [:t1 :t2 :t3]}
          :task-schedulers {:j1 :onyx.task-scheduler/balanced
                            :j2 :onyx.task-scheduler/balanced}
          :job-scheduler :onyx.job-scheduler/balanced
          :required-tags {:j1 {:t1 [:datomic]}}
          :peer-tags {:p1 [:datomic]}
          :task-saturation {:j1 {:t1 1}}
          :messaging {:onyx.messaging/impl :aeron}})
        old-replica)))))

(deftest two-tags
  (is
   (= {:j1 {:t1 [:p1]
            :t2 [:p2]
            :t3 [:p3]}}
      (:allocations
       (reconfigure-cluster-workload
        (one-group
         {:jobs [:j1]
          :allocations {}
          :peers [:p1 :p2 :p3]
          :tasks {:j1 [:t1 :t2 :t3]}
          :task-schedulers {:j1 :onyx.task-scheduler/balanced
                            :j2 :onyx.task-scheduler/balanced}
          :job-scheduler :onyx.job-scheduler/balanced
          :required-tags {:j1 {:t1 []
                               :t2 [:mysql :datomic]
                               :t3 [:datomic]}}
          :peer-tags {:p1 []
                      :p2 [:datomic :mysql]
                      :p3 [:datomic]}
          :messaging {:onyx.messaging/impl :aeron}})
        old-replica)))))

(deftest two-jobs
  (is
   (= {:j1 {:t1 [:p7]
            :t2 [:p3]
            :t3 [:p5]}
       :j2 {:t4 [:p8]
            :t5 [:p4]
            :t6 [:p1]}}
      (:allocations
       (reconfigure-cluster-workload
        (one-group
         {:jobs [:j1 :j2]
          :allocations {:j1 {:t1 [:p7]
                             :t2 [:p3 :p4 :p5]
                             :t3 [:p8]}}
          :peers [:p1 :p3 :p4 :p5 :p7 :p8]
          :tasks {:j1 [:t1 :t2 :t3]
                  :j2 [:t4 :t5 :t6]}
          :task-schedulers {:j1 :onyx.task-scheduler/balanced
                            :j2 :onyx.task-scheduler/balanced}
          :job-scheduler :onyx.job-scheduler/balanced
          :task-saturation {:j1 {:t1 1 :t2 42 :t3 1}
                            :t2 {:t4 1 :t5 42 :t6 1}}
          :required-tags {:j1 {:t1 [:datomic]
                               :t2 []
                               :t3 []}
                          :j2 {:t4 [:datomic]
                               :t5 []
                               :t6 []}}
          :peer-tags {:p7 [:datomic]
                      :p8 [:datomic]}
          :messaging {:onyx.messaging/impl :aeron}})
        old-replica)))))

; (def onyx-id "tagged-gen-test-id")

; (def peer-config
;   {:onyx/id onyx-id
;    :onyx.messaging/impl :aeron})

; (defn name->task-id [catalog job-entry name]
;   (get (zipmap (map :onyx/name catalog)
;                (:tasks (:args job-entry)))
;        name))

; (deftest peer-leave-tagged
;   (let [job-1-id "job-1"
;         job-1 {:workflow [[:a :b] [:b :c]]
;                :catalog [{:onyx/name :a
;                           :onyx/plugin :onyx.plugin.core-async/input
;                           :onyx/type :input
;                           :onyx/medium :core.async
;                           :onyx/batch-size 20
;                           :onyx/max-peers 1
;                           :onyx/required-tags [:special-peer]
;                           :onyx/doc "Reads segments from a core.async channel"}

;                          {:onyx/name :b
;                           :onyx/fn :mock/fn
;                           :onyx/type :function
;                           :onyx/batch-size 20}

;                          {:onyx/name :c
;                           :onyx/plugin :onyx.plugin.core-async/output
;                           :onyx/type :output
;                           :onyx/max-peers 1
;                           :onyx/medium :core.async
;                           :onyx/batch-size 20
;                           :onyx/doc "Writes segments to a core.async channel"}]
;                :task-scheduler :onyx.task-scheduler/balanced}
;         job-2-id "job-2"
;         job-2 {:workflow [[:d :e] [:e :f]]
;                :catalog [{:onyx/name :d
;                           :onyx/plugin :onyx.plugin.core-async/input
;                           :onyx/type :input
;                           :onyx/medium :core.async
;                           :onyx/batch-size 20
;                           :onyx/max-peers 1
;                           :onyx/doc "Reads segments from a core.async channel"}

;                          {:onyx/name :e
;                           :onyx/fn :mock/fn
;                           :onyx/type :function
;                           :onyx/batch-size 20}

;                          {:onyx/name :f
;                           :onyx/plugin :onyx.plugin.core-async/output
;                           :onyx/type :output
;                           :onyx/max-peers 1
;                           :onyx/medium :core.async
;                           :onyx/batch-size 20
;                           :onyx/required-tags [:special-peer]
;                           :onyx/doc "Writes segments to a core.async channel"}]
;                :task-scheduler :onyx.task-scheduler/balanced}
;         job-entry-1 (onyx.api/create-submit-job-entry
;                       job-1-id
;                       peer-config
;                       job-1
;                       (planning/discover-tasks (:catalog job-1) (:workflow job-1)))
;         job-entry-2 (onyx.api/create-submit-job-entry
;                       job-2-id
;                       peer-config
;                       job-2
;                       (planning/discover-tasks (:catalog job-2) (:workflow job-2)))] (checking
;     "Peers leaving keep the job running"
;     (times 50)
;     [{:keys [replica log entries peer-choices]}
;      (log-gen/apply-entries-gen
;        (gen/return
;          {:replica {:job-scheduler :onyx.job-scheduler/balanced
;                     :messaging {:onyx.messaging/impl :aeron}}
;           :message-id 0
;           :entries
;           (assoc (merge (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 1 8))
;                         (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 2 1 9 3) {:tags [:special-peer]}))
;                  :job-1 {:queue [job-entry-1]}
;                  :job-2 {:queue [job-entry-2]}
;                  :leave-1 {:predicate (fn [replica entry]
;                                         (some #{:g1-p1} (:peers replica)))
;                            :queue [{:fn :leave-cluster :args {:id :g1-p1 :group-id :g1}}]}
;                  :leave-with-tag {:predicate (fn [replica entry]
;                                                (some #{:g2-p11} (:peers replica)))
;                                   :queue [{:fn :leave-cluster :args {:id :g2-p11 :group-id :g2}}]}
;                  :leave-2 {:predicate (fn [replica entry]
;                                         (some #{:g1-p2} (:peers replica)))
;                            :queue [{:fn :leave-cluster :args {:id :g1-p2 :group-id :g1}}]})
;           :log []
;           :peer-choices []}))]
;     (let [task-a-id (name->task-id (:catalog job-1) job-entry-1 :a)
;           task-f-id (name->task-id (:catalog job-2) job-entry-2 :f)
;           task-a-peers (get-in replica [:allocations job-1-id task-a-id])
;           task-f-peers (get-in replica [:allocations job-2-id task-f-id])]
;       (is (= #{[:g2-p9 [:special-peer]] [:g2-p10 [:special-peer]]} (set (remove (comp nil? val) (:peer-tags replica)))))
;       (is (= 8 (count (:peers replica))))
;       (is (= [4 4]
;              (map (comp (partial apply +) vals)
;                   (get-counts replica
;                               [{:job-id job-1-id}
;                                {:job-id job-2-id}]))))
;       (is (some (into #{} task-a-peers) #{:g2-p9 :g2-p10}))
;       (is (some (into #{} task-f-peers) #{:g2-p9 :g2-p10}))))))

; (deftest peer-leave-tagged-deallocate
;   (let [job-1-id "job-1"
;         job-1 {:workflow [[:a :b] [:b :c]]
;                :catalog [{:onyx/name :a
;                           :onyx/plugin :onyx.plugin.core-async/input
;                           :onyx/type :input
;                           :onyx/medium :core.async
;                           :onyx/batch-size 20
;                           :onyx/max-peers 1
;                           :onyx/required-tags [:special-peer]
;                           :onyx/doc "Reads segments from a core.async channel"}

;                          {:onyx/name :b
;                           :onyx/fn :mock/fn
;                           :onyx/type :function
;                           :onyx/batch-size 20}

;                          {:onyx/name :c
;                           :onyx/plugin :onyx.plugin.core-async/output
;                           :onyx/type :output
;                           :onyx/max-peers 1
;                           :onyx/medium :core.async
;                           :onyx/batch-size 20
;                           :onyx/doc "Writes segments to a core.async channel"}]
;                :task-scheduler :onyx.task-scheduler/balanced}
;         job-entry-1 (onyx.api/create-submit-job-entry
;                       job-1-id
;                       peer-config
;                       job-1
;                       (planning/discover-tasks (:catalog job-1) (:workflow job-1)))]
;     (checking
;       "Tagged peer leaves, deallocates the job requiring that peer"
;       (times 50)
;       [{:keys [replica log entries peer-choices]}
;        (log-gen/apply-entries-gen
;          (gen/return
;            {:replica {:job-scheduler :onyx.job-scheduler/balanced
;                       :messaging {:onyx.messaging/impl :aeron}}
;             :message-id 0
;             :entries
;             (assoc (merge (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 1 3))
;                           (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 2 1 4 1) {:tags [:special-peer]}))
;                    :job-1 {:queue [job-entry-1]}
;                    :leave-1 {:predicate (fn [replica entry]
;                                           (some #{:g2-p4} (:peers replica)))
;                              :queue [{:fn :leave-cluster :args {:id :g2-p4 :group-id :g2}}]})
;             :log []
;             :peer-choices []}))]
;       (is (= #{} (set (remove (comp nil? val) (:peer-tags replica)))))
;         (is (= 3 (count (:peers replica))))
;         (is (= [0]
;                (map (comp (partial apply +) vals)
;                     (get-counts replica
;                                 [{:job-id job-1-id}])))))))

; (deftest all-tagged-still-balances
;   (let [job-1-id "job-1"
;         job-1 {:workflow [[:a :b] [:b :c]]
;                :catalog [{:onyx/name :a
;                           :onyx/plugin :onyx.plugin.core-async/input
;                           :onyx/type :input
;                           :onyx/medium :core.async
;                           :onyx/batch-size 20
;                           :onyx/required-tags [:special-peer]
;                           :onyx/doc "Reads segments from a core.async channel"}

;                          {:onyx/name :b
;                           :onyx/fn :mock/fn
;                           :onyx/type :function
;                           :onyx/batch-size 20}

;                          {:onyx/name :c
;                           :onyx/plugin :onyx.plugin.core-async/output
;                           :onyx/type :output
;                           :onyx/medium :core.async
;                           :onyx/batch-size 20
;                           :onyx/doc "Writes segments to a core.async channel"}]
;                :task-scheduler :onyx.task-scheduler/balanced}
;         job-2-id "job-2"
;         job-2 {:workflow [[:d :e] [:e :f]]
;                :catalog [{:onyx/name :d
;                           :onyx/plugin :onyx.plugin.core-async/input
;                           :onyx/type :input
;                           :onyx/medium :core.async
;                           :onyx/batch-size 20
;                           :onyx/doc "Reads segments from a core.async channel"}

;                          {:onyx/name :e
;                           :onyx/fn :mock/fn
;                           :onyx/type :function
;                           :onyx/batch-size 20}

;                          {:onyx/name :f
;                           :onyx/plugin :onyx.plugin.core-async/output
;                           :onyx/type :output
;                           :onyx/medium :core.async
;                           :onyx/batch-size 20
;                           :onyx/required-tags [:special-peer]
;                           :onyx/doc "Writes segments to a core.async channel"}]
;                :task-scheduler :onyx.task-scheduler/balanced}
;         job-3-id "job-3"
;         job-3 job-2
;         job-entry-1 (onyx.api/create-submit-job-entry
;                       job-1-id
;                       peer-config
;                       job-1
;                       (planning/discover-tasks (:catalog job-1) (:workflow job-1)))
;         job-entry-2 (onyx.api/create-submit-job-entry
;                       job-2-id
;                       peer-config
;                       job-2
;                       (planning/discover-tasks (:catalog job-2) (:workflow job-2)))
;         job-entry-3 (onyx.api/create-submit-job-entry
;                       job-3-id
;                       peer-config
;                       job-3
;                       (planning/discover-tasks (:catalog job-3) (:workflow job-3)))]
;     (checking
;       "More peers than necessary are tagged, job is killed, still has balanced allocation"
;       (times 50)
;       [{:keys [replica log entries peer-choices]}
;        (log-gen/apply-entries-gen
;          (gen/return
;            {:replica {:job-scheduler :onyx.job-scheduler/balanced
;                       :messaging {:onyx.messaging/impl :aeron}}
;             :message-id 0
;             :entries
;             (assoc (merge (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 1 10) {:tags [:special-peer]})
;                           (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 2 1 11 10)))
;                    :job-1 {:queue [job-entry-1]}
;                    :job-2 {:queue [job-entry-2]}
;                    :job-3 {:queue [job-entry-3
;                                    {:fn :kill-job :args {:job job-3-id}}]}
;                    :leave-untagged {:predicate (fn [replica entry]
;                                                  (some #{:g2-p14} (:peers replica)))
;                                     :queue [{:fn :leave-cluster :args {:id :g2-p14 :group-id :g2}}]}
;                    :leave-tagged {:predicate (fn [replica entry]
;                                                (some #{:g1-p1} (:peers replica)))
;                                   :queue [{:fn :leave-cluster :args {:id :g1-p1 :group-id :g1}}]})
;             :log []
;             :peer-choices []}))]
;       (is (= 18 (count (:peers replica))))
;       (is (= [[3 3 3] [3 3 3]]
;              (map vals
;                   (get-counts replica
;                               [{:job-id job-1-id}
;                                {:job-id job-2-id}])))))))
