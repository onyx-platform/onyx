(ns onyx.log.leave-cluster-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.messaging.dummy-messenger :refer [dummy-messenger]]
            [onyx.system]
            [onyx.log.replica :as replica]
            [clojure.test :refer [deftest is testing]]))

(let [entry (create-log-entry :leave-cluster {:id :c})
      f (partial extensions/apply-log-entry entry)
      rep-diff (partial extensions/replica-diff entry)
      rep-reactions (partial extensions/reactions entry)
      old-replica (merge replica/base-replica 
                         {:pairs {:a :b :b :c :c :a} :peers [:a :b :c]
                          :messaging {:onyx.messaging/impl :dummy-messenger}
                          :job-scheduler :onyx.job-scheduler/greedy})
      new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)]
  (is (get-in new-replica [:pairs :a]) => :b)
  (is (get-in new-replica [:pairs :b]) => :a)
  (is (get-in new-replica [:pairs :c]) => nil)
  (is diff => {:died :c :updated-watch {:observer :b :subject :a}})
  (is (rep-reactions old-replica new-replica diff {:id :a}) => nil))

(let [entry (create-log-entry :leave-cluster {:id :b})
      f (partial extensions/apply-log-entry entry)
      rep-diff (partial extensions/replica-diff entry)
      rep-reactions (partial extensions/reactions entry)
      old-replica (merge replica/base-replica 
                         {:pairs {:a :b :b :a} :peers [:a :b]
                          :messaging {:onyx.messaging/impl :dummy-messenger}
                          :job-scheduler :onyx.job-scheduler/greedy})
      new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)]
  (is (get-in new-replica [:pairs :a]) => nil)
  (is (get-in new-replica [:pairs :b]) => nil)
  (is diff => {:died :b :updated-watch {:observer :a :subject :a}})
  (is (rep-reactions old-replica new-replica diff {:id :a}) => nil))

(let [entry (create-log-entry :leave-cluster {:id :d})
      f (partial extensions/apply-log-entry entry)
      rep-diff (partial extensions/replica-diff entry)
      rep-reactions (partial extensions/reactions entry)
      old-replica (merge replica/base-replica 
                         {:job-scheduler :onyx.job-scheduler/balanced
                          :pairs {:a :b :b :c :c :d :d :a}
                          :peers [:a :b :c :d]
                          :jobs [:j1 :j2]
                          :task-schedulers {:j1 :onyx.task-scheduler/balanced
                                            :j2 :onyx.task-scheduler/balanced}
                          :tasks {:j1 [:t1] :j2 [:t2]}
                          :allocations {:j1 {:t1 [:a :b]}
                                        :j2 {:t2 [:c :d]}}
                          :messaging {:onyx.messaging/impl :dummy-messenger}})
      new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)]
  (is (:allocations (f old-replica)) => {:j1 {:t1 [:a :b]} :j2 {:t2 [:c]}})
  (is (rep-reactions old-replica new-replica diff {:id :a}) => nil)
  (is (rep-reactions old-replica new-replica diff {:id :b}) => nil)
  (is (rep-reactions old-replica new-replica diff {:id :c}) => nil))

(let [entry (create-log-entry :leave-cluster {:id :c})
      f (partial extensions/apply-log-entry entry)
      rep-diff (partial extensions/replica-diff entry)
      rep-reactions (partial extensions/reactions entry)
      old-replica (merge replica/base-replica 
                         {:job-scheduler :onyx.job-scheduler/balanced
                          :messaging {:onyx.messaging/impl :dummy-messenger}
                          :pairs {:a :b :b :c :c :a}
                          :peers [:a :b :c]
                          :jobs [:j1 :j2]
                          :task-schedulers {:j1 :onyx.task-scheduler/balanced
                                            :j2 :onyx.task-scheduler/balanced}
                          :tasks {:j1 [:t1] :j2 [:t2]}
                          :allocations {:j1 {:t1 [:a :b]} :j2 {:t2 [:c]}}})
      new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)]
  (is (:allocations new-replica) => {:j1 {:t1 [:a]} :j2 {:t2 [:b]}})
  (is (rep-reactions old-replica new-replica diff {:id :a}) => nil)
  (is (rep-reactions old-replica new-replica diff {:id :b}) => nil))
