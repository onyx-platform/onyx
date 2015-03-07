(ns onyx.log.leave-cluster-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.system]
            [midje.sweet :refer :all]))

(def entry (create-log-entry :leave-cluster {:id :c}))

(def f (partial extensions/apply-log-entry entry))

(def rep-diff (partial extensions/replica-diff entry))

(def rep-reactions (partial extensions/reactions entry))

(def old-replica {:pairs {:a :b :b :c :c :a} :peers [:a :b :c]})

(let [new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)]
  (fact (get-in new-replica [:pairs :a]) => :b)
  (fact (get-in new-replica [:pairs :b]) => :a)
  (fact (get-in new-replica [:pairs :c]) => nil)
  (fact diff => {:died :c :updated-watch {:observer :b :subject :a}})
  (fact (rep-reactions old-replica new-replica diff {:id :a}) => nil))

(def entry (create-log-entry :leave-cluster {:id :b}))

(def f (partial extensions/apply-log-entry entry))

(def rep-diff (partial extensions/replica-diff entry))

(def rep-reactions (partial extensions/reactions entry))

(def old-replica {:pairs {:a :b :b :a} :peers [:a :b]})

(let [new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)]
  (fact (get-in new-replica [:pairs :a]) => nil)
  (fact (get-in new-replica [:pairs :b]) => nil)
  (fact diff => {:died :b :updated-watch {:observer :a :subject :a}})
  (fact (rep-reactions old-replica new-replica diff {:id :a}) => nil))

(def entry (create-log-entry :leave-cluster {:id :d}))

(def f (partial extensions/apply-log-entry entry))

(def rep-diff (partial extensions/replica-diff entry))

(def rep-reactions (partial extensions/reactions entry))

(def old-replica {:job-scheduler :onyx.job-scheduler/round-robin
                  :pairs {:a :b :b :c :c :d :d :a} 
                  :peers [:a :b :c :d]
                  :jobs [:j1 :j2]
                  :task-schedulers {:j1 :onyx.task-scheduler/round-robin
                                    :j2 :onyx.task-scheduler/round-robin} 
                  :tasks {:j1 [:t1] :j2 [:t2]}
                  :allocations {:j1 {:t1 [:a :b]}
                                :j2 {:t2 [:c :d]}}})

(let [new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)]
  (:allocations (f old-replica)) => {:j1 {:t1 [:a :b]} :j2 {:t2 [:c]}}
  (fact (rep-reactions old-replica new-replica diff {:id :a}) => nil)
  (fact (rep-reactions old-replica new-replica diff {:id :b}) => nil)
  (fact (rep-reactions old-replica new-replica diff {:id :c}) => nil))

(def entry (create-log-entry :leave-cluster {:id :c}))

(def f (partial extensions/apply-log-entry entry))

(def rep-diff (partial extensions/replica-diff entry))

(def rep-reactions (partial extensions/reactions entry))

(def old-replica {:job-scheduler :onyx.job-scheduler/round-robin
                  :pairs {:a :b :b :c :c :a} 
                  :peers [:a :b :c]
                  :jobs [:j1 :j2]
                  :task-schedulers {:j1 :onyx.task-scheduler/round-robin
                                    :j2 :onyx.task-scheduler/round-robin} 
                  :tasks {:j1 [:t1] :j2 [:t2]}
                  :allocations {:j1 {:t1 [:a :b]} :j2 {:t2 [:c]}}})

(let [new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)]
  (:allocations (f old-replica)) => {:j1 {:t1 [:a :b]} :j2 {:t2 []}}
  (fact (rep-reactions old-replica new-replica diff {:id :a}) => nil)
  (fact (rep-reactions old-replica new-replica diff {:id :b})
        => [{:fn :volunteer-for-task :args {:id :b}}]))

(def old-replica {:job-scheduler :onyx.job-scheduler/round-robin
                  :pairs {:a :b :b :c :c :a} 
                  :peers [:a :b :c]
                  :jobs [:j1 :j2]
                  :task-schedulers {:j1 :onyx.task-scheduler/round-robin
                                    :j2 :onyx.task-scheduler/round-robin} 
                  :tasks {:j1 [:t1] :j2 [:t2]}
                  :allocations {:j1 {:t1 [:a :b]} :j2 {:t2 [:c]}}
                  :sealing-tasks {:t2 :c}})

(let [new-replica (f old-replica)]
  (:sealing-tasks new-replica) => {})

