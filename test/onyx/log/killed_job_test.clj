(ns onyx.log.killed-job-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.log.replica :as replica]
            [onyx.system]
            [clojure.test :refer [deftest is testing]]))

(def entry (create-log-entry :kill-job {:job :j1}))

(def f (partial extensions/apply-log-entry entry))

(def rep-diff (partial extensions/replica-diff entry))

(def rep-reactions (partial extensions/reactions entry))

(def old-replica (merge replica/base-replica
                        {:messaging {:onyx.messaging/impl :dummy-messenger}
                         :job-scheduler :onyx.job-scheduler/greedy
                         :jobs [:j1]
                         :tasks {:j1 [:t1 :t2]}
                         :allocations {:j1 {:t1 [:a :b] :t2 [:c]}}
                         :task-metadata {:j1 {:t1 [:task-data]}}
                         :task-schedulers {:j1 :onyx.task-scheduler/balanced}
                         :peer-state {:a :active :b :active :c :active}
                         :peers [:a :b :c]}))

(let [new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      a-reactions (rep-reactions old-replica new-replica diff {:id :a})
      d-reactions (rep-reactions old-replica new-replica diff {:id :d})]
  (fact (:killed-jobs new-replica) => [:j1])
  (fact (:task-metadata new-replica) => {})
  (fact (get-in new-replica [:allocations :j1]) => nil)
  (fact (get-in new-replica [:peer-state :a]) => :idle)
  (fact (get-in new-replica [:peer-state :b]) => :idle)
  (fact (get-in new-replica [:peer-state :c]) => :idle)

  (fact diff => #{:j1})

  (fact a-reactions => [])
  (fact d-reactions => []))
