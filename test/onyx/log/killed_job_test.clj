(ns onyx.log.killed-job-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.system]
            [midje.sweet :refer :all]))

(def entry (create-log-entry :kill-job {:job :j1}))

(def f (partial extensions/apply-log-entry entry))

(def rep-diff (partial extensions/replica-diff entry))

(def rep-reactions (partial extensions/reactions entry))

(def old-replica {:job-scheduler :onyx.job-scheduler/greedy
                  :jobs [:j1]
                  :tasks {:j1 [:t1 :t2]}
                  :allocations {:j1 {:t1 [:a :b] :t2 [:c]}}
                  :task-schedulers {:j1 :onyx.task-scheduler/round-robin}
                  :peer-state {:a :active :b :active :c :active}
                  :peers [:a :b :c]})

(let [new-replica (f old-replica)]
  (fact (:killed-jobs new-replica) => [:j1])
  (fact (get-in new-replica [:allocations :j1]) => nil)
  (fact (get-in new-replica [:peer-state :a]) => :idle)
  (fact (get-in new-replica [:peer-state :b]) => :idle)
  (fact (get-in new-replica [:peer-state :c]) => :idle))

;; (let [new-replica (f old-replica)
;;       diff (rep-diff old-replica new-replica)]
;;   (fact (:completions new-replica) => {:j1 [:t1]})
;;   (fact (:allocations new-replica) => {:j1 {:t2 [:c]}})
;;   (fact (rep-reactions old-replica new-replica diff {:id :a}) =>
;;         [{:fn :volunteer-for-task :args {:id :a}}])
;;   (fact (rep-reactions old-replica new-replica diff {:id :b}) =>
;;         [{:fn :volunteer-for-task :args {:id :b}}])
;;   (fact (rep-reactions old-replica new-replica diff {:id :c}) => nil?))

;; (def entry (create-log-entry :volunteer-for-task {:id :a}))

;; (def f (partial extensions/apply-log-entry entry))

;; (def rep-diff (partial extensions/replica-diff entry))

;; (def rep-reactions (partial extensions/reactions entry))

;; (def old-replica {:job-scheduler :onyx.job-scheduler/greedy
;;                   :jobs [:j1 :j2]
;;                   :tasks {:j1 [:t1] :j2 [:t2]}
;;                   :allocations {:j1 {}}
;;                   :completions {:j1 [:t1]}
;;                   :task-schedulers {:j1 :onyx.task-scheduler/greedy :j2 :onyx.task-scheduler/greedy}
;;                   :peers [:a]})

;; (fact (:allocations (f old-replica)) => {:j1 {} :j2 {:t2 [:a]}})

;; (def old-replica {:job-scheduler :onyx.job-scheduler/round-robin
;;                   :jobs [:j1 :j2 :j3]
;;                   :tasks {:j1 [:t1] :j2 [:t2] :j3 [:t3]}
;;                   :allocations {:j1 {}}
;;                   :completions {:j1 [:t1]}
;;                   :task-schedulers {:j1 :onyx.task-scheduler/greedy
;;                                     :j2 :onyx.task-scheduler/greedy
;;                                     :j3 :onyx.task-scheduler/greedy}
;;                   :peers [:a]})

;; (fact (:allocations (f old-replica)) => {:j1 {} :j2 {:t2 [:a]}})

;; (def old-replica {:job-scheduler :onyx.job-scheduler/round-robin
;;                   :jobs [:j1 :j2 :j3]
;;                   :tasks {:j1 [:t1] :j2 [:t2] :j3 [:t3]}
;;                   :allocations {:j1 {}}
;;                   :completions {:j1 [:t1] :j2 [:t3]}
;;                   :task-schedulers {:j1 :onyx.task-scheduler/greedy
;;                                     :j2 :onyx.task-scheduler/greedy
;;                                     :j3 :onyx.task-scheduler/greedy}
;;                   :peers [:a]})

;; (fact (:allocations (f old-replica)) => {:j1 {} :j3 {:t3 [:a]}})

;; (def old-replica {:job-scheduler :onyx.job-scheduler/round-robin
;;                   :jobs [:j1]
;;                   :tasks {:j1 [:t1]}
;;                   :allocations {:j1 {}}
;;                   :completions {:j1 [:t1]}
;;                   :task-schedulers {:j1 :onyx.task-scheduler/greedy}
;;                   :peers [:a]})

;; (fact (:allocations (f old-replica)) => {:j1 {}})

;; (def entry (create-log-entry :volunteer-for-task {:id :a}))

;; (def f (partial extensions/apply-log-entry entry))

;; (def rep-diff (partial extensions/replica-diff entry))

;; (def rep-reactions (partial extensions/reactions entry))

;; (def old-replica {:job-scheduler :onyx.job-scheduler/greedy
;;                   :jobs [:j1]
;;                   :tasks {:j1 [:t1 :t2]}
;;                   :allocations {:j1 {}}
;;                   :completions {:j1 [:t1]}
;;                   :task-schedulers {:j1 :onyx.task-scheduler/greedy}
;;                   :peers [:a]})

;; (fact (:allocations (f old-replica)) => {:j1 {:t2 [:a]}})

