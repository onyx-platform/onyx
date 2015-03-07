(ns onyx.log.gc-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.system]
            [midje.sweet :refer :all]))

(def entry (create-log-entry :gc {:id :my-client-id}))

(def f (partial extensions/apply-log-entry entry))

(def rep-diff (partial extensions/replica-diff entry))

(def rep-reactions (partial extensions/reactions entry))

(def old-replica
  {:jobs [:j1 :j2 :j3]
   :tasks {:j1 [:t1 :t2]
           :j2 [:t3 :t4]
           :j3 [:t5 :t6]}
   :task-schedulers {:j1 :onyx.task-scheduler/round-robin
                     :j2 :onyx.task-scheduler/round-robin
                     :j3 :onyx.task-scheduler/round-robin}
   :killed-jobs [:j1]
   :completions {:j3 [:t5 :t6]}
   :allocations {:j1 {:t2 []}
                 :j2 {:t3 [:p1]}}})

(let [new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {})]
  (fact new-replica =>
        {:jobs [:j2]
         :tasks {:j2 [:t3 :t4]}
         :task-schedulers {:j2 :onyx.task-scheduler/round-robin}
         :killed-jobs []
         :completions {}
         :allocations {:j2 {:t3 [:p1]}}
         :percentages nil
         :saturation nil
         :task-percentages nil
         :input-tasks nil
         :output-tasks nil})
  (fact reactions => [])
  (fact diff => {:jobs #{:j1 :j3}
                 :killed-jobs #{:j1}
                 :completions {:j3 [:t5 :t6]}
                 :tasks {:j1 [:t1 :t2] :j3 [:t5 :t6]}
                 :allocations {:j1 {:t2 []}}}))

