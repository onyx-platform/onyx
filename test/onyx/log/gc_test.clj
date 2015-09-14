(ns onyx.log.gc-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.system]
            [schema.core :as s]
            [onyx.log.replica :as replica]
            [clojure.test :refer [deftest is testing]]))

(namespace-state-changes [(around :facts (s/with-fn-validation ?form))])

(facts

(def entry (create-log-entry :gc {:id :my-client-id}))

(def f (partial extensions/apply-log-entry entry))

(def rep-diff (partial extensions/replica-diff entry))

(def rep-reactions (partial extensions/reactions entry))

(def old-replica
  (merge replica/base-replica 
         {:job-scheduler :onyx.job-scheduler/balanced
          :messaging {:onyx.messaging/impl :dummy-messenger}
          :jobs [:j2]
          :tasks {:j1 [:t1 :t2]
                  :j2 [:t3 :t4]
                  :j3 [:t5 :t6]}
          :task-schedulers {:j1 :onyx.task-scheduler/balanced
                            :j2 :onyx.task-scheduler/balanced
                            :j3 :onyx.task-scheduler/balanced}
          :killed-jobs [:j1]
          :completed-jobs [:j3]
          :allocations {:j1 {:t2 []}
                        :j2 {:t3 [:p1]}}}))

(let [new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {})]
  (is new-replica =>
        (merge replica/base-replica 
               {:job-scheduler :onyx.job-scheduler/balanced
                :messaging {:onyx.messaging/impl :dummy-messenger}
                :jobs [:j2]
                :tasks {:j2 [:t3 :t4]}
                :task-schedulers {:j2 :onyx.task-scheduler/balanced}
                :allocations {:j2 {:t3 [:p1]}}}))
  (is reactions => [])
  (is diff => {:killed-jobs #{:j1}
                 :completed-jobs #{:j3}
                 :tasks {:j1 [:t1 :t2] :j3 [:t5 :t6]}
                 :allocations {:j1 {:t2 []}}}))
)
