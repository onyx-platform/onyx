(ns onyx.log.max-peers-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.log.commands.common :as common]
            [onyx.system]
            [midje.sweet :refer :all]))

(fact
 (common/balance-jobs
  {:peers [:a :b :c :d :e :f :g :h]
   :jobs [:j1 :j2]
   :tasks {:j1 [:t1 :t2] :j2 [:t3 :t4]}
   :saturation {:j1 Double/POSITIVE_INFINITY :j2 Double/POSITIVE_INFINITY}
   :task-schedulers {:j1 :onyx.task-scheduler/round-robin
                     :j2 :onyx.task-scheduler/round-robin}
   :job-scheduler :onyx.job-scheduler/round-robin})
 => {:j1 4 :j2 4})

(fact
 (common/balance-jobs
  {:peers [:a :b :c :d :e :f :g :h]
   :jobs [:j1 :j2]
   :tasks {:j1 [:t1 :t2] :j2 [:t3 :t4]}
   :saturation {:j1 2 :j2 Double/POSITIVE_INFINITY}
   :task-schedulers {:j1 :onyx.task-scheduler/round-robin
                     :j2 :onyx.task-scheduler/round-robin}
   :job-scheduler :onyx.job-scheduler/round-robin})
 => {:j1 2 :j2 6})

(fact
 (common/balance-jobs
  {:peers [:a :b :c :d :e :f :g :h]
   :jobs [:j1 :j2]
   :tasks {:j1 [:t1 :t2] :j2 [:t3 :t4]}
   :saturation {:j1 4 :j2 4}
   :task-schedulers {:j1 :onyx.task-scheduler/round-robin
                     :j2 :onyx.task-scheduler/round-robin}
   :job-scheduler :onyx.job-scheduler/round-robin})
 => {:j1 4 :j2 4})

(fact
 (common/balance-jobs
  {:peers [:a :b :c :d :e :f :g :h]
   :jobs [:j1 :j2]
   :tasks {:j1 [:t1 :t2] :j2 [:t3 :t4]}
   :saturation {:j1 2 :j2 3}
   :task-schedulers {:j1 :onyx.task-scheduler/round-robin
                     :j2 :onyx.task-scheduler/round-robin}
   :job-scheduler :onyx.job-scheduler/round-robin})
 => {:j1 2 :j2 3})

(fact
 (common/balance-jobs
  {:peers [:a :b :c]
   :jobs [:j1 :j2]
   :tasks {:j1 [:t1 :t2] :j2 [:t3 :t4]}
   :saturation {:j1 5 :j2 5}
   :task-schedulers {:j1 :onyx.task-scheduler/round-robin
                     :j2 :onyx.task-scheduler/round-robin}
   :job-scheduler :onyx.job-scheduler/round-robin})
 => {:j1 2 :j2 1})

(fact
 (common/balance-jobs
  {:peers [:a :b :c :d :e :f :g :h]
   :jobs [:j1 :j2]
   :tasks {:j1 [:t1 :t2] :j2 [:t3 :t4]}
   :task-schedulers {:j1 :onyx.task-scheduler/round-robin
                     :j2 :onyx.task-scheduler/round-robin}
   :job-scheduler :onyx.job-scheduler/round-robin})
 => {:j1 4 :j2 4})

