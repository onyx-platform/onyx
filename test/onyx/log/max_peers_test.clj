(ns onyx.log.max-peers-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.log.commands.common :as common]
            [midje.sweet :refer :all]))



(def old-replica {:peers [:a :b :c :d :e :f :g :h]
                  :jobs [:j1 :j2]
                  :tasks {:j1 [:t1 :t2] :j2 [:t3 :t4]}
                  :saturation {:j1 2 :j2 Double/POSITIVE_INFINITY}
                  :task-schedulers {:j1 :onyx.task-scheduler/round-robin
                                    :j2 :onyx.task-scheduler/round-robin}
                  :job-scheduler :onyx.job-scheduler/round-robin})

(common/balance-jobs old-replica)

