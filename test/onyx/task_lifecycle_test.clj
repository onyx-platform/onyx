(ns onyx.task-lifecycle-test
  (:require [clojure.test :refer [deftest is testing]]
            [onyx.state.state-extensions :as state-extensions]
            [onyx.peer.task-lifecycle :as task-lifecycle]))

(deftest initialise-log-test
  (let [event {:onyx.core/job-id :job2
               :onyx.core/id :peer-id-1
               :onyx.core/task-id :task-id-1
               :onyx.core/replica (atom {:task-slot-ids {:job-1 {:task-id-1 {:peer-id-1 :slot-0}}}})
               :onyx.core/test-entries-log {:slot-0 (atom [])}}
        log (state-extensions/initialise-log :atom event)]
    (is (= @log [])))) 

(deftest replay-window-from-log-test
  (let [event {:onyx.core/replica (atom {:task-slot-ids {:job-1 {:task-id-1 {:peer-id-1 :slot-0}}}})
               :onyx.core/windows [{:window/id :a
                                    :window/agg-init (fn [window] [])
                                    :window/log-resolve (fn [state [t entry]]
                                                          (case t
                                                            :conj (conj state entry)))}
                                   {:window/id :b
                                    :window/agg-init (fn [window] nil)
                                    :window/log-resolve (fn [state [t entry]]
                                                          (case t
                                                            :set-value entry))}]
               :onyx.core/test-entries-log {:slot-0 (atom [])}
               :onyx.core/window-state (atom {})
               :onyx.core/state-log (atom [[:a :extent [:conj 1]]
                                           [:a :extent [:conj 2]]
                                           [:b :extent [:set-value 2]]
                                           [:b :extent [:set-value 4]]])}
        replayed (task-lifecycle/replay-windows-from-log event)]
    (is (= @(:onyx.core/window-state event)
           {:a {:extent [1 2]} 
            :b {:extent 4}}))))
