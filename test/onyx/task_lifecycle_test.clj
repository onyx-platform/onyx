(ns onyx.task-lifecycle-test
  (:require [clojure.test :refer [deftest is testing]]
            [onyx.peer.task-lifecycle :as task-lifecycle]))

(deftest replay-window-from-log-test
  (let [event {:onyx.core/windows [{:window/id :a
                                    :window/log-resolve (fn [state [t entry]]
                                                          (case t
                                                            :conj (conj state entry)))}
                                   {:window/id :b
                                    :window/log-resolve (fn [state [t entry]]
                                                          (case t
                                                            :set-value entry))}]
               :onyx.core/window-state (atom {})
               :onyx.core/state-log (atom [[:a :extent [:conj 1]]
                                           [:a :extent [:conj 2]]
                                           [:b :extent [:set-value 2]]
                                           [:b :extent [:set-value 4]]
                                           ])}
        replayed (task-lifecycle/replay-windows-from-log event)]
    (= @(:onyx.core/window-state event)
       {:a {:extent '(2 1)}, :b {:extent 4}}))) 
