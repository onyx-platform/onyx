(ns onyx.triggers.trigger-timer-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [onyx.windowing.aggregation]
            [onyx.refinements]
            [onyx.triggers]
            [onyx.windowing.window-compile :as wc]
            [onyx.windowing.window-extensions :as we]
            [onyx.peer.window-state :as ws]
            [schema.test]
            [onyx.api]))

(use-fixtures :once schema.test/validate-schemas)

(def trigger-count (atom 0))

(defn fire [& all]
  (swap! trigger-count inc))

(deftest trigger-timer-test 
  (let [window {:window/id :collect-segments
                :window/task :identity
                :window/type :fixed
                :window/aggregation :onyx.windowing.aggregation/count
                :window/window-key :event-time
                :window/range [5 :minutes]}
        triggers [{:trigger/window-id :collect-segments
                   :trigger/refinement :onyx.refinements/accumulating
                   :trigger/on :onyx.triggers/timer
                   :trigger/period [2 :seconds]
                   :trigger/sync ::fire
                   :trigger/id :trigger-id}]
        task-map {}
        event {}
        segment {:id 1  :age 21 :event-time #inst "2015-09-13T03:00:00.829-00:00"}
        windows-state [(wc/resolve-window-state window triggers task-map)]
        windows-state-next (ws/fire-state-event windows-state 
                                                (assoc (ws/new-state-event :new-segment event) 
                                                       :segment segment))]
    (is (zero? @trigger-count))
    (Thread/sleep 1000)
    (ws/fire-state-event windows-state-next 
                         (ws/new-state-event :timer-tick event))
    (Thread/sleep 500)
    (ws/fire-state-event windows-state-next 
                         (ws/new-state-event :timer-tick event))
    (Thread/sleep 500)
    (ws/fire-state-event windows-state-next 
                         (ws/new-state-event :timer-tick event))

    (is (= 1 @trigger-count))))
