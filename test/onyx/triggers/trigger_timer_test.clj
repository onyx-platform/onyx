(ns onyx.triggers.trigger-timer-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [onyx.windowing.aggregation]
            [onyx.refinements]
            [onyx.triggers]
            [onyx.state.serializers.utils :as u]
            [onyx.windowing.window-compile :as wc]
            [onyx.state.protocol.db :as db]
            [onyx.windowing.window-extensions :as we]
            [onyx.peer.window-state :as ws]
            [onyx.types :as t]
            [schema.test]
            [onyx.api]))

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
                   
                   :trigger/on :onyx.triggers/timer
                   :trigger/period [2 :seconds]
                   :trigger/sync ::fire
                   :trigger/id :trigger-id}]
        task-map {:onyx/group-by :X}
        peer-config {}
        event {:onyx.core/windows [window]
               :onyx.core/triggers triggers
               :onyx.core/task-map task-map}
        state-store (db/create-db peer-config 
                                  {:onyx.peer/state-store-impl :memory}
                                  (u/event->state-serializers event))
        segment {:id 1 :age 21 :event-time #inst "2015-09-13T03:00:00.829-00:00"}
        state-indices (ws/state-indices event)
        windows-state [(wc/build-window-executor window triggers state-store state-indices task-map)]
        windows-state-next (ws/fire-state-event windows-state 
                                                (assoc (t/new-state-event :new-segment event 0 0)
                                                       :segment segment)
                                                (transient []))]
    (is (zero? @trigger-count))
    (Thread/sleep 1000)
    (ws/fire-state-event windows-state-next 
                         (t/new-state-event :timer-tick event 0 0)
                         (transient []))
    (Thread/sleep 500)
    (ws/fire-state-event windows-state-next 
                         (t/new-state-event :timer-tick event 0 0)
                         (transient []))
    (Thread/sleep 500)
    (ws/fire-state-event windows-state-next 
                         (t/new-state-event :timer-tick event 0 0)
                         (transient []))

    (is (= 1 @trigger-count))))
