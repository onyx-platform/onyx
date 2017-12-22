(ns onyx.triggers.trigger-punctuation-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [onyx.windowing.aggregation]
            [onyx.state.protocol.db :as db]
            [onyx.state.serializers.utils :as u]
            [onyx.refinements]
            [onyx.windowing.window-compile :as wc]
            [onyx.windowing.window-extensions :as we]
            [onyx.peer.window-state :as ws]
            [onyx.types :as t]
            [schema.test]
            [onyx.api]))

(def true-pred (constantly true))

(def trigger-fired? (atom false))

(defn fire [& all]
  (reset! trigger-fired? true))

(deftest trigger-punc-test 
  (let [segments [{:id 1  :age 21 :event-time #inst "2015-09-13T03:00:00.829-00:00"}
                  {:id 2  :age 12 :event-time #inst "2015-09-13T03:04:00.829-00:00"}
                  {:id 3  :age 3  :event-time #inst "2015-09-13T03:05:00.829-00:00"}
                  {:id 4  :age 64 :event-time #inst "2015-09-13T03:06:00.829-00:00"}
                  {:id 5  :age 53 :event-time #inst "2015-09-13T03:07:00.829-00:00"}
                  {:id 6  :age 52 :event-time #inst "2015-09-13T03:08:00.829-00:00"}
                  {:id 7  :age 24 :event-time #inst "2015-09-13T03:09:00.829-00:00"}
                  {:id 8  :age 35 :event-time #inst "2015-09-13T03:15:00.829-00:00"}
                  {:id 9  :age 49 :event-time #inst "2015-09-13T03:25:00.829-00:00"}
                  {:id 10 :age 37 :event-time #inst "2015-09-13T03:45:00.829-00:00"}
                  {:id 11 :age 15 :event-time #inst "2015-09-13T03:03:00.829-00:00"}
                  {:id 12 :age 22 :event-time #inst "2015-09-13T03:56:00.829-00:00"}
                  {:id 13 :age 83 :event-time #inst "2015-09-13T03:59:00.829-00:00"}
                  {:id 14 :age 60 :event-time #inst "2015-09-13T03:32:00.829-00:00"}
                  {:id 15 :age 35 :event-time #inst "2015-09-13T03:16:00.829-00:00"}]
        window {:window/id :collect-segments
                :window/task :identity
                :window/type :fixed
                :window/aggregation :onyx.windowing.aggregation/count
                :window/window-key :event-time
                :window/range [5 :minutes]}
        triggers [{:trigger/window-id :collect-segments
                   :trigger/on :onyx.triggers/punctuation
                   :trigger/pred ::true-pred
                   :trigger/sync ::fire
                   :trigger/id :trigger-id}]
        task-map {:onyx/group-by :X}
        event {:onyx.core/windows [window]
               :onyx.core/triggers triggers
               :onyx.core/task-map task-map}
        peer-config {}
        state-store (db/create-db peer-config 
                                  {:onyx.peer/state-store-impl :memory}
                                  (u/event->state-serializers event))
        state-indices (ws/state-indices event)
        windows-state [(wc/build-window-executor window triggers state-store state-indices task-map)]
        segment {:event-time #inst "2016-02-18T12:56:00.910-00:00"}]
    (ws/fire-state-event windows-state 
                         (assoc (t/new-state-event :new-segment event 0 0) 
                                :segment segment)
                         (transient []))
    (is @trigger-fired?)))
