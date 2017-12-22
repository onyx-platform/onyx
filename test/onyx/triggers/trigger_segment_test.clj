(ns onyx.triggers.trigger-segment-test
  (:require [clojure.test :refer [deftest is]]
            [onyx.windowing.aggregation]
            [onyx.state.serializers.utils :as u]
            [onyx.state.protocol.db :as db]
            [onyx.refinements]
            [onyx.windowing.window-compile :as wc]
            [onyx.windowing.window-extensions :as we]
            [onyx.peer.window-state :as ws]
            [onyx.types :as t]
            [schema.test]
            [onyx.api]))

(def new-state (atom nil))

(defn fire [_ _ _ {:keys [next-state] :as state-event} cnt] 
  ;; Check that it's destructively refined to the right value
  (assert (nil? next-state))
  (reset! new-state cnt))

(deftest trigger-segment-test 
  (let [segments [{:id 1  :age 21 :event-time #inst "2015-09-13T03:00:00.829-00:00"}
                  {:id 2  :age 12 :event-time #inst "2015-09-13T03:04:00.829-00:00"}]
        window {:window/id :collect-segments
                :window/task :identity
                :window/type :fixed
                :window/aggregation :onyx.windowing.aggregation/count
                :window/window-key :event-time
                :window/range [5 :minutes]}
        triggers [{:trigger/window-id :collect-segments
                   :trigger/on :onyx.triggers/segment
                   :trigger/post-evictor [:all]
                   :trigger/threshold [2 :elements]
                   :trigger/sync ::fire
                   :trigger/id :trigger-id}]
        task-map {}
        event {}
        peer-config {}
        event {:onyx.core/windows [window]
               :onyx.core/triggers triggers
               :onyx.core/task-map task-map}
        state-store (db/create-db peer-config 
                                  {:onyx.peer/state-store-impl :memory}
                                  (u/event->state-serializers event))
        state-indices (ws/state-indices event)
        windows-state [(wc/build-window-executor window triggers state-store state-indices task-map)]
        segment1 {:event-time #inst "2016-02-18T12:56:00.910-00:00"}
        new-segment-event (assoc (t/new-state-event :new-segment event 0 0) :segment segment1)
        ws-1 (ws/fire-state-event windows-state new-segment-event (transient []))
        _ (is (nil? @new-state))
        ws-2 (ws/fire-state-event ws-1 new-segment-event (transient []))
        _ (is (= 2 @new-state))
        ws-3 (ws/fire-state-event ws-2 new-segment-event (transient []))
        _ (is (= 2 @new-state))
        ;; Ensure that new-state is nil so we can check if it fires correctly
        ;; the second time
        _ (reset! new-state nil)
        ws-4 (ws/fire-state-event ws-3 new-segment-event (transient []))]
    (is (= 2 @new-state))))
