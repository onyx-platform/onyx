(ns onyx.windowing.trigger-segment-test
  (:require [clojure.test :refer [deftest is]]
            [onyx.triggers.triggers-api :as api]
            [onyx.api]))

(deftest segment-trigger-test
  (let [window {:window/id :collect-segments
                :window/task :identity
                :window/type :fixed
                :window/aggregation :onyx.windowing.aggregation/count
                :window/window-key :event-time
                :window/range [5 :minutes]}
        trigger {:trigger/window-id :collect-segments
                 :trigger/refinement :accumulating
                 :trigger/on :segment
                 :trigger/threshold [5 :elements]
                 :trigger/sync ::no-op
                 :trigger/id :trigger-id}
        segment {}
        event {:onyx.triggers/segments (atom {:trigger-id 4})}]
    (is
     (api/trigger-fire?
      event trigger
      {:window window :segment segment})))

  (let [window {:window/id :collect-segments
                :window/task :identity
                :window/type :fixed
                :window/aggregation :onyx.windowing.aggregation/count
                :window/window-key :event-time
                :window/range [5 :minutes]}
        trigger {:trigger/window-id :collect-segments
                 :trigger/refinement :accumulating
                 :trigger/on :segment
                 :trigger/threshold [5 :elements]
                 :trigger/sync ::no-op
                 :trigger/id :trigger-id}
        segment {}
        event {:onyx.triggers/segments (atom {:trigger-id 3})}]
    (is
     (not
      (api/trigger-fire?
       event trigger
       {:window window :segment segment})))))
