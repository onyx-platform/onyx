(ns onyx.windowing.trigger-percentile-watermark-test
  (:require [clojure.test :refer [deftest is]]
            [onyx.triggers.triggers-api :as api]
            [onyx.api]))

(deftest watermark-test
  (let [t 1444443468904
        window {:window/id :collect-segments
                :window/task :identity
                :window/type :fixed
                :window/aggregation :onyx.windowing.aggregation/count
                :window/window-key :event-time
                :window/range [5 :minutes]}
        trigger {:trigger/window-id :collect-segments
                 :trigger/refinement :accumulating
                 :trigger/on :percentile-watermark
                 :trigger/watermark-percentage 0.50
                 :trigger/sync ::no-op
                 :trigger/id :trigger-id}
        segment {:event-time t}
        event {}]
    (is
     (api/trigger-fire?
      event trigger
      {:window window :segment segment
       :lower-extent (- t 75)
       :upper-extent (+ t 25)})))

  (let [t 1444443468904
        window {:window/id :collect-segments
                :window/task :identity
                :window/type :fixed
                :window/aggregation :onyx.windowing.aggregation/count
                :window/window-key :event-time
                :window/range [5 :minutes]}
        trigger {:trigger/window-id :collect-segments
                 :trigger/refinement :accumulating
                 :trigger/on :percentile-watermark
                 :trigger/watermark-percentage 0.50
                 :trigger/sync ::no-op
                 :trigger/id :trigger-id}
        segment {:event-time t}
        event {}]
    (is
     (not
      (api/trigger-fire?
       event trigger
       {:window window :segment segment
        :lower-extent (- t 25)
        :upper-extent (+ t 75)})))))
