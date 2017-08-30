(ns onyx.triggers.trigger-percentile-watermark-test
  (:require [clojure.test :refer [deftest is]]
            [onyx.types :refer [map->StateEvent]]
            [onyx.triggers]
            [onyx.api]))

(deftest percentile-watermark-test
  (let [t 1444443468904
        window {:window/id :collect-segments
                :window/task :identity
                :window/type :fixed
                :window/aggregation :onyx.windowing.aggregation/count
                :window/window-key :event-time
                :window/range [5 :minutes]}
        trigger {:trigger/window-id :collect-segments
                 
                 :trigger/on :onyx.triggers/percentile-watermark
                 :trigger/watermark-percentage 0.50
                 :trigger/sync ::no-op
                 :trigger/id :trigger-id}
        segment {:event-time t}
        event {}]

    (is (onyx.triggers/percentile-watermark-fire? trigger 
                                                  nil 
                                                  (map->StateEvent {:window window 
                                                                    :segment segment
                                                                    :lower-bound (- t 75)
                                                                    :upper-bound (+ t 25)}))))

  (let [t 1444443468904
        window {:window/id :collect-segments
                :window/task :identity
                :window/type :fixed
                :window/aggregation :onyx.windowing.aggregation/count
                :window/window-key :event-time
                :window/range [5 :minutes]}
        trigger {:trigger/window-id :collect-segments
                 
                 :trigger/on :onyx.triggers/percentile-watermark
                 :trigger/watermark-percentage 0.50
                 :trigger/sync ::no-op
                 :trigger/id :trigger-id}
        segment {:event-time t}
        event {}]
    (is
     (not
      (onyx.triggers/percentile-watermark-fire? trigger nil 
                                                (map->StateEvent {:window window 
                                                                  :segment segment
                                                                  :lower-bound (- t 25)
                                                                  :upper-bound (+ t 75)}))))))
