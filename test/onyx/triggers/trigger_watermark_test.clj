(ns onyx.triggers.trigger-watermark-test
  (:require [clojure.test :refer [deftest is]]
            [onyx.peer.window-state :refer [map->StateEvent]]
            [onyx.triggers.triggers]
            [onyx.api]))

(deftest watermark-test
  (let [t (System/currentTimeMillis)
        window {:window/id :collect-segments
                :window/task :identity
                :window/type :fixed
                :window/aggregation :onyx.windowing.aggregation/count
                :window/window-key :event-time
                :window/range [5 :minutes]}
        trigger {:trigger/window-id :collect-segments
                 :trigger/refinement :onyx.triggers.refinements/accumulating
                 :trigger/on :watermark
                 :trigger/sync ::no-op
                 :trigger/id :trigger-id}
        segment {:event-time t}
        event {}]
    (is (onyx.triggers.triggers/watermark-fire? trigger nil 
                                                (map->StateEvent {:window window 
                                                                  :segment segment
                                                                  :upper-bound (dec t)}))))

  (let [t (System/currentTimeMillis)
        window {:window/id :collect-segments
                :window/task :identity
                :window/type :fixed
                :window/aggregation :onyx.windowing.aggregation/count
                :window/window-key :event-time
                :window/range [5 :minutes]}
        trigger {:trigger/window-id :collect-segments
                 :trigger/refinement :onyx.triggers.refinements/accumulating
                 :trigger/on :watermark
                 :trigger/sync ::no-op
                 :trigger/id :trigger-id}
        segment {:event-time t}
        event {}]
    (is
     (not
       (onyx.triggers.triggers/watermark-fire? trigger nil 
                                               (map->StateEvent {:window window 
                                                                 :segment segment
                                                                 :upper-bound (inc t)}))))))
