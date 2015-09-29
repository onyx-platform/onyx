(ns onyx.windowing.trigger-predicate-test
  (:require [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test :refer [deftest is]]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [onyx.triggers.triggers-api :as api]
            [onyx.triggers.watermark :as w]
            [onyx.api]))

(def true-pred (constantly true))

(def false-pred (constantly false))

(deftest punctuation-true-pred
  (let [window {:window/id :collect-segments
                :window/task :identity
                :window/type :fixed
                :window/aggregation :count
                :window/window-key :event-time
                :window/range [5 :minutes]}
        trigger {:trigger/window-id :collect-segments
                 :trigger/refinement :accumulating
                 :trigger/on :punctuation
                 :trigger/pred ::true-pred
                 :trigger/sync ::no-op
                 :trigger/id :trigger-id}
        segment {}
        event {:onyx.triggers/punctuation-preds {:trigger-id true-pred}}]
    (is
     (api/trigger-fire? event trigger {:window window :segment segment}))))

(deftest punctuation-false-pred
  (let [window {:window/id :collect-segments
                :window/task :identity
                :window/type :fixed
                :window/aggregation :count
                :window/window-key :event-time
                :window/range [5 :minutes]}
        trigger {:trigger/window-id :collect-segments
                 :trigger/refinement :accumulating
                 :trigger/on :punctuation
                 :trigger/pred ::false-pred
                 :trigger/sync ::no-op
                 :trigger/id :trigger-id}
        segment {}
        event {:onyx.triggers/punctuation-preds {:trigger-id false-pred}}]
    (is
     (not (api/trigger-fire? event trigger {:window window :segment segment})))))
