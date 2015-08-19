(ns onyx.messaging.gen-pick-peer
  (:require [onyx.peer.operation :as operation]
            [clojure.set :refer [intersection difference]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test :refer :all]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]))

(def peer-id-gen
  (gen/fmap (fn [_]
              (java.util.UUID/randomUUID))
            (gen/return nil)))

(deftest pick-peer-gen-test
  (checking "peer should stably select candidates even when the peers to select from change"
            (times 50)
            [[id peer-ids n-select peers-to-add n-remove]
             (gen/tuple
               peer-id-gen
               (gen/vector peer-id-gen)
               gen/s-pos-int
               (gen/vector peer-id-gen)
               gen/s-pos-int)]

            (let [selected-start (operation/select-n-peers id peer-ids n-select)
                  peer-ids-after (drop n-remove (shuffle (concat peer-ids peers-to-add)))
                  selected-after (operation/select-n-peers id peer-ids-after n-select)]

              ;; Possible set of peer ids has changed by maximum the number of added + the number of removed
              ;; The selection should not change any more than this
              (<= (+ (count peers-to-add) n-remove)
                  (count (difference (set selected-after) (set selected-start)))))))
