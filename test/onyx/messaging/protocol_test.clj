(ns onyx.messaging.protocol-test
  (:require [clojure.test :refer [deftest is]]
            [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [onyx.messaging.serialize :as sz]
            [onyx.messaging.serializers.segment-encoder :as segment-encoder]
            [onyx.messaging.serializers.segment-decoder :as segment-decoder]
            [onyx.messaging.serializers.base-encoder :as base-encoder]
            [onyx.messaging.serializers.base-decoder :as base-decoder])
  (:import [org.agrona.concurrent UnsafeBuffer]))

(deftest test-segments-encoding 
  (let [buf (UnsafeBuffer. (byte-array 100000))
        enc (-> (segment-encoder/->Encoder buf nil nil)
                (segment-encoder/wrap 0))
        segments [{:n 1} {:n 2} {:n 3} {:n 4}]
        enc (reduce (fn [enc segment]
                      (segment-encoder/add-message enc (messaging-compress segment)))
                    enc
                    segments)
        decoder (-> (segment-decoder/->Decoder (byte-array 1000000) nil nil nil)
                    (segment-decoder/wrap (.buffer ^onyx.messaging.serializers.segment_encoder.Encoder enc) 0))
        vs (transient [])]
    (segment-decoder/read-segments! decoder vs messaging-decompress)
    (is (= segments (persistent! vs)))))

(deftest test-base-encoding 
  (let [buf (UnsafeBuffer. (byte-array 100000))
        type 0
        replica-version 99
        dest-id 25000
        payload-length 630
        enc (-> (base-encoder/->Encoder buf 0)
                (base-encoder/set-type type)
                (base-encoder/set-replica-version replica-version)
                (base-encoder/set-dest-id dest-id)
                (base-encoder/set-payload-length payload-length))
        decoder (-> (base-decoder/->Decoder nil 0)
                    (base-decoder/wrap (.buffer ^onyx.messaging.serializers.base_encoder.Encoder enc) 0))]
    (is (= (base-decoder/get-type decoder) type))
    (is (= (base-decoder/get-replica-version decoder) replica-version))
    (is (= (base-decoder/get-dest-id decoder) dest-id))
    (is (= (base-decoder/get-payload-length decoder) payload-length))))

(deftest non-segment-encoding 
  (let [msg (sz/barrier 33 44 989)
        buf (sz/serialize msg)
        start-offset 0 
        decoder (base-decoder/->Decoder buf start-offset)]
    (is (= msg (sz/deserialize buf 
                               (+ start-offset (base-decoder/length decoder)) 
                               (base-decoder/get-payload-length decoder))))))

(deftest segment-base 
  (let [msg (sz/barrier 33 44 989)
        buf (sz/serialize msg)
        start-offset 0 
        decoder (base-decoder/->Decoder buf start-offset)]
    (is (= msg (sz/deserialize buf 
                               (+ start-offset (base-decoder/length decoder)) 
                               (base-decoder/get-payload-length decoder))))))
