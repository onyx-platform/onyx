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
        enc (-> (segment-encoder/->Encoder buf 10 nil nil)
                (segment-encoder/wrap 0))
        segments [{:n 1} {:n 2} {:n 3} {:n 4}]
        enc (reduce (fn [enc segment]
                      (segment-encoder/add-message enc (messaging-compress segment)))
                    enc
                    segments)
        decoder (segment-decoder/wrap (.buffer ^onyx.messaging.serializers.segment_encoder.Encoder enc)
                                      (byte-array 1000000)
                                      0)
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

(deftest test-serialization-types
  (let [buf (UnsafeBuffer. (byte-array 500))
        rr (onyx.types/ready-reply 33 
                                   [:coordinator (java.util.UUID/randomUUID)]
                                   [:coordinator (java.util.UUID/randomUUID)]
                                   33
                                   4)
        _ (sz/serialize buf 0 rr)
        _ (is (= rr (sz/deserialize buf 0)) [rr (sz/deserialize buf 0)])
        r (onyx.types/ready 33 [:coordinator (java.util.UUID/randomUUID)] 4)
        _ (sz/serialize buf 0 r)
        _ (is (= r (sz/deserialize buf 0)) [r (sz/deserialize buf 0)])
        hb (assoc (onyx.types/heartbeat 33 44 
                                        [:coordinator (java.util.UUID/randomUUID)]
                                        [:coordinator (java.util.UUID/randomUUID)]
                                        33
                                        4) 
                  :opts :hey)
        _ (sz/serialize buf 0 hb)
        _ (is (= hb (sz/deserialize buf 0)) [hb (sz/deserialize buf 0)])
        barrier (merge (onyx.types/barrier 388 8 44) {:some :optional-stuff})
        _ (sz/serialize buf 0 barrier)
        _ (is (= barrier (sz/deserialize buf 0)) [barrier (sz/deserialize buf 0)])]))
