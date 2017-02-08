(ns onyx.peer.serialization-test
   (:require [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
             [taoensso.nippy :as n])
   (:import [onyx.serialization MessageEncoder MessageDecoder MessageEncoder$SegmentsEncoder]
            [org.agrona.concurrent UnsafeBuffer]
            [java.nio ByteBuffer]))

(comment (let [decoders #^"[Lclojure.lang.IFn;" (make-array clojure.lang.IFn 30)]
  (aset decoders (MessageEncoder/TEMPLATE_ID) (fn [v] (MessageDecoder.)))
  decoders)

(defn put-message-type [^UnsafeBuffer buf offset type-id] 
  (.putByte buf offset type-id))

(defn read-message-type [^UnsafeBuffer buf offset]
  (.getByte buf ^long offset))


(time 
 (let [encoder ^MessageEncoder (MessageEncoder.)
       buffer (byte-array 900000)
       buf ^UnsafeBuffer (UnsafeBuffer. buffer)]
   (dotimes [v 10000]
     (let [seg-count 200
           segments (map (fn [v]
                           {:n v :a "hiseta" :b "esntiarn"})
                         (range seg-count))
           encoder (wrap-message-encoder buf)
           replica-version 44
           dest-id 33
           encoder (-> encoder 
                       (.replicaVersion replica-version)
                       (.destId dest-id))
           len (add-segment-payload! encoder segments)
           decoder (wrap-message-decoder buf)
           segts (transient [])]
       {:replica-version (.replicaVersion decoder) 
        :dest-id (.destId decoder) 
        :segments (persistent! (into-segments! decoder segts))})))))
