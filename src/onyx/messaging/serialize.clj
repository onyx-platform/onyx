(ns onyx.messaging.serialize
  (:require [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [onyx.types :as t :refer [barrier ready ready-reply heartbeat]]
            [onyx.messaging.serializers.helpers :refer [uncoerce-peer-id coerce-peer-id]]
            [onyx.messaging.serializers.heartbeat-encoder :as hbenc]
            [onyx.messaging.serializers.heartbeat-decoder :as hbdec]
            [onyx.messaging.serializers.barrier-encoder :as benc]
            [onyx.messaging.serializers.barrier-decoder :as bdec]
            [onyx.messaging.serializers.ready-encoder :as renc]
            [onyx.messaging.serializers.ready-decoder :as rdec]
            [onyx.messaging.serializers.ready-reply-encoder :as rrenc]
            [onyx.messaging.serializers.ready-reply-decoder :as rrdec]
            [onyx.messaging.serializers.base-decoder :as basedec]
            [onyx.messaging.serializers.base-encoder :as baseenc])
  (:import [org.agrona.concurrent UnsafeBuffer]
           [onyx.messaging.serializers.base_decoder.Decoder]))


(defn deserialize 
  "Message payload deserializer for when you don't want to interact with decoders directly.
   Note: slower."
  [^UnsafeBuffer buf offset]
  (let [decoder (basedec/wrap (basedec/->Decoder nil offset) buf offset)
        t (basedec/get-type decoder)
        rv (basedec/get-replica-version decoder)
        dest-id (basedec/get-dest-id decoder)
        base {:type t
              :replica-version rv
              :short-id dest-id}]
    (cond (= t t/heartbeat-id)
          (let [hb-dec (hbdec/wrap buf (+ offset (basedec/length decoder)))]
            (merge base 
                   (messaging-decompress (hbdec/get-opts-map-bytes hb-dec))
                   {:epoch (hbdec/get-epoch hb-dec)
                    :session-id (hbdec/get-session-id hb-dec)
                    :src-peer-id (uncoerce-peer-id (hbdec/get-src-peer-id hb-dec))
                    :dst-peer-id (uncoerce-peer-id (hbdec/get-dst-peer-id hb-dec))}))

          (= t t/ready-reply-id)
          (let [rrdec (rrdec/wrap buf (+ offset (basedec/length decoder)))] 
            (merge base 
                   {:src-peer-id (uncoerce-peer-id (rrdec/get-src-peer-id rrdec))
                    :dst-peer-id (uncoerce-peer-id (rrdec/get-dst-peer-id rrdec))
                    :session-id (rrdec/get-session-id rrdec)}))

          (= t t/ready-id)
          (let [rdec (rdec/wrap buf (+ offset (basedec/length decoder)))] 
            (merge base 
                   {:src-peer-id (uncoerce-peer-id (rdec/get-src-peer-id rdec))}))

          (= t t/barrier-id)
          (let [bdec (bdec/wrap buf (+ offset (basedec/length decoder)))]
            (merge base 
                   {:epoch (bdec/get-epoch bdec)}
                   (messaging-decompress (bdec/get-opts-map-bytes bdec)))))))

(defn serialize 
  "Message payload serializer for when you don't want to interact with decoders directly.
   Note: slower than just building it without a map."
  [^UnsafeBuffer buf offset msg]
  (let [t (:type msg)
        enc (-> (baseenc/->Encoder nil offset)
                (baseenc/wrap buf offset)
                (baseenc/set-type t)
                (baseenc/set-replica-version (:replica-version msg))
                (baseenc/set-dest-id (:short-id msg)))]
    (cond (= t onyx.types/heartbeat-id)
          (let [hb-enc (-> (hbenc/wrap buf (baseenc/length enc))
                           (hbenc/set-epoch (:epoch msg))
                           (hbenc/set-session-id (:session-id msg))
                           (hbenc/set-src-peer-id (coerce-peer-id (:src-peer-id msg)))
                           (hbenc/set-dst-peer-id (coerce-peer-id (:dst-peer-id msg)))
                           (hbenc/set-opts-map-bytes (-> msg
                                                         (dissoc :type)
                                                         (dissoc :replica-version)
                                                         (dissoc :short-id)
                                                         (dissoc :session-id)
                                                         (dissoc :epoch)
                                                         (dissoc :src-peer-id)
                                                         (dissoc :dst-peer-id)
                                                         (messaging-compress))))]
            (baseenc/set-payload-length enc (hbenc/length hb-enc))
            (+ (hbenc/length hb-enc) (baseenc/length enc)))

          (= t onyx.types/barrier-id)
          (let [benc (-> (benc/wrap buf (baseenc/length enc))
                         (benc/set-epoch (:epoch msg))
                         (benc/set-opts-map-bytes (-> msg
                                                      (dissoc :type)
                                                      (dissoc :replica-version)
                                                      (dissoc :short-id)
                                                      (dissoc :epoch)
                                                      (messaging-compress))))]
            (baseenc/set-payload-length enc (benc/length benc))
            (+ (benc/length benc) (baseenc/length enc)))

          (= t onyx.types/ready-id)
          (let [renc (-> (renc/wrap buf (baseenc/length enc))
                         (renc/set-src-peer-id (coerce-peer-id (:src-peer-id msg))))]
            (baseenc/set-payload-length enc (renc/length renc))
            (+ (renc/length renc) (baseenc/length enc)))

          (= t onyx.types/ready-reply-id)
          (let [rrenc (-> (rrenc/wrap buf (baseenc/length enc))
                          (rrenc/set-src-peer-id (coerce-peer-id (:src-peer-id msg)))
                          (rrenc/set-dst-peer-id (coerce-peer-id (:dst-peer-id msg)))
                          (rrenc/set-session-id (:session-id msg)))]
            (baseenc/set-payload-length enc (rrenc/length rrenc))
            (+ (rrenc/length rrenc) (baseenc/length enc))))))

(comment )
