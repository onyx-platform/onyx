(ns onyx.state.serializers.group-reverse-encoder
  (:import [org.agrona.concurrent UnsafeBuffer]))

(defprotocol PEncoder
  (set-state-idx [this idx])
  (set-group-id [this group])
  (get-bytes [this])
  (wrap-impl [this bs])
  (length [this]))

(deftype GroupReverseEncoder [^UnsafeBuffer buffer offset]
  PEncoder
  (set-state-idx [this idx]
    (.putShort buffer offset idx))
  (set-group-id [this group-id]
    (.putBytes buffer (unchecked-add-int offset 2) group-id))
  (length [this] 10)
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs))
  (get-bytes [this]
    (let [ret-bs (byte-array (length this))]
      (.getBytes buffer 0 ^bytes ret-bs)
      ret-bs)))
