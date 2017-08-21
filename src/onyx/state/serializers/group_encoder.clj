(ns onyx.state.serializers.group-encoder
  (:import [org.agrona.concurrent UnsafeBuffer]))

(defprotocol PEncoder
  (set-state-idx [this idx])
  (set-group [this group])
  (get-group-len [this])
  (get-bytes [this])
  (wrap-impl [this bs])
  (length [this]))

(defn group-id->bytes [id]
  (let [bs (byte-array 8)
        buf (UnsafeBuffer. bs)]
    (.putLong buf 0 id)
    bs))

(deftype GroupEncoder [^UnsafeBuffer buffer offset]
  PEncoder
  (set-state-idx [this idx]
    (.putShort buffer offset idx))
  (set-group [this bs]
    (.putShort buffer (unchecked-add-int offset 2) (short (alength ^bytes bs)))
    (.putBytes buffer (unchecked-add-int offset 4) ^bytes bs))
  (get-group-len [this]
    (.getShort buffer (unchecked-add-int offset 2)))
  (length [this]
    (unchecked-add-int 4 (get-group-len this)))
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs))
  (get-bytes [this]
    (let [ret-bs (byte-array (length this))]
      (.getBytes buffer 0 ^bytes ret-bs)
      ret-bs)))
