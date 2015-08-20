(ns onyx.messaging.aeron.peer-manager
  "Fast way for peer group subscribers to dispatch via short id to peer channels"
  (:refer-clojure :exclude [assoc dissoc])
  (:import [uk.co.real_logic.agrona.collections Int2ObjectHashMap]))

(defprotocol PeerManager
  (clone [this])
  (assoc [this k v])
  (dissoc [this k])
  (peer-channels [this k]))

(deftype VPeerManager [^Int2ObjectHashMap m]
  PeerManager
  (assoc [this k v]
    (let [vp ^VPeerManager (clone this)]
      (.put (.m vp) (int k) v)
      vp))
  (dissoc [this k]
    (let [vp ^VPeerManager (clone this)]
      (.remove (.m vp) (int k))
      vp))
  (peer-channels [this k]
    (.get m (int k)))
  (clone [this]
    (VPeerManager.
      (let [iterator (.iterator (.entrySet m))
            new-hm (Int2ObjectHashMap.)]
        (while (.hasNext iterator)
          (let [kv (.next iterator)]
            (.put new-hm (.getKey kv) (.getValue kv))))
        new-hm))))

(defn vpeer-manager []
  (VPeerManager. (Int2ObjectHashMap.)))
