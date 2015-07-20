(ns onyx.messaging.aeron.peer-manager
  "Fast way for peer group subscribers to dispatch via short id to peer channels"
  (:import [uk.co.real_logic.agrona.collections Int2ObjectHashMap]))

(defprotocol PeerManager 
  (clone [this])
  (add [this k v])
  (remove [this k])
  (peer-channels [this k]))

(deftype VPeerManager [^Int2ObjectHashMap m]
  PeerManager
  (add [this k v]
    (let [vp ^VPeerManager (clone this)] 
      (.put (.m vp) (int k) v)
      vp))
  (remove [this k]
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
