(ns onyx.messaging.aeron.peer-manager
  "Fast way for peer group subscribers to multiplex via a short id to peer channels. "
  (:refer-clojure :exclude [assoc dissoc])
  (:require [taoensso.timbre :refer [fatal info] :as timbre])
  (:import [uk.co.real_logic.agrona.collections Int2ObjectHashMap Int2ObjectHashMap$EntryIterator])) 

(defrecord PeerChannels [acking-ch inbound-ch release-ch retry-ch])

;; Note, slow to assoc/dissoc to as it clones with a lock on it.
;; Very fast to get from via peer-channels function - which is the main case, as dissoc/assoc 
;; only occurs when peers join/leave
(defprotocol PeerManager
  (clone [this])
  (assoc [this k v])
  (dissoc [this k])
  (peer-channels [this k]))

(deftype VPeerManager [^Int2ObjectHashMap m]
  PeerManager
  (assoc [this k v]
    (let [vp ^VPeerManager (clone this)]
      (.put ^Int2ObjectHashMap (.m vp) (int k) ^PeerChannels v)
      vp))
  (dissoc [this k]
    (let [vp ^VPeerManager (clone this)]
      (.remove ^Int2ObjectHashMap (.m vp) (int k))
      vp))
  (peer-channels [this k]
    (.get m (int k)))
  (clone [this]
    ;; Unsure why a lock is needed, however concurrency bugs have shown up
    ;; Look into this later. Clone is a very infrequent operation
    (locking this 
      (VPeerManager.
        (let [iterator (.iterator (.entrySet (.m this)))
              new-hm ^Int2ObjectHashMap (Int2ObjectHashMap.)]
          (while (.hasNext iterator)
            (let [kv ^Int2ObjectHashMap$EntryIterator (.next iterator)
                  k ^java.lang.Integer (.getKey kv) 
                  v ^PeerChannels (.getValue kv)]
              (.put new-hm k v)))
          new-hm)))))

(defn vpeer-manager []
  (VPeerManager. (Int2ObjectHashMap.)))
