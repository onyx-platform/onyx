(ns ^:no-doc onyx.messaging.aeron.peer-manager
  "Fast way for peer group subscribers to multiplex via a short id to peer channels. "
  (:refer-clojure :exclude [assoc dissoc])
  (:require [taoensso.timbre :refer [fatal info] :as timbre])
  (:import [uk.co.real_logic.agrona.collections Int2ObjectHashMap Int2ObjectHashMap$KeyIterator Int2ObjectHashMap$EntryIterator])) 


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
      (.put ^Int2ObjectHashMap (.m vp) (int k) v)
      vp))
  (dissoc [this k]
    (let [vp ^VPeerManager (clone this)]
      (.remove ^Int2ObjectHashMap (.m vp) (int k))
      vp))
  (peer-channels [this k]
    (.get m (int k)))
  (clone [this]
    ;; Needs to be locked because we're using a deftype, not defrecord
    (locking this 
      (VPeerManager.
        (let [iterator ^Int2ObjectHashMap$EntryIterator (.iterator (.entrySet ^Int2ObjectHashMap (.m this)))
              new-hm ^Int2ObjectHashMap (Int2ObjectHashMap.)]
          (while (.hasNext iterator)
            (let [kv ^Int2ObjectHashMap$EntryIterator (.next iterator)
                  k ^java.lang.Integer (.getKey kv) 
                  v (.getValue kv)]
              (.put new-hm k v)))
          new-hm)))))

(defn vpeer-manager []
  (VPeerManager. (Int2ObjectHashMap.)))
