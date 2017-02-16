(ns ^:no-doc onyx.messaging.aeron.int2objectmap
  "Fast way to multiplex to short ids"
  (:require [taoensso.timbre :refer [fatal info] :as timbre])
  (:import [org.agrona.collections Int2ObjectHashMap Int2ObjectHashMap$KeyIterator Int2ObjectHashMap$EntryIterator])) 

;; Note, slow to assoc/dissoc as it makes a complete clone, but this only happens on reallocations
;; Very fast to get the object given an int
(defprotocol IntObjectMap
  (clone [this]))

(deftype CljInt2ObjectHashMap [^Int2ObjectHashMap m]
  clojure.lang.Associative
  (assoc [this k v]
    (let [vp ^CljInt2ObjectHashMap (clone this)]
      (.put ^Int2ObjectHashMap (.m vp) (int k) v)
      vp))
  clojure.lang.ILookup
  (valAt [this k]
    (.get m (int k)))
  (valAt [this k default]
    (or (.valAt this k) default))
  clojure.lang.IPersistentMap
  (without [this k]
    (let [vp ^CljInt2ObjectHashMap (clone this)]
      (.remove ^Int2ObjectHashMap (.m vp) (int k))
      vp))
  clojure.lang.Seqable
  (seq [this]
    (let [iterator ^Int2ObjectHashMap$EntryIterator (.iterator (.entrySet ^Int2ObjectHashMap (.m this)))]
      (loop [vs {}]
        (if (.hasNext iterator)
          (let [kv ^Int2ObjectHashMap$EntryIterator (.next iterator)
                k ^java.lang.Integer (.getKey kv) 
                v (.getValue kv)] 
            (recur (assoc vs k v)))
          (seq vs)))))

  java.util.Map
  ;; slow non lazy iterator is fine as we won't be accessing elements in this way
  ;; in the hot path. This is for convenience.
  (iterator [this]
    (clojure.lang.SeqIterator. (seq this)))

  IntObjectMap
  (clone [this]
    (CljInt2ObjectHashMap.
     (let [iterator ^Int2ObjectHashMap$EntryIterator (.iterator (.entrySet ^Int2ObjectHashMap (.m this)))
           new-hm ^Int2ObjectHashMap (Int2ObjectHashMap.)]
       (while (.hasNext iterator)
         (let [kv ^Int2ObjectHashMap$EntryIterator (.next iterator)
               k ^java.lang.Integer (.getKey kv) 
               v (.getValue kv)]
           (.put new-hm k v)))
       new-hm))))

(defn int2objectmap []
  (CljInt2ObjectHashMap. (Int2ObjectHashMap.)))

;(assoc (int2objectmap) 3 :b)

(comment (def a (reduce (int2objectmap)
                      (map (fn [i]
                             [i {:hi :there}])

                           (range 20)))) 

         (def b (reduce (fn [m i]
                          (assoc m i {:hi :there}))
                        {}
                        (range 20))))
