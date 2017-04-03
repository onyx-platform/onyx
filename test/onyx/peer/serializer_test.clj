(ns onyx.peer.serializer-test
  (:require [taoensso.nippy :as nippy])
  (:import [org.agrona.concurrent UnsafeBuffer]))

(deftype LazyVal [^UnsafeBuffer key-buffer ^UnsafeBuffer val-buffer val-cached])

(def vs (object-array [(->LazyVal (UnsafeBuffer. ^bytes (nippy/fast-freeze 5))             
                                  (UnsafeBuffer. ^bytes (nippy/fast-freeze {:a :7}))            
                                  nil)
                       (->LazyVal (UnsafeBuffer. ^bytes (nippy/fast-freeze [:a :b]))
                                  (UnsafeBuffer. ^bytes (nippy/fast-freeze {:a :7}))            
                                  nil)
                       (->LazyVal (UnsafeBuffer. ^bytes (nippy/fast-freeze ["fff"]))
                                  (UnsafeBuffer. ^bytes (nippy/fast-freeze {:a :7}))            
                                  nil)]))

(def m {5 {:a :7} 
        [:a :b] {:a :7}
        ["fff"] {:a :7}})

(def szed (nippy/fast-freeze m))

(defn get-val [#^"[Lonyx.peer.serializer_test.LazyVal;" vs k]
  (let [kbs (UnsafeBuffer. ^bytes (nippy/fast-freeze k))
        cnt (alength vs)]
    (loop [i 0]
      (if-not (= i cnt)
        (let [lv ^onyx.peer.serializer_test.LazyVal (aget vs i)] 
          (if (zero? (.compareTo kbs (.key_buffer lv)))
            (nippy/fast-thaw (.byteArray ^UnsafeBuffer (.val_buffer lv)))
            (recur (unchecked-add-int i 1))))))))
