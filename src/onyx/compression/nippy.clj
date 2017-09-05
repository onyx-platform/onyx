(ns onyx.compression.nippy
  (:require [taoensso.nippy :as nippy :refer [fast-freeze fast-thaw]]))

(def messaging-compress-opts 
  {:v1-compatibility? false 
   :compressor nil
   :encryptor nil
   :password nil
   :no-header? true})

(def messaging-compress 
  nippy/fast-freeze)

(def messaging-decompress-opts 
  messaging-compress-opts)

(def messaging-decompress nippy/fast-thaw)

(def zookeeper-compress-opts {})

(defn zookeeper-compress [x]
  (nippy/freeze x zookeeper-compress-opts))

(def zookeeper-decompress-opts {:v1-compatibility? false})

(defn zookeeper-decompress [x]
  (nippy/thaw x zookeeper-decompress-opts))

(defn window-log-compress [x]
  (nippy/freeze x {}))

(def window-log-decompress-opts {:v1-compatibility? false})

(defn window-log-decompress [x]
  (nippy/thaw x window-log-decompress-opts))

(def ^:const statedb-compress fast-freeze)
(def ^:const statedb-decompress fast-thaw)

(def checkpoint-compress-opts 
  {:v1-compatibility? false :compressor nippy/lz4-compressor :encryptor nil :password nil})

(defn checkpoint-compress ^bytes [x]
  (nippy/freeze x checkpoint-compress-opts))

(def checkpoint-decompress-opts 
  {:v1-compatibility? false :compressor nippy/lz4-compressor :encryptor nil :password nil})

(defn checkpoint-decompress [^bytes x]
  (nippy/thaw x checkpoint-decompress-opts))
