(ns onyx.compression.nippy
  (:require [taoensso.nippy :as nippy]))

(def compress-opts {:compressor nil})

(defn compress [x]
  (nippy/freeze x compress-opts))

(def decompress-opts {:v1-compatibility? false})

(defn decompress [x]
  (nippy/thaw x decompress-opts))

(defn window-log-compress [x]
  (nippy/freeze x {}))

(def window-log-decompress-opts {:v1-compatibility? false})

(defn window-log-decompress [x]
  (nippy/thaw x window-log-decompress-opts))

(def localdb-compress-opts {:skip-header? true :compressor nil :encryptor nil :password nil})

(defn localdb-compress [x]
  (nippy/freeze x localdb-compress-opts))

(def local-db-decompress-opts {:v1-compatibility? false :compressor nil :encryptor nil})

(defn localdb-decompress [x]
  (nippy/thaw x local-db-decompress-opts))
