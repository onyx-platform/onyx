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

(defn window-log-decompress [x]
  (nippy/thaw x {:v1-compatibility? false}))

(defn localdb-compress [x]
  (nippy/freeze x {:skip-header? true :compressor nil :encryptor nil :password nil}))

(defn localdb-decompress [x]
  (nippy/thaw x {:v1-compatibility? false :compressor nil :encryptor nil}))
