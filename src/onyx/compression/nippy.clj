(ns onyx.compression.nippy
  (:require [taoensso.nippy :as nippy]))

(defn compress [x]
  (nippy/freeze x {:compressor nil}))

(defn decompress [x]
  (nippy/thaw x {:v1-compatibility? false}))

(defn window-log-compress [x]
  (nippy/freeze x {}))

(defn window-log-decompress [x]
  (nippy/thaw x {:v1-compatibility? false}))
