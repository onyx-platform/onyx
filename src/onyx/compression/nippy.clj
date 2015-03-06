(ns onyx.compression.nippy
  (:require [taoensso.nippy :as nippy]))

(defn compress [x]
  (nippy/freeze x))

(defn decompress [x]
  (nippy/thaw x {:v1-compatibility? false}))

