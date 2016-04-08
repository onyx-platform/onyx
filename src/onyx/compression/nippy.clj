(ns onyx.compression.nippy
  (:require [taoensso.nippy :as nippy]
            [schema.utils :as su])
  (:import [schema.utils ValidationError NamedError]))

(nippy/extend-freeze clojure.lang.ExceptionInfo :onyx/exception-info
                     [x data-output]
                     (.writeUTF data-output (str {:ex (.getData x)
                                                  :str (.getMessage x)
                                                  :cause (.getCause x)})))

(nippy/extend-thaw :onyx/exception-info
                   [data-input]
                   (let [{:keys [ex str cause]} (read-string (.readUTF data-input))]
                     (clojure.lang.ExceptionInfo. str ex cause)))

(def messaging-compress-opts {})

(defn messaging-compress [x]
  (nippy/freeze x messaging-compress-opts))

(def messaging-decompress-opts {:v1-compatibility? false})

(defn messaging-decompress [x]
  (nippy/thaw x messaging-decompress-opts))

(def zookeeper-compress-opts {})
(def aaa (atom []))
(defn zookeeper-compress [x]
  (swap! aaa conj x)
  (nippy/freeze x zookeeper-compress-opts))

(def zookeeper-decompress-opts {:v1-compatibility? false})

(defn zookeeper-decompress [x]
  (nippy/thaw x zookeeper-decompress-opts))

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
