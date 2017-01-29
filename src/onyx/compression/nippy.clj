(ns onyx.compression.nippy
  (:require [taoensso.nippy :as nippy]
            [schema.utils :as su]
            [clojure.edn])
  (:import [schema.utils ValidationError NamedError]))

(nippy/extend-freeze clojure.lang.ExceptionInfo :onyx/exception-info
                     [x data-output]
                     (.writeUTF data-output (str {:ex (.getData x)
                                                  :str (.getMessage x)
                                                  :cause (.getCause x)})))

(defn exception-info-reader [tag value]
  (let [maybe-record? (pos? (.indexOf (name tag) "."))]
    (if-not maybe-record?
      {:tag (keyword tag)
       :value value
       :deserialize-attempted? false}
      (try
        (let [rec-class (Class/forName (name tag))
              create-method (.getMethod rec-class
                                        "create"
                                        (into-array Class [clojure.lang.IPersistentMap]))]
          (.invoke create-method rec-class (into-array Object [value])))
        (catch Throwable ex
          {:tag (keyword tag)
           :value value
           :deserialize-attempted? true
           :deserialize-exception-type (type ex)})))))

(nippy/extend-thaw :onyx/exception-info
                   [data-input]
                   (let [{:keys [ex str cause]}
                         (clojure.edn/read-string {:default exception-info-reader}
                                                  (.readUTF data-input))]
                     (clojure.lang.ExceptionInfo. str ex cause)))

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

(def localdb-compress-opts 
  {:v1-compatibility? false :compressor nil :encryptor nil :password nil})

(defn localdb-compress [x]
  (nippy/freeze x localdb-compress-opts))

(def local-db-decompress-opts 
  {:v1-compatibility? false :compressor nil :encryptor nil :password nil})

(defn localdb-decompress [x]
  (nippy/thaw x local-db-decompress-opts))

(def checkpoint-compress-opts 
  {:v1-compatibility? false :compressor nippy/lz4-compressor :encryptor nil :password nil})

(defn checkpoint-compress ^bytes [x]
  (nippy/freeze x checkpoint-compress-opts))

(def checkpoint-decompress-opts 
  {:v1-compatibility? false :compressor nippy/lz4-compressor :encryptor nil :password nil})

(defn checkpoint-decompress [^bytes x]
  (nippy/thaw x checkpoint-decompress-opts))
