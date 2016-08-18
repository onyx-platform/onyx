(ns onyx.state.filter.lmdb
  (:import  [org.apache.commons.io FileUtils])
  (:require [onyx.state.state-extensions :as state-extensions]
            [clojure.core.async :refer [chan >!! <!! alts!! timeout go <! alts! close! thread]]
            [onyx.static.default-vals :refer [arg-or-default defaults]]
            [onyx.compression.nippy :as nippy]
            [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [clj-lmdb.simple :as lmdb]))


(defrecord LMDBInstance [dir db id-counter buckets bucket rotation-thread shutdown-ch])

;; TODO - implement if needed
(defn build-bucket [db id]
  {})

(defmethod state-extensions/initialize-filter :lmdb [_ {:keys [onyx.core/peer-opts onyx.core/id onyx.core/task-id] :as event}] 
  (let [db_size    500000          ;;500MB
        ;; options from schema
        compression-opt    (arg-or-default :onyx.rocksdb.filter/compression peer-opts)
        block-size         (arg-or-default :onyx.rocksdb.filter/block-size peer-opts)
        block-cache-size   (arg-or-default :onyx.rocksdb.filter/peer-block-cache-size peer-opts)
        base-dir-path      (arg-or-default :onyx.rocksdb.filter/base-dir peer-opts)
        bloom-filter-bits  (arg-or-default :onyx.rocksdb.filter/bloom-filter-bits peer-opts)
        
        ;; db location
        base-dir-path-file ^java.io.File (java.io.File. ^String base-dir-path)
        _ (when-not (.exists base-dir-path-file) (.mkdir base-dir-path-file))
        db-dir (str base-dir-path "/" id "_" task-id)
        db-path-file ^java.io.File (java.io.File. ^String db-dir)
        _ (when-not (.exists db-path-file) (.mkdir db-path-file))
        ; bloom-filter (BloomFilter. bloom-filter-bits false)
        ; block-config (doto (BlockBasedTableConfig.)
        ;                (.setBlockSize block-size)
        ;                (.setBlockCacheSize block-size)
        ;                (.setFilter bloom-filter))
        ; options (doto (Options.)
        ;           (.setCompressionType ^CompressionType (r/compression-option->type compression-opt))
        ;           (.setCreateIfMissing true)
        ;           (.setTableFormatConfig block-config))
        ; db (RocksDB/open options db-dir)
        db (lmdb/make-named-db db-dir 
                               "db"
                               db_size)
        initial-bucket (build-bucket db (java.util.UUID/randomUUID))
        buckets    (atom [initial-bucket])
        bucket     (atom initial-bucket)
        id-counter (atom 0)
        shutdown-ch (chan 1)
        ; rotation-thread (start-rotation-thread! shutdown-ch peer-opts db id-counter buckets bucket)
        rotation-thread nil
        ]
    (println "Filter initialized")    
    (->LMDBInstance db-dir db id-counter buckets bucket rotation-thread shutdown-ch)))