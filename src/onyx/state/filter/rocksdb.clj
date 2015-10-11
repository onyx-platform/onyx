(ns onyx.state.filter.rocksdb
  (:import [org.rocksdb RocksDB Options BloomFilter BlockBasedTableConfig]
           [org.apache.commons.io FileUtils])
  (:require [onyx.state.state-extensions :as state-extensions]
            [onyx.state.rocksdb :as r]
            [onyx.static.default-vals :refer [arg-or-default defaults]]
            [onyx.compression.nippy :as nippy]))

(defrecord RocksDbInstance [dir db bucket-counter cleanup-fut])

(defmethod state-extensions/initialize-filter :rocksdb [_ {:keys [onyx.core/peer-opts onyx.core/id onyx.core/task-id] :as event}] 
  (let [compression-opt (arg-or-default :onyx.rocksdb.filter/compression peer-opts)
        block-size (arg-or-default :onyx.rocksdb.filter/block-size peer-opts)
        block-cache-size (arg-or-default :onyx.rocksdb.filter/peer-block-cache-size peer-opts)
        base-dir-path (arg-or-default :onyx.rocksdb.filter/base-dir peer-opts)
        base-dir-path-file (java.io.File. base-dir-path)
        _ (when-not (.exists base-dir-path-file) (.mkdir base-dir-path-file))
        db-dir (str base-dir-path "/" id "_" task-id)
        _ (RocksDB/loadLibrary)
        bloom-filter (BloomFilter. (arg-or-default :onyx.rocksdb.filter/bloom-filter-bits peer-opts) false)
        block-config (doto (BlockBasedTableConfig.)
                       (.setBlockSize block-size)
                       (.setBlockCacheSize block-size)
                       (.setFilter bloom-filter))
        options (doto (Options.)
                  (.setCompressionType (r/compression-option->type compression-opt))
                  (.setCreateIfMissing true)
                  (.setTableFormatConfig block-config))
        db (RocksDB/open options db-dir)
        bucket-counter (byte-array 1)
        _ (aset bucket-counter 0 (byte 1))]
    (->RocksDbInstance db-dir db bucket-counter nil)))

(defmethod state-extensions/apply-filter-id onyx.state.filter.rocksdb.RocksDbInstance [rocks-db _ id] 
  (let [k ^bytes (nippy/localdb-compress id)]
    (.put ^RocksDB (:db rocks-db) k ^bytes (:bucket-counter rocks-db)))
  ;; Expects a filter back
  rocks-db)

(defmethod state-extensions/filter? onyx.state.filter.rocksdb.RocksDbInstance [rocks-db _ id] 
  (let [k (nippy/localdb-compress id)]
    (not (nil? (.get ^RocksDB (:db rocks-db) k)))))

(defmethod state-extensions/close-filter onyx.state.filter.rocksdb.RocksDbInstance [rocks-db _]
  (.close ^RocksDB (:db rocks-db))
  (FileUtils/deleteDirectory (java.io.File. (:dir rocks-db))))
