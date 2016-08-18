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
  (let [db_size    500000000          ;;500MB
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
    (->LMDBInstance db-dir db id-counter buckets bucket rotation-thread shutdown-ch)))

(def magic-value 
  (doto (byte-array 1)
    (aset 0 (byte 99))))

; (defmethod state-extensions/apply-filter-id onyx.state.filter.lmdb.LMDBInstance [lmdb _ id] 
;   (let [k ^bytes (nippy/localdb-compress id)]
;     (swap! (:id-counter lmdb) inc)
;     (lmdb/with-txn [txn (lmdb/write-txn (:db lmdb))]
;         (lmdb/put! ^clj_lmdb.core.NamedDB (:db lmdb)
;                    txn
;                    @(:bucket lmdb) 
;                    k)))
;   ;; Expects a filter back
;   lmdb)

; (defmethod state-extensions/apply-filter-id onyx.state.filter.rocksdb.RocksDbInstance [rocks-db _ id] 
;   (let [k ^bytes (nippy/localdb-compress id)]
;     (swap! (:id-counter rocks-db) inc)
;     (.put ^RocksDB (:db rocks-db) ^ColumnFamilyHandle @(:bucket rocks-db) k ^bytes magic-value))
;   ;; Expects a filter back
;   rocks-db)

; (defmethod state-extensions/filter? onyx.state.filter.rocksdb.RocksDbInstance [rocks-db _ id] 
;   (let [k ^bytes (nippy/localdb-compress id)
;         strbuf (StringBuffer.)
;         db ^RocksDB (:db rocks-db)]
;     (some (fn [^ColumnFamilyHandle bucket]
;             (let [may-exist? (.keyMayExist db bucket k strbuf)]
;               (and may-exist? 
;                    (or (pos? (.length strbuf))
;                        (not (nil? (.get db bucket k)))))))
;           @(:buckets rocks-db))))

; (defn clear-buckets! [{:keys [db bucket buckets] :as rocks-db}]
;   (run! (partial clear-bucket! db) @buckets)
;   (reset! buckets []))

; (defmethod state-extensions/close-filter onyx.state.filter.rocksdb.RocksDbInstance [rocks-db _]
;   (close! (:shutdown-ch rocks-db))
;   ;; Block until background processing has been stopped before closing the db
;   (<!! (:rotation-thread rocks-db))
;   (clear-buckets! rocks-db)
;   (.close ^RocksDB (:db rocks-db))
;   (FileUtils/deleteDirectory (java.io.File. ^String (:dir rocks-db))))

; (defn add-bucket! [{:keys [db bucket buckets] :as lmdb}
;                    bucket-values]
;   (let [new-bucket (build-bucket db (java.util.UUID/randomUUID))] 
;     (reset! bucket new-bucket)
;     (swap! buckets conj new-bucket)
;     (run! (fn [[k v]]
;             (.put ^RocksDB db new-bucket ^bytes k ^bytes v))
;           bucket-values)))

; (defmethod state-extensions/restore-filter onyx.state.filter.rocksdb.RocksDbInstance 
;   [{:keys [db bucket buckets id-counter] :as rocks-db} event snapshot]
;   (clear-buckets! rocks-db)
;   (reset! id-counter (:id-counter snapshot))
;   (run! #(add-bucket! rocks-db %) (:buckets snapshot))
;   rocks-db)

; (defn capture-bucket [^RocksDB db ^ReadOptions read-options ^ColumnFamilyHandle bucket]
;   (let [iterator ^RocksIterator (.newIterator db bucket read-options)]
;     (try
;       (.seekToFirst iterator)
;       (loop [ids (list)]
;         (if (.isValid iterator)
;           (let [id (list (.key iterator) (.value iterator))] 
;             (.next iterator)
;             (recur (conj ids id)))
;           ids))
;       (finally
;         (.dispose iterator)))))

; (defmethod state-extensions/snapshot-filter onyx.state.filter.rocksdb.RocksDbInstance 
;   [filter-state _] 
;   (let [db ^RocksDB (:db filter-state)
;         snapshot ^Snapshot (.getSnapshot db)
;         buckets @(:buckets filter-state)
;         id-counter @(:id-counter filter-state)
;         read-options ^ReadOptions (doto (ReadOptions.)
;                                     (.setSnapshot snapshot))]
;     (future 
;       (try {:id-counter id-counter 
;             :buckets (mapv #(capture-bucket db read-options %) buckets)}
;            (finally
;              (.releaseSnapshot db (.snapshot read-options)))))))
