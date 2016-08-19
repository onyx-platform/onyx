(ns onyx.state.filter.lmdb
  (:import  [org.apache.commons.io FileUtils])
  (:require [onyx.state.state-extensions :as state-extensions]
            [clojure.core.async :refer [chan >!! <!! alts!! timeout go <! alts! close! thread]]
            [onyx.static.default-vals :refer [arg-or-default defaults]]
            [onyx.compression.nippy :as nippy]
            [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [clj-lmdb.core   :as lmdbc]
            [clj-lmdb.simple :as lmdbs]))

(defrecord LMDBInstance [dir db shutdown-ch])

(defmethod state-extensions/initialize-filter :lmdb [_ {:keys [onyx.core/peer-opts onyx.core/id onyx.core/task-id] :as event}] 
  (let [db_size    500000000          ;;500MB
        ;; options from schema
        base-dir-path      (arg-or-default :onyx.rocksdb.filter/base-dir peer-opts)
        ;; db location
        base-dir-path-file ^java.io.File (java.io.File. ^String base-dir-path)
        _ (when-not (.exists base-dir-path-file) (.mkdir base-dir-path-file))
        db-dir (str base-dir-path "/" id "_" task-id)
        db-path-file ^java.io.File (java.io.File. ^String db-dir)
        _ (when-not (.exists db-path-file) (.mkdir db-path-file))

        db (lmdbs/make-named-db db-dir 
                               "db"
                               db_size)
        _ (println "LMDB")

        shutdown-ch (chan 1)]

        (->LMDBInstance db-dir db shutdown-ch)))

(def magic-value 
  (doto (byte-array 1)
    (aset 0 (byte 99))))

(defmethod state-extensions/apply-filter-id onyx.state.filter.lmdb.LMDBInstance [lmdb _ id] 
  (let [k ^bytes (nippy/localdb-compress id)
        db (:db lmdb)]
       (lmdbs/with-txn [txn (lmdbc/write-txn db)]
          (lmdbc/put! ^clj_lmdb.core.NamedDB db
                      txn
                      k 
                      magic-value )))
  ;; Expects a filter back
  lmdb)

(defmethod state-extensions/filter? onyx.state.filter.lmdb.LMDBInstance [lmdb _ id] 
  (let [k ^bytes (nippy/localdb-compress id)
        db (:db lmdb)]
        (lmdbs/with-txn [txn (lmdbc/read-txn db)]
          (not (nil? (lmdbc/get! db
                                 txn
                                 k))))))

(defmethod state-extensions/close-filter onyx.state.filter.lmdb.LMDBInstance [lmdb _]
  (close! (:shutdown-ch lmdb))
  (doto ^org.fusesource.lmdbjni.Database (-> lmdb :db :db)  (.close))
  (doto ^org.fusesource.lmdbjni.Env      (-> lmdb :db :env) (.close))
  (FileUtils/deleteDirectory (java.io.File. ^String (-> lmdb :dir))))

(defmethod state-extensions/restore-filter onyx.state.filter.lmdb.LMDBInstance
  [lmdb event snapshot]
  lmdb)

(defmethod state-extensions/snapshot-filter onyx.state.filter.lmdb.LMDBInstance
  [filter-state _] 
  filter-state)
