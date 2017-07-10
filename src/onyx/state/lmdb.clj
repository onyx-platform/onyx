(ns onyx.state.lmdb
  "Implementation of a LMDB backed state store. Currently this is alpha level quality."
  (:require [onyx.state.protocol.db :as db]
            [onyx.state.serializers.utils :as u]
            [onyx.state.serializers.windowing-key-encoder :as enc :refer [encode-key]]
            [onyx.state.serializers.windowing-key-decoder :as dec]
            [onyx.state.serializers.checkpoint :as cp]
            [onyx.compression.nippy :refer [statedb-compress statedb-decompress]])
  (:import [org.fusesource.lmdbjni Database Env Transaction Entry]
           [org.agrona MutableDirectBuffer]
           [org.agrona.concurrent UnsafeBuffer]))

(defn ^Transaction read-txn [^Env env]
   (.createReadTransaction env))

 (defn items
   [^Database db txn]
   (iterator-seq (.iterate db txn)))

(defn db-empty? [db env]
  (let [txn (read-txn env)]
    (try 
     (zero? (count (items db txn)))
     (finally
      (.abort txn)))))

;; TODO: export metrics
(defn stat [^Database db]
  (let [stat (.stat db)]
    {:ms-entries (.ms_entries stat)
     :ms-psize (.ms_psize stat)
     :ms-overflow-pages (.ms_overflow_pages stat)
     :ms-depth (.ms_depth stat)
     :ms-leaf-pages (.ms_leaf_pages stat)}))

(defn get-state-idx [^bytes bs]
  ;; remove extra allocation
  (.getShort (UnsafeBuffer. bs) 0))

(defn set-state-idx [^bytes bs idx]
  ;; remove extra allocation
  (.putShort (UnsafeBuffer. bs) 0 idx))

(deftype StateBackend [^Database db ^String name ^Env env serialize-fn deserialize-fn 
                       window-encoders window-decoders trigger-encoders trigger-decoders]
  db/State
  (put-extent! [this window-id group-id extent v]
    (let [enc (get window-encoders window-id)]
      (.put db 
            ^bytes (encode-key enc window-id group-id extent)
            ^bytes (serialize-fn v))))
  (get-extent [this window-id group-id extent]
    (let [enc (get window-encoders window-id)]
      (some-> (.get db ^bytes (encode-key enc window-id group-id extent))
              (deserialize-fn))))
  (delete-extent! [this window-id group-id extent]
    (let [enc (get window-encoders window-id)]
      (.delete db ^bytes (encode-key enc window-id group-id extent))))
  (put-trigger! [this trigger-id group-id v]
    (let [enc (get trigger-encoders trigger-id)]
      (.put db 
            ^bytes (encode-key enc trigger-id group-id nil)
            ^bytes (serialize-fn v))))
  (get-trigger [this trigger-id group-id]
    (when-let [enc (get trigger-encoders trigger-id)] 
      (some-> (.get db ^bytes (encode-key enc trigger-id group-id nil))
              (deserialize-fn))))
  (group-id [this group-key]
    (serialize-fn group-key))
  (group-key [this group-id]
    ;; nil for ungrouped windows
    ;; improve the way this works?
    (some-> group-id deserialize-fn))
  (groups [this state-idx]
    ;; TODO, seek directly to state index, iterate until group id changes
    ;; For now, full scan will be fine
    (let [txn (read-txn env)
          decoder (or (get window-decoders state-idx) 
                      (get trigger-decoders state-idx))]
      (try 
       (->> (items db txn)
            (keep (fn [^Entry entry] 
                    (when (= state-idx (get-state-idx (.getKey entry)))
                      (dec/wrap-impl decoder (.getKey entry))
                      (some-> (dec/get-group decoder)
                              (deserialize-fn))))) 
            (into #{}))
       (finally
        (.abort txn)))))
  (trigger-keys [this]
    (let [txn (read-txn env)]
      (try 
       (let [iterator (.iterate db txn)
             vs (transient [])] 
         (loop []
           (if (.hasNext iterator)
             (let [entry ^Entry (.next iterator)]
               (when-let [d (get trigger-decoders (get-state-idx (.getKey entry)))]
                 (dec/wrap-impl d (.getKey entry))  
                 (conj! vs [(dec/get-state-idx d)
                            (dec/get-group d)
                            (db/group-key this (dec/get-group d))]))
               (recur))))
         (persistent! vs))
       (finally 
        (.abort txn)))))
  (group-extents [this window-idx group-id]
    ;; TODO, remove full group / extent-scan
    (let [ungrouped? (nil? group-id)
          txn (read-txn env)]
      (try 
       (let [iterator (.iterate db txn)
             decoder (get window-decoders window-idx)
             vs (transient [])] 
         (loop []
           (if (.hasNext iterator)
             (let [entry ^Entry (.next iterator)]
               (when (= window-idx (get-state-idx (.getKey entry)))
                 (dec/wrap-impl decoder (.getKey entry))
                 (when (or ungrouped?
                           ;; improve the comparison here without ever actually copying the group id out
                           (and (= (alength ^bytes group-id) (dec/get-group-len decoder))
                                (u/equals (dec/get-group decoder) group-id)))
                   (conj! vs (dec/get-extent decoder))))
               (recur))))
         ;; TODO: shouldn't need to sort when using the correct comparator
         (sort (persistent! vs)))
       (finally 
        (.abort txn)))))
  (drop! [this]
    ;; FIXME, does not actually delete the DB files
    (.drop db true))
  (close! [this]
    (.close env)
    (.close db))
  (export-reader [this]
    nil)
  (export [this state-encoder]
    (let [txn (read-txn env)]
      (try 
       (->> txn
            (items db)
            (run! (fn [^Entry entry]
                    (cp/set-next-bytes state-encoder (.getKey entry))
                    (cp/set-next-bytes state-encoder (.getValue entry))))) 
       (finally
        (.abort txn)))))
  (restore! [this state-decoder mapping]
    (when-not (db-empty? db env)
      (throw (Exception. "LMDB db is not empty. This should never happen.")))
    (loop []
      (let [k ^bytes (cp/get-next-bytes state-decoder)
            v ^bytes (cp/get-next-bytes state-decoder)]
        (when k
          (assert v)
          (let [serialized-state-index (get-state-idx k)] 
            (when-let [new-idx (mapping serialized-state-index)]
              ;; re-index state index
              (set-state-idx k new-idx)
              (.put db k v)))
          (recur))))))

(defmethod db/open-db-reader :lmdb
  [peer-config 
   definition
   {:keys [window-encoders 
           window-decoders 
           trigger-encoders 
           trigger-decoders]}]
  ;; Not implemented yet.
  nil)

(defmethod db/create-db 
  :lmdb
  [peer-config 
   db-name 
   {:keys [window-encoders 
           window-decoders 
           trigger-encoders 
           trigger-decoders]}]
  (let [max-size 1024000
        path (str (System/getProperty "java.io.tmpdir") "/onyx/" (java.util.UUID/randomUUID) "/")
        _ (.mkdirs (java.io.File. path))
        env (doto (Env. path)
              (.setMapSize max-size))
        db (.openDatabase env db-name)]
    (->StateBackend db name env 
                    statedb-compress 
                    statedb-decompress 
                    window-encoders 
                    window-decoders 
                    trigger-encoders 
                    trigger-decoders)))
