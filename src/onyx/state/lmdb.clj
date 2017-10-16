(ns onyx.state.lmdb
  "Implementation of a LMDB backed state store. Currently this is alpha level quality."
  (:require [onyx.state.protocol.db :as db]
            [onyx.state.serializers.utils :as u]
            [onyx.state.serializers.windowing-key-encoder :as enc :refer [encode-key]]
            [onyx.state.serializers.windowing-key-decoder :as dec]
            [onyx.state.serializers.state-entry-key-encoder :as senc]
            [onyx.state.serializers.state-entry-key-decoder :as sdec]
            [onyx.state.serializers.group-encoder :as genc]
            [onyx.state.serializers.group-decoder :as gdec]
            [onyx.state.serializers.group-reverse-encoder :as grenc]
            [onyx.state.serializers.group-reverse-decoder :as grdec]
            [onyx.state.serializers.checkpoint :as cp]
            [onyx.compression.nippy :refer [statedb-compress statedb-decompress]])
  (:import [org.fusesource.lmdbjni Database Env Transaction Entry Constants]
           [org.agrona MutableDirectBuffer]
           [org.agrona.concurrent UnsafeBuffer]))

(defn ^Transaction read-txn [^Env env]
   (.createReadTransaction env))

(defn items
  [^Database db txn]
  (iterator-seq (.iterate db txn)))

(defn seek-items
  [^Database db txn ^bytes k]
  (iterator-seq (.seek db txn k)))

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

(deftype StateBackend [groups group-counter entry-counter ^String path ^Database db ^String name ^Env env 
                       serialize-fn deserialize-fn group-coder group-reverse-coder window-coders trigger-coders]
  db/State
  (put-state-entry! [this window-id group-id t v]
    (let [{:keys [entry-encoder]} (get window-coders window-id)]
      (some->> group-id (senc/set-group entry-encoder))
      (senc/set-time entry-encoder t)
      (senc/set-offset entry-encoder (long (swap! entry-counter inc)))
      (.put db 
            ^bytes (senc/get-bytes entry-encoder) 
            ^bytes (serialize-fn v))))
  (delete-state-entries! [this window-id group-id start end]
    (let [{:keys [entry-encoder entry-decoder entry-idx grouped?]} (get window-coders window-id)]
      (some->> group-id (senc/set-group entry-encoder))
      (senc/set-time entry-encoder start)
      (senc/set-offset entry-encoder 0)
      (let [seek-key (senc/get-bytes entry-encoder) 
            txn (read-txn env)]
        (try 
         (let [iterator (.seek db txn seek-key)] 
           (loop []
             (if (.hasNext iterator)
               (let [entry ^Entry (.next iterator)
                     key-bs (.getKey entry)]
                 (sdec/wrap-impl entry-decoder key-bs)
                 (when (and (= entry-idx (sdec/get-idx entry-decoder))
                            (u/equals group-id (sdec/get-group-id entry-decoder))
                            (<= (sdec/get-time entry-decoder) end))
                   (.delete db ^bytes key-bs)
                   (recur))))))
         (finally 
          (.abort txn))))))
  (get-state-entries [this window-id group-id start end]
    (let [{:keys [entry-encoder entry-decoder entry-idx]} (get window-coders window-id)
          vs (transient [])]
      (some->> group-id (senc/set-group entry-encoder))
      (senc/set-time entry-encoder start)
      (senc/set-offset entry-encoder 0)
      (let [seek-key (senc/get-bytes entry-encoder) 
            txn (read-txn env)]
        (try 
         (let [iterator (.seek db txn seek-key)] 
           (loop []
             (if (.hasNext iterator)
               (let [entry ^Entry (.next iterator)
                     _ (sdec/wrap-impl entry-decoder (.getKey entry))]
                 (when (and (= entry-idx (sdec/get-idx entry-decoder))
                            (u/equals group-id (sdec/get-group-id entry-decoder))
                            (<= (sdec/get-time entry-decoder) end))
                   (conj! vs (deserialize-fn (.getValue entry)))
                   (recur))))))
         (finally 
          (.abort txn))))
      (persistent! vs)))
  (put-extent! [this window-id group-id extent v]
    (let [{:keys [encoder grouped?]} (get window-coders window-id)]
      (.put db 
            ^bytes (encode-key encoder window-id group-id extent)
            ^bytes (serialize-fn v))))
  (get-extent [this window-id group-id extent]
    (let [{:keys [encoder]} (get window-coders window-id)]
      (some-> (.get db ^bytes (encode-key encoder window-id group-id extent))
              (deserialize-fn))))
  (delete-extent! [this window-id group-id extent]
    (let [{:keys [encoder]} (get window-coders window-id)]
      (.delete db ^bytes (encode-key encoder window-id group-id extent))))
  (put-trigger! [this trigger-id group-id v]
    (let [enc (:encoder (get trigger-coders trigger-id))]
      (.put db 
            ^bytes (encode-key enc trigger-id group-id nil)
            ^bytes (serialize-fn v))))
  (get-trigger [this trigger-id group-id]
    (if-let [enc (:encoder (get trigger-coders trigger-id))] 
      (if-let [value (.get db ^bytes (encode-key enc trigger-id group-id nil))]
        (deserialize-fn value)
        :not-found)
      :not-found))
  (get-group-id [this group-key]
    (let [group-bytes ^bytes (serialize-fn group-key)
          group-enc (doto (:encoder group-coder)
                      (genc/set-group group-bytes))
          group-key-bs (genc/get-bytes group-enc)]
      (.get db ^bytes group-key-bs)))
  (group-id [this group-key]
    (let [group-bytes ^bytes (serialize-fn group-key)
          group-enc (:encoder group-coder)
          _ (genc/set-group group-enc group-bytes)
          group-key-bs (genc/get-bytes group-enc)]
      (if-let [bs (.get db ^bytes group-key-bs)]
        bs
        (let [group-id (swap! group-counter inc)
              bs (genc/group-id->bytes group-id)
              group-reverse-enc (:encoder group-reverse-coder)]
          (grenc/set-group-id group-reverse-enc bs)
          (.put db ^bytes group-key-bs ^bytes bs)
          (.put db ^bytes (grenc/get-bytes group-reverse-enc) group-bytes)
          bs))))
  (group-key [this group-id]
    (when group-id
      (let [group-reverse-enc (:encoder group-reverse-coder)]
        (grenc/set-group-id group-reverse-enc group-id)
        (some-> (.get db ^bytes (grenc/get-bytes group-reverse-enc))
                (deserialize-fn)))))
  (groups [this]
    (let [{:keys [encoder decoder idx]} group-coder
          _ (genc/set-group encoder (byte-array 0))
          k (genc/get-bytes encoder) 
          txn (read-txn env)]
      (try 
       (let [iterator (.seek db txn k)
             vs (transient [])]
         (loop []
           (if (.hasNext iterator)
             (let [entry ^Entry (.next iterator)] 
               (gdec/wrap-impl decoder (.getKey entry))
               (when (= idx (gdec/get-state-idx decoder))
                 (let [group-bytes (gdec/get-group decoder)]
                   (conj! vs (list group-bytes (deserialize-fn group-bytes))))
                 (recur)))))
         (persistent! vs))
       (finally 
        (.abort txn)))))
  (trigger-keys [this trigger-idx]
    (when-let [decoder (:decoder (get trigger-coders trigger-idx))]
      (let [vs (transient [])
            txn (read-txn env)]
        (try 
         (let [iterator (.iterate db txn)] 
           (loop []
             (if (.hasNext iterator)
               (let [entry ^Entry (.next iterator)]
                 (when (= trigger-idx (get-state-idx (.getKey entry)))
                   (dec/wrap-impl decoder (.getKey entry))  
                   (let [group-id (dec/get-group-id decoder)]
                     (conj! vs group-id)))
                 (recur)))))
         (finally 
          (.abort txn)))
        (mapv (fn [group-id]
                [group-id (some->> group-id (db/group-key this))]) 
              (persistent! vs)))))
  (group-extents [this window-idx group-id]
     (let [enc (:encoder (get window-coders window-idx))
           _ (enc/set-group-id enc group-id)
           _ (enc/set-min-extent enc)
           k (enc/get-bytes enc) 
           txn (read-txn env)]
       (try 
        (let [iterator (.seek db txn k)
              decoder (:decoder (get window-coders window-idx))
              vs (transient [])] 
          (loop []
            (if (.hasNext iterator)
              (let [entry ^Entry (.next iterator)]
                (dec/wrap-impl decoder (.getKey entry))
                (when (and (= window-idx (dec/get-state-idx decoder))
                           (or (nil? group-id)
                               (u/equals (dec/get-group-id decoder) group-id)))
                  ;; TODO, check whether less than the extent for the watermark.
                  (conj! vs (dec/get-extent decoder))
                  (recur)))))
          (persistent! vs))
        (finally 
         (.abort txn)))))
  (drop! [this]
    (.drop db true)
    (let [dir (java.io.File. path)] 
      (run! (fn [s]
              (.delete (java.io.File. path (str s)))) 
            (.list dir))
      (.delete dir)))
  (close! [this]
    (.close env)
    (.close db))
  (export-reader [this]
    nil)
  (export [this state-encoder]
    (let [txn (read-txn env)]
      (try 
       (cp/set-next-bytes state-encoder 
                          (serialize-fn {:group-counter @group-counter
                                         :entry-counter @entry-counter}))
       (->> txn
            (items db)
            (run! (fn [^Entry entry]
                    (cp/set-next-bytes state-encoder (.getKey entry))
                    (cp/set-next-bytes state-encoder (.getValue entry))))) 
       (finally
        (.abort txn)))))
  (restore! [this state-decoder mapping]
    (when-not (db-empty? db env)
      (throw (Exception. "LMDB DB is not empty. This should never happen.")))
    (let [counters (deserialize-fn (cp/get-next-bytes state-decoder))] 
      (reset! group-counter (:group-counter counters))
      (reset! entry-counter (:entry-counter counters))
      (loop []
        (let [k ^bytes (cp/get-next-bytes state-decoder)
              v ^bytes (cp/get-next-bytes state-decoder)]
          (when k
            (assert v)
            (let [serialized-state-index (get-state-idx k)] 
              ;; re-index state index
              (when-let [new-idx (mapping serialized-state-index)]
                (set-state-idx k new-idx)
                (.put db k v)))
            (recur)))))))

(defmethod db/open-db-reader :lmdb
  [peer-config 
   definition
   {:keys [window-coders trigger-coders group-coder]}]
  ;; Not implemented yet.
  nil)

(defmethod db/create-db 
  :lmdb
  [peer-config 
   db-name 
   {:keys [group-coder group-reverse-coder window-coders trigger-coders]}]
  (let [max-size 102400000000
        path (str (System/getProperty "java.io.tmpdir") "/onyx/" (java.util.UUID/randomUUID) "/")
        _ (.mkdirs (java.io.File. path))
        env (doto (Env. path)
              (.addFlags (reduce bit-or [Constants/NOSYNC Constants/MAPASYNC Constants/NOMETASYNC]))
              (.setMapSize max-size))
        db (.openDatabase env db-name)]
    (->StateBackend (atom {})
                    (atom (long -1))
                    (atom (long -1))
                    path
                    db name env 
                    statedb-compress 
                    statedb-decompress 
                    group-coder
                    group-reverse-coder
                    window-coders 
                    trigger-coders)))
