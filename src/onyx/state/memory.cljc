(ns onyx.state.memory
  (:require [onyx.state.protocol.db :as db]
            #?(:clj [onyx.state.serializers.group-reverse-encoder :as grenc])
            #?(:clj [onyx.state.serializers.group-reverse-decoder :as grdec])
            #?(:clj [onyx.state.serializers.group-encoder :as genc])
            #?(:clj [onyx.state.serializers.group-decoder :as gdec])
            #?(:clj [onyx.state.serializers.state-entry-key-encoder :as senc])
            #?(:clj [onyx.state.serializers.state-entry-key-decoder :as sdec])
            #?(:clj [onyx.state.serializers.checkpoint :as cp])
            #?(:clj [onyx.state.serializers.windowing-key-encoder :as enc :refer [encode-key]])
            #?(:clj [onyx.state.serializers.windowing-key-decoder :as dec])
            #?(:clj [onyx.compression.nippy :refer [statedb-compress statedb-decompress]]))
  #?(:clj (:import [org.agrona.concurrent UnsafeBuffer])))

#?(:cljs (def statedb-compress identity))
#?(:cljs (def statedb-decompress identity))

(defn clean-state [wstate group-id]
  (let [extents (get wstate group-id)] 
    (if (empty? extents)
      ;; TODO, remove group-id from group map when group no longer has extents
      (dissoc wstate group-id)
      wstate)))

#?(:clj 
   (defn export-triggers [triggers trigger-coders state-encoder serialize-fn]
     (run! (fn [[idx group-extents]]
             (let [enc (:encoder (get trigger-coders idx))] 
               (run! (fn [[group-id v]]
                       (cp/set-next-bytes state-encoder (encode-key enc idx (some-> group-id genc/group-id->bytes) nil))
                       (cp/set-next-bytes state-encoder (serialize-fn v)))
                     group-extents)))
           @triggers)))

#?(:clj 
   (defn export-windows [windows window-coders state-encoder serialize-fn]
     (run! (fn [[idx group-extents]]
             (let [enc (:encoder (get window-coders idx))] 
               (run! (fn [[group-id extents-values]]
                       (run! (fn [[extent value]]
                               (cp/set-next-bytes state-encoder (encode-key enc idx (some-> group-id (genc/group-id->bytes)) extent))
                               (cp/set-next-bytes state-encoder (serialize-fn value)))
                             extents-values))
                     group-extents)))
           @windows)))

#?(:clj 
   (defn export-state-entries [items window-coders state-encoder serialize-fn]
     (run! (fn [[idx group-items]]
             (let [enc (:entry-encoder (get window-coders idx))] 
               (run! (fn [[group-id time-values]]
                       (run! (fn [[time offset value]]
                               (some->> group-id 
                                        genc/group-id->bytes 
                                        (senc/set-group enc))
                               (senc/set-time enc time)
                               (senc/set-offset enc offset)

                               (cp/set-next-bytes state-encoder (senc/get-bytes enc))
                               (cp/set-next-bytes state-encoder (serialize-fn value)))
                             time-values))
                     group-items)))
           @items)))

#?(:clj 
   (defn export-groups [groups group-coder group-reverse-coder state-encoder serialize-fn]
     (let [genc (:encoder group-coder)
           grenc (:encoder group-reverse-coder)] 
       (run! (fn [[group group-id]]
               (let [group-bytes (serialize-fn group)
                     group-id-bytes (genc/group-id->bytes group-id)]

                 ;; write group -> group-id kv
                 (genc/set-group genc group-bytes)
                 (cp/set-next-bytes state-encoder (genc/get-bytes genc))
                 (cp/set-next-bytes state-encoder group-id-bytes)

                 ;; write group-id -> group kv
                 (assert (= 10 (count (grenc/get-bytes grenc))))
                 (grenc/set-group-id grenc group-id-bytes)
                 (cp/set-next-bytes state-encoder (grenc/get-bytes grenc))
                 (cp/set-next-bytes state-encoder group-bytes)))
             @groups))))

(defn get-state-idx [^bytes bs]
  #?(:clj (.getShort (UnsafeBuffer. bs) 0)))

(defn put-state-entry-offset! [items window-id group-id time offset v]
  (swap! items 
         update-in [window-id group-id]
         (fn [coll] (conj (or coll []) [time offset v]))))

(deftype StateBackend [windows triggers items groups groups-reverse group-counter entry-counter 
                       serialize-fn deserialize-fn group-coder group-reverse-coder window-coders trigger-coders]
  db/State
  (put-extent! [this window-id group-id extent v]
    (swap! windows 
           update window-id 
           update group-id 
           assoc extent v))
  (put-state-entry! [this window-id group-id time v]
    (put-state-entry-offset! items window-id group-id time (swap! entry-counter inc) v))
  (delete-state-entries! [this window-id group-id start end]
    (swap! items 
           update-in [window-id group-id]
           (fn [values]
             (doall
              (remove (fn [[time]]
                        (and (>= time start)
                             (<= time end))) 
                      values)))))
  (get-state-entries-times [this window-id group-id]
    (distinct (sort (map first (get-in @items [window-id group-id])))))
  (get-state-entries [this window-id group-id start end]
    (map (fn [[_ _ v]] v) 
         (sort-by (juxt first second) 
                  (filter (fn [[time]]
                            (and (>= time start)
                                 (<= time end))) 
                          (get-in @items [window-id group-id])))))
  (get-extent [this window-id group-id extent]
    (-> (get @windows window-id)
        (get group-id)
        (get extent)))
  (delete-extent! [this window-id group-id extent]
    (swap! windows 
           (fn [window-state] 
             (-> window-state 
                 (update window-id (fn [w] 
                                     (-> w 
                                         (update group-id dissoc extent)
                                         (clean-state group-id))))))))
  (put-trigger! [this trigger-id group-id v]
    (swap! triggers assoc-in [trigger-id group-id] v))
  (get-trigger [this trigger-id group-id]
    (get-in @triggers [trigger-id group-id] :not-found))
  (trigger-keys [this trigger-idx]
    (when-let [trigger (get @triggers trigger-idx)] 
      (let [trigger-ks (transient [])] 
        (run! (fn [[group-id v]]
                (conj! trigger-ks (list group-id (get @groups-reverse group-id))))
              trigger)
        (persistent! trigger-ks))))
  (get-group-id [this group-key]
    (get @groups group-key))
  (group-id [this group-key]
    (if-let [group-id (db/get-group-id this group-key)]
      group-id
      (let [group-id (swap! group-counter inc)]
        (swap! groups assoc group-key group-id)
        (swap! groups-reverse assoc group-id group-key)
        group-id)))
  (groups [this]
    (map (fn [[group-key group-id]]
           (list group-id group-key)) 
         @groups))
  (group-extents [this window-id group-id]
    (sort (keys (get (get @windows window-id) group-id))))
  (drop! [this]
    (reset! windows {})
    (reset! triggers {}))
  (close! [this])
  (export-reader [this] {:windows windows :triggers triggers :groups groups :items items :groups-reverse groups-reverse})
#?(:clj 
  (export [this state-encoder]
          (cp/set-next-bytes state-encoder 
                             (serialize-fn {:group-counter @group-counter
                                            :entry-counter @entry-counter}))
          (export-groups groups group-coder group-reverse-coder state-encoder serialize-fn)
          (export-state-entries items window-coders state-encoder serialize-fn)
          (export-triggers triggers trigger-coders state-encoder serialize-fn)
          (export-windows windows window-coders state-encoder serialize-fn)))
  #?(:clj 
   (restore! [this state-decoder mapping]
     (let [counters (deserialize-fn (cp/get-next-bytes state-decoder))] 
       (reset! group-counter (:group-counter counters))
       (reset! entry-counter (:entry-counter counters)))
     (loop []
       (let [k ^bytes (cp/get-next-bytes state-decoder)
             v ^bytes (cp/get-next-bytes state-decoder)]
         (when k
           (assert v)
           ;; if mapping is not found then we just ignore the window/trigger
           ;; as this extent/trigger is not being restored
           (if-let [idx (mapping (get-state-idx k))] 
             (let [entry-decoders (into {} (map (juxt :entry-idx :entry-decoder) (vals window-coders)))
                   group-decoder {(:idx group-coder) (:decoder group-coder)}
                   group-reverse-decoder {(:idx group-reverse-coder) (:decoder group-reverse-coder)}]
               (if-let [d (:decoder (get window-coders idx))]
                 (do (dec/wrap-impl d k)
                     (db/put-extent! this 
                                     idx 
                                     (some-> (dec/get-group-id d) gdec/get-group-id) 
                                     (dec/get-extent d) 
                                     (deserialize-fn v)))
                 (if-let [d (:decoder (get trigger-coders idx))]
                   (do 
                    (dec/wrap-impl d k)
                    (db/put-trigger! this idx (some-> (dec/get-group-id d) gdec/get-group-id) (deserialize-fn v)))
                   (if-let [d (get entry-decoders idx)]
                     (do
                      (sdec/wrap-impl d k)
                      (put-state-entry-offset! items 
                                               ;; FIXME window-idx is in memory is currently shared with regular extents
                                               ;; even though it's serialized differently.
                                               (dec idx) 
                                               (some-> (sdec/get-group-id d) gdec/get-group-id)
                                               (sdec/get-time d) 
                                               (sdec/get-offset d) 
                                               (deserialize-fn v)))
                     (if-let [d (group-decoder idx)]
                       (do
                        (gdec/wrap-impl d k)
                        (swap! groups assoc (deserialize-fn (gdec/get-group d)) (gdec/get-group-id v)))
                       (if-let [d (group-reverse-decoder idx)]
                         (do
                          (grdec/wrap-impl d k)
                          (swap! groups-reverse assoc (grdec/get-group-id d) (deserialize-fn v)))
                         (throw (ex-info "Trigger or window decoder not found." {:idx idx})))))))))
           (recur)))))))

(defmethod db/create-db :memory
  [peer-config 
   _
   {:keys [window-coders trigger-coders group-coder group-reverse-coder]}]
  (->StateBackend (atom {}) (atom {}) (atom {}) (atom {}) (atom {})
                  (atom (long -1)) (atom (long -1)) 
                  statedb-compress statedb-decompress
                  group-coder group-reverse-coder
                  window-coders trigger-coders))

(defmethod db/open-db-reader :memory
  [peer-config 
   {:keys [windows triggers groups groups-reverse items]} 
   {:keys [window-coders trigger-coders group-coder group-reverse-coder]}]
  (->StateBackend windows triggers items groups groups-reverse nil nil
                  statedb-compress statedb-decompress 
                  group-coder group-reverse-coder
                  window-coders trigger-coders))
