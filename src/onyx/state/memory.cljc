(ns onyx.state.memory
  (:require [onyx.state.protocol.db :as db]
            #?(:clj [onyx.state.serializers.checkpoint :as cp])
            #?(:clj [onyx.state.serializers.windowing-key-encoder :as enc :refer [encode-key]])
            #?(:clj [onyx.state.serializers.windowing-key-decoder :as dec])
            #?(:clj [onyx.compression.nippy :refer [statedb-compress statedb-decompress]]))
  #?(:clj (:import [org.agrona.concurrent UnsafeBuffer])))

#?(:cljs (def statedb-compress identity))
#?(:cljs (def statedb-decompress identity))

(defn clean-groups [groups]
  (->> groups
       (remove (fn [[_ extents]]
                 (empty? extents)))
       (into {})))

;; Slow, but needed for equiv implementation to db store
(defn clean-state [state]
  (->> state
      (map (fn [[k groups]]
             [k (clean-groups groups)]))
      (remove (fn [[_ groups]]
                (empty? groups)))
      (into {})))

#?(:clj 
   (defn export-triggers [triggers trigger-coders state-encoder serialize-fn]
     (run! (fn [[idx group-extents]]
             (let [enc (:encoder (get trigger-coders idx))] 
               (run! (fn [[group v]]
                       (cp/set-next-bytes state-encoder (encode-key enc idx (serialize-fn group) nil))
                       (cp/set-next-bytes state-encoder (serialize-fn v)))
                     group-extents)))
           @triggers)))

#?(:clj 
   (defn export-windows [windows window-coders state-encoder serialize-fn]
     (run! (fn [[idx group-extents]]
             (let [enc (:encoder (get window-coders idx))] 
               (run! (fn [[group extents-values]]
                       (run! (fn [[extent value]]
                               (cp/set-next-bytes state-encoder (encode-key enc idx (serialize-fn group) extent))
                               (cp/set-next-bytes state-encoder (serialize-fn value)))
                             extents-values))
                     group-extents)))
           @windows)))

(defn get-state-idx [^bytes bs]
  #?(:clj (.getShort (UnsafeBuffer. bs) 0)))

(deftype StateBackend [windows triggers items offset serialize-fn deserialize-fn 
                       window-coders trigger-coders]
  db/State
  (put-extent! [this window-id group extent v]
    (swap! windows 
           update window-id 
           update group 
           assoc extent v))
  ;; rename put time entry
  (put-state-entry! [this window-id group time v]
    (swap! items 
           update-in [window-id group]
           (fn [coll] (conj (or coll []) [time (swap! offset inc) v]))))
  (delete-state-entries! [this window-id group start end]
    (swap! items 
           update-in [window-id group]
           (fn [values]
             (doall
              (remove (fn [[time]]
                        (and (>= time start)
                             (<= time end))) 
                      values)))))
  (get-state-entries [this window-id group start end]
    (map (fn [[_ _ v]] v) 
         (sort-by (juxt first second) 
                  (filter (fn [[time]]
                            (and (>= time start)
                                 (<= time end))) 
                          (get-in @items [window-id group])))))
  (get-extent [this window-id group extent]
    (-> (get @windows window-id)
        (get group)
        (get extent)))
  (delete-extent! [this window-id group extent]
    (swap! windows 
           (fn [window-state] 
             (-> window-state 
                 (update window-id update group dissoc extent)
                 clean-state))))
  (put-trigger! [this trigger-id group v]
    (swap! triggers assoc-in [trigger-id group] v))
  (get-trigger [this trigger-id group]
    (get-in @triggers [trigger-id group] :not-found))
  (trigger-keys [this trigger-idx]
    (when-let [trigger (get @triggers trigger-idx)] 
      (let [trigger-ks (transient [])] 
        (run! (fn [[group v]]
                (conj! trigger-ks (list group group)))
              trigger)
        (persistent! trigger-ks))))
  (group-id [this group-key]
    group-key)
  (groups [this]
    (set (mapcat keys (vals @windows)))
    (println "GROUPS" (set (mapcat keys (vals @windows))) @windows)
    #_(into (set (mapcat keys (vals @windows)))
          (set (mapcat keys (vals @triggers)))))
  (group-extents [this window-id group]
    (sort (keys (get (get @windows window-id) group))))
  (drop! [this]
    (reset! windows {})
    (reset! triggers {}))
  (close! [this])
  (export-reader [this] [windows triggers])
  #?(:clj 
  (export [this state-encoder]
          #_(cp/set-next-bytes state-encoder (serialize-fn @items))
          #_(export-triggers triggers trigger-coders state-encoder serialize-fn)
          #_(export-windows windows window-coders state-encoder serialize-fn)))
  #?(:clj 
   (restore! [this state-decoder mapping]
          #_(reset! items (deserialize-fn (cp/get-next-bytes state-decoder)))
             #_(loop []
               (let [k ^bytes (cp/get-next-bytes state-decoder)
                     v ^bytes (cp/get-next-bytes state-decoder)]
                 (when k
                   (assert v)
                   ;; if mapping is not found then we should just ignore the window/trigger
                   ;; as this extent/trigger is not being restored
                   (if-let [idx (mapping (get-state-idx k))] 
                     (let [value (deserialize-fn v)
                           window-decoder (:decoder (get window-coders idx))
                           trigger-decoder (:decoder (get trigger-coders idx))]
                       (cond window-decoder
                             (let [_ (dec/wrap-impl window-decoder k)
                                   group (some-> window-decoder dec/get-group deserialize-fn)
                                   extent (dec/get-extent window-decoder)]
                               (db/put-extent! this idx group extent value))

                             trigger-decoder
                             (let [_ (dec/wrap-impl trigger-decoder k)
                                   group (some-> trigger-decoder dec/get-group deserialize-fn)]
                               (db/put-trigger! this idx group value))

                             :else
                             (throw (ex-info "Trigger or window decoder not found." {})))))
                   (recur)))))))

(defmethod db/create-db :memory
  [peer-config 
   _
   {:keys [window-coders trigger-coders]}]
  (->StateBackend (atom {}) (atom {}) (atom {}) (atom -1)
                  statedb-compress statedb-decompress
                  window-coders trigger-coders))

(defmethod db/open-db-reader :memory
  [peer-config 
   [windows triggers]
   {:keys [window-coders trigger-coders]}]
  (->StateBackend windows triggers nil nil
                  statedb-compress statedb-decompress
                  window-coders trigger-coders))
