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
   (defn export-triggers [triggers trigger-encoders state-encoder serialize-fn]
     (run! (fn [[idx group-extents]]
             (let [enc (get trigger-encoders idx)] 
               (run! (fn [[group v]]
                       (cp/set-next-bytes state-encoder (encode-key enc idx (serialize-fn group) nil))
                       (cp/set-next-bytes state-encoder (serialize-fn v)))
                     group-extents)))
           @triggers)))

#?(:clj 
   (defn export-windows [windows window-encoders state-encoder serialize-fn]
     (run! (fn [[idx group-extents]]
             (let [enc (get window-encoders idx)] 
               (run! (fn [[group extents-values]]
                       (run! (fn [[extent value]]
                               (cp/set-next-bytes state-encoder (encode-key enc idx (serialize-fn group) extent))
                               (cp/set-next-bytes state-encoder (serialize-fn value)))
                             extents-values))
                     group-extents)))
           @windows)))

(defn get-state-idx [^bytes bs]
  ;; remove extra allocation
  #?(:clj (.getShort (UnsafeBuffer. bs) 0)))

(deftype StateBackend [windows triggers serialize-fn deserialize-fn 
                       window-encoders window-decoders trigger-encoders trigger-decoders]
  db/State
  (put-extent! [this window-id group extent v]
    (swap! windows 
           update window-id 
           update group 
           assoc extent v))
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
    (get-in @triggers [trigger-id group]))
  (trigger-keys [this]
    (let [trigger-ks (transient [])] 
      (run! (fn [[trigger-id group-trigger-values]]
              (run! (fn [[group v]]
                      (conj! trigger-ks (list trigger-id group group)))
                    group-trigger-values))
            @triggers)
      (persistent! trigger-ks)))
  (group-id [this group-key]
    group-key)
  (groups [this window-id]
    (into (set (keys (get @windows window-id)))
          (set (keys (get @triggers window-id)))))
  (group-extents [this window-id group]
    (sort (keys (get (get @windows window-id) group))))
  (drop! [this]
    (reset! windows {})
    (reset! triggers {}))
  (close! [this])
  (export-reader [this]
    [windows triggers])
  #?(:clj 
      (export [this state-encoder]
              (export-triggers triggers trigger-encoders state-encoder serialize-fn)
              (export-windows windows window-encoders state-encoder serialize-fn)))
  #?(:clj 
      (restore! [this state-decoder mapping]
                (loop []
                  (let [k ^bytes (cp/get-next-bytes state-decoder)
                        v ^bytes (cp/get-next-bytes state-decoder)]
                    (when k
                      (assert v)
                      ;; if mapping is not found then we should just ignore the window/trigger
                      ;; as this extent/trigger is not being restored
                      (if-let [idx (mapping (get-state-idx k))] 
                        (let [value (deserialize-fn v)
                              window-decoder (get window-decoders idx)
                              trigger-decoder (get trigger-decoders idx)]
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
   {:keys [window-encoders 
           window-decoders 
           trigger-encoders 
           trigger-decoders]}]
  (->StateBackend (atom {}) (atom {}) 
                  statedb-compress statedb-decompress
                  window-encoders window-decoders
                  trigger-encoders trigger-decoders))

(defmethod db/open-db-reader :memory
  [peer-config 
   [windows triggers]
   {:keys [window-encoders 
           window-decoders 
           trigger-encoders 
           trigger-decoders]}]
  (->StateBackend windows triggers 
                  statedb-compress statedb-decompress
                  window-encoders window-decoders 
                  trigger-encoders trigger-decoders))
