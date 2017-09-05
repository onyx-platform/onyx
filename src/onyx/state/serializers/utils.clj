(ns onyx.state.serializers.utils
  (:require [onyx.state.serializers.windowing-key-encoder :as enc]
            [onyx.state.serializers.windowing-key-decoder :as dec]
            [onyx.state.serializers.state-entry-key-encoder :as senc]
            [onyx.state.serializers.state-entry-key-decoder :as sdec]
            [onyx.state.serializers.group-encoder :as genc]
            [onyx.state.serializers.group-decoder :as gdec]
            [onyx.state.serializers.group-reverse-encoder :as grenc]
            [onyx.state.serializers.group-reverse-decoder :as grdec]
            [onyx.compression.nippy :refer [statedb-compress statedb-decompress]]
            [onyx.windowing.window-extensions :as wext]
            [onyx.peer.window-state :as ws]
            [onyx.peer.grouping :as g])
   (:import [org.agrona.concurrent UnsafeBuffer]))

#_(deftype Comparator [^UnsafeBuffer buf1 decoder1 ^UnsafeBuffer buf2 decoder2]
  java.util.Comparator
  (compare [this o1 o2]
    (.wrap buf1 ^bytes o1)
    (.wrap buf2 ^bytes o2)
    (let [i (- (dec/get-state-idx decoder2)
               (dec/get-state-idx decoder1))]
      (if (zero? i)
        (let [g (- (dec/get-group decoder2) 
                   (dec/get-group decoder1))]
          (if (zero? g)
            (- (dec/get-extent decoder2)
               (dec/get-extent decoder1))))))))

#_(deftype Comparator [^UnsafeBuffer buf1 decoder1 ^UnsafeBuffer buf2 decoder2]
  java.util.Comparator
  (compare [this o1 o2]
    (.wrap buf1 ^bytes o1)
    (.wrap buf2 ^bytes o2)
    (let [i (- (dec/get-state-idx decoder2)
               (dec/get-state-idx decoder1))]
      (if (zero? i)
        (let [g (- (dec/get-group decoder2) 
                   (dec/get-group decoder1))]
          (if (zero? g)
            (- (dec/get-extent decoder2)
               (dec/get-extent decoder1))
            g)) 
        i))))

(defn equals [^bytes bs1 ^bytes bs2]
  (java.util.Arrays/equals bs1 bs2))

(def max-window-key-size-bytes 1000)

(defn get-trigger-coder [{:keys [grouped?] :as w}]
  (let [key-enc-bs (byte-array max-window-key-size-bytes)
        encoder-buf (UnsafeBuffer. key-enc-bs)
        decoder-buf (UnsafeBuffer. (byte-array 0))] 
    (if grouped?
      {:encoder (enc/grouped-trigger encoder-buf 0)
       :decoder (dec/grouped-trigger decoder-buf 0)}
      {:encoder (enc/ungrouped-trigger encoder-buf 0)
       :decoder (dec/ungrouped-trigger decoder-buf 0)})))

(defn get-window-coder [{:keys [grouped? extent idx entry-idx] :as w}]
  (cond (and grouped? (= extent :long))
        {:grouped? grouped?
         :entry-decoder sdec/->GroupedEntryDecoder
         :entry-encoder senc/->GroupedEntryEncoder 
         :encoder enc/grouped-long-extent 
         :decoder dec/grouped-long-extent}

        (and grouped? (= extent :long-long))
        {:grouped? grouped?
         :entry-decoder sdec/->GroupedEntryDecoder
         :entry-encoder senc/->GroupedEntryEncoder
         :encoder enc/grouped-long-long-extent
         :decoder dec/grouped-long-long-extent}

        (and grouped? (= extent :nil))
        {:grouped? grouped?
         :entry-decoder sdec/->GroupedGlobalEntryDecoder
         :entry-encoder senc/->GroupedGlobalEntryEncoder
         :encoder enc/grouped-no-extent
         :decoder dec/grouped-no-extent}

        (and (not grouped?) (= extent :long))
        {:grouped? grouped?
         :entry-decoder sdec/->UngroupedEntryDecoder
         :entry-encoder senc/->UngroupedEntryEncoder
         :encoder enc/ungrouped-long-extent
         :decoder dec/ungrouped-long-extent}

        (and (not grouped?) (= extent :long-long))
        {:grouped? grouped?
         :entry-decoder sdec/->UngroupedEntryDecoder
         :entry-encoder senc/->UngroupedEntryEncoder
         :encoder enc/ungrouped-long-long-extent
         :decoder dec/ungrouped-long-long-extent}

        (and (not grouped?) (= extent :nil))
        {:grouped? grouped?
         :entry-decoder sdec/->UngroupedGlobalEntryDecoder
         :entry-encoder senc/->UngroupedGlobalEntryEncoder
         :encoder enc/ungrouped-no-extent
         :decoder dec/ungrouped-no-extent}

        :else
        (throw (ex-info "No serializer has been implemented for window" w))))

(defn instantiate [{:keys [idx entry-idx] :as w}]
  (-> w
      (update :entry-encoder (fn [e] (doto (e (UnsafeBuffer. (byte-array max-window-key-size-bytes)) 0)
                                       (senc/set-state-idx entry-idx))))
      (update :entry-decoder (fn [d] (d (UnsafeBuffer. (byte-array 0))  0)))
      (update :encoder (fn [e] (doto (e (UnsafeBuffer. (byte-array max-window-key-size-bytes)) 0)
                                 (enc/set-state-idx idx))))
      (update :decoder (fn [d] (d (UnsafeBuffer. (byte-array 0)) 0)))))

(defn build-coders [group-idx group-reverse-idx window-definitions trigger-definitions]
  (let [window-coders (map (fn [w] 
                             (instantiate (merge w (get-window-coder w)))) 
                           window-definitions)
        trigger-coders (map (fn [t] 
                              (merge t (get-trigger-coder t))) 
                            trigger-definitions)
        window-coders (into {} (map (juxt :idx identity)) window-coders)
        trigger-coders (into {} (map (juxt :idx identity)) trigger-coders)]
    {:group-coder {:idx group-idx
                   :encoder (let [buf (UnsafeBuffer. (byte-array 1000))]
                              (doto (genc/->GroupEncoder buf 0)
                                (genc/set-state-idx group-idx)))
                   :decoder (gdec/->GroupDecoder (UnsafeBuffer. (byte-array 0)) 0)}
     :group-reverse-coder {:idx group-reverse-idx
                           :encoder (let [buf (UnsafeBuffer. (byte-array 1000))]
                                      (doto (grenc/->GroupReverseEncoder buf 0)
                                        (grenc/set-state-idx group-reverse-idx)))
                           :decoder (grdec/->GroupReverseDecoder (UnsafeBuffer. (byte-array 0)) 0)}
     :window-coders window-coders
     :trigger-coders trigger-coders}))

(defn state-serializers [grouped? windows triggers]
  (if (and (empty? windows) (empty? triggers)) 
    {}
    (let [state-indices (ws/state-indices windows triggers)
          window-definitions (map (fn [{:keys [window/id] :as w}]
                                    {:extent (wext/extent-serializer w)
                                     :window w 
                                     :grouped? grouped?
                                     :entry-idx (get state-indices [id :state-entry])
                                     :idx (get state-indices id)})
                                  windows)
          trigger-definitions (map (fn [trigger]
                                     {:grouped? grouped?
                                      :trigger trigger
                                      :idx (get state-indices (ws/trigger-state-index-id trigger))})
                                   triggers)]
      (build-coders (get state-indices :group-idx)
                    (get state-indices :group-reverse-idx)
                    window-definitions
                    trigger-definitions))))

(defn event->state-serializers [{:keys [onyx.core/task-map onyx.core/windows onyx.core/triggers] :as event}]
  (state-serializers (g/grouped-task? task-map) windows triggers))
