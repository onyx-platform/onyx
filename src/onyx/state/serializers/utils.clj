(ns onyx.state.serializers.utils
  (:require [onyx.state.serializers.windowing-key-encoder :as enc]
            [onyx.state.serializers.windowing-key-decoder :as dec]
            [onyx.windowing.window-extensions :as wext]
            [onyx.peer.window-state :as ws]
            [onyx.peer.grouping :as g])
   (:import [org.agrona.concurrent UnsafeBuffer]))

(deftype Comparator [^UnsafeBuffer buf1 decoder1 ^UnsafeBuffer buf2 decoder2]
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

(defn get-trigger-coder [{:keys [grouped?] :as w} encoder-buf decoder-buf]
  (if grouped?
    {:encoder (enc/grouped-trigger encoder-buf 0)
     :decoder (dec/grouped-trigger decoder-buf 0)}
    {:encoder (enc/ungrouped-trigger encoder-buf 0)
     :decoder (dec/ungrouped-trigger decoder-buf 0)}))

(defn get-window-coder [{:keys [grouped? extent] :as w} encoder-buf decoder-buf]
  (cond (and grouped? (= extent :long))
        {:encoder (enc/grouped-long-extent encoder-buf 0)
         :decoder (dec/grouped-long-extent decoder-buf 0)}
        (and grouped? (= extent :long-long))
        {:encoder (enc/grouped-long-long-extent encoder-buf 0)
         :decoder (dec/grouped-long-long-extent decoder-buf 0)}
        (and grouped? (= extent :nil))
        {:encoder (enc/grouped-no-extent encoder-buf 0)
         :decoder (dec/grouped-no-extent decoder-buf 0)}
        (and (not grouped?) (= extent :long))
        {:encoder (enc/ungrouped-long-extent encoder-buf 0)
         :decoder (dec/ungrouped-long-extent decoder-buf 0)}
        (and (not grouped?) (= extent :long-long))
        {:encoder (enc/ungrouped-long-long-extent encoder-buf 0)
         :decoder (dec/ungrouped-long-long-extent decoder-buf 0)}
        (and (not grouped?) (= extent :nil))
        {:encoder (enc/ungrouped-no-extent encoder-buf 0)
         :decoder (dec/ungrouped-no-extent decoder-buf 0)}

        :else
        (throw (ex-info "No serializer has been implemented for window" w))))

(def max-window-key-size-bytes 1000)

(defn build-coders [window-definitions trigger-definitions]
  (let [key-enc-bs (byte-array max-window-key-size-bytes)
        encoder-buf (UnsafeBuffer. key-enc-bs)
        decoder-buf (UnsafeBuffer. (byte-array 0))
        window-coders (map (fn [w] 
                             (merge w (get-window-coder w encoder-buf decoder-buf))) 
                           window-definitions)
        trigger-coders (map (fn [t] 
                              (merge t (get-trigger-coder t encoder-buf decoder-buf))) 
                            trigger-definitions)
        window-encoders (into {} (map (juxt :idx :encoder)) window-coders)
        window-decoders (into {} (map (juxt :idx :decoder)) window-coders)
        trigger-encoders (into {} (map (juxt :idx :encoder)) trigger-coders)
        trigger-decoders (into {} (map (juxt :idx :decoder)) trigger-coders)]
    {:window-encoders window-encoders
     :window-decoders window-decoders
     :trigger-encoders trigger-encoders
     :trigger-decoders trigger-decoders}))

(defn event->state-serializers [{:keys [onyx.core/task-map onyx.core/windows 
                                        onyx.core/triggers onyx.core/job-id] :as event}]
  (if (and (empty? windows) (empty? triggers)) 
    {}
    (let [grouped? (g/grouped-task? task-map)
          state-indices (ws/state-indices event)
          window-definitions (map (fn [{:keys [window/id] :as w}]
                                    {:extent (wext/extent-serializer w)
                                     :grouped? grouped?
                                     :idx (get state-indices id)})
                                  windows)
          trigger-definitions (map (fn [trigger]
                                     {:grouped? grouped?
                                      :idx (get state-indices (ws/trigger-state-index-id trigger))})
                                   triggers)]
      (build-coders window-definitions trigger-definitions))))
