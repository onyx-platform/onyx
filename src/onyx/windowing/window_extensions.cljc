(ns onyx.windowing.window-extensions
  (:require [onyx.windowing.units :refer [to-standard-units coerce-key] :as units]
            [onyx.windowing.window-id :as wid]
            [onyx.static.default-vals :as d]))

(defn window-id-impl-extents [range min-value window-key w-range w-slide segment]
  (let [units (units/standard-units-for (last range))
        min-value (or min-value 0)]
    (wid/wids min-value w-range w-slide window-key segment)))

(defprotocol IWindow
  (speculate-update [this extents segment]
    "Speculatively update the window extents in reaction to seeing
     segment. This is used to apply changes ahead of time that
     take place due to stateful extent changes. Must return all
     the extents.")

  (extents [this all-extents segment]
    "Given a segment, return the window identifier extents
     that it belongs to.")

  (merge-extents [this extents super-agg-fn segment]
    "Takes all the existing extents with their associated state.
     Returns all the extents, with any extents and state fused,
     deleted, or otherwise rearranged for this segment's session key.")

  (uniform-units [this segment]
    "Given a segment, return an updated segment with any needed changes in units.")

  (bounds [this window-id]
    "Returns a vector of two elements. The first is the lower bound that this window
     id accepts, and the second is the upper."))

(defrecord FixedWindow 
  [id task type aggregation init window-key min-value range slide timeout-gap session-key doc window]
  IWindow
  (speculate-update [this extents segment]
    extents)

  (extents [this all-extents segment]
    (let [w-range (apply units/to-standard-units range)]
      (window-id-impl-extents range min-value window-key w-range w-range segment)))

  (merge-extents [this extents super-agg-fn segment]
    extents)

  (uniform-units [this segment]
    (let [units (units/standard-units-for (last range))]
      (update segment window-key units/coerce-key units)))

  (bounds [this window-id]
    (let [win-min (or min-value (get d/default-vals :onyx.windowing/min-value))
          w-range (apply to-standard-units range)]
      [(wid/extent-lower win-min w-range w-range window-id)
       (wid/extent-upper win-min w-range window-id)])))

(defrecord SlidingWindow 
  [id task type aggregation init window-key min-value range slide timeout-gap session-key doc window]
  IWindow
  (speculate-update [this extents segment]
    extents)

  (extents [this all-extents segment]
    (let [w-range (apply units/to-standard-units range)
          w-slide (apply units/to-standard-units slide)]
      (window-id-impl-extents range min-value window-key w-range w-slide segment)))

  (merge-extents [this extents super-agg-fn segment]
    extents)

  (uniform-units [this segment]
    (let [units (units/standard-units-for (last range))]
      (update segment window-key units/coerce-key units)))

  (bounds [this window-id]
    (let [win-min (or min-value (get d/default-vals :onyx.windowing/min-value))
          w-range (apply to-standard-units range)
          w-slide (apply to-standard-units (or slide range))]
      [(wid/extent-lower win-min w-range w-slide window-id)
       (wid/extent-upper win-min w-slide window-id)])))

(defrecord GlobalWindow 
  [id task type aggregation init window-key min-value range slide timeout-gap session-key doc window]
  IWindow
  (speculate-update [this extents segment]
    extents)

  (extents [this all-extents segment]
    ;; Always return the same window ID, the actual number
    ;; doesn't matter - as long as its constant.
    [1])

  (merge-extents [this extents super-agg-fn segment]
    extents)

  (uniform-units [this segment]
    segment)

  (bounds [this window-id]
    ;; Everything is in bounds.
    #?(:clj  [Double/NEGATIVE_INFINITY Double/POSITIVE_INFINITY])
    #?(:cljs [(.-NEGATIVE_INFINITY js/Number) (.-POSITIVE_INFINITY js/Number)])))

(defn super-aggregate [window super-agg-fn extents extent-1 all-extents]
  (reduce
   (fn [all e]
     (super-agg-fn window all (get extents e)))
   (get extents extent-1) all-extents))

(defrecord SessionWindow 
  [id task type aggregation init window-key min-value range slide timeout-gap session-key doc window]
  IWindow
  (speculate-update [this extents segment]
    (let [gap (apply units/to-standard-units timeout-gap)
          t (get segment window-key)]
      (reduce-kv
       (fn [all s v]
         (if (and (= (:session-key s) (get segment session-key))
                  (>= t (- (:session-lower-bound s) gap))
                  (<= t (+ (:session-upper-bound s) gap)))
           (if (>= (get segment window-key) (:session-upper-bound s))
             (assoc all (assoc s :session-upper-bound t) v)
             (assoc all (assoc s :session-lower-bound t) v))
           (assoc all s v)))
       {}
       extents)))

  (extents [this all-extents segment]
    (let [sessions (filter
                    (fn [s]
                      (and (= (:session-key s) (get segment session-key))
                           (>= (get segment window-key) (:session-lower-bound s))
                           (<= (get segment window-key) (:session-upper-bound s))))
                    all-extents)]
      (if (empty? sessions)
        [{:session-key (get segment session-key)
          :session-lower-bound (get segment window-key)
          :session-upper-bound (get segment window-key)}]
        sessions)))

  (merge-extents [this extents super-agg-fn segment]
    (let [session-key (get segment session-key)]
      (loop [ks (sort-by :session-lower-bound (filter #(= (:session-key %) session-key) (keys extents)))
             results (into {} (filter (fn [[e v]] (not= (:session-key e) session-key)) extents))]
        (if-not (seq ks)
          results
          (let [matches (take-while (fn [x]
                                      (>= (:session-upper-bound (first ks))
                                          (:session-lower-bound x)))
                                    (rest ks))]
            (if (seq matches)
              (let [merged (super-aggregate window super-agg-fn extents (first ks) matches)
                    new-key {:session-key (:session-key (first ks))
                             :session-lower-bound (:session-lower-bound (first ks))
                             :session-upper-bound (apply max (map :session-upper-bound matches))}]
                ;; Insert it back at the head of the sequence to try
                ;; and match further up the chain
                (recur (conj (drop (count matches) (reverse (into (list) (rest ks)))) new-key) (assoc results new-key merged)))
              (recur (rest ks) (assoc results (first ks) (or (get results (first ks)) (get extents (first ks)))))))))))

  (uniform-units [this segment]
    (let [units (units/standard-units-for (last timeout-gap))]
      (update segment window-key units/coerce-key units)))

  (bounds [this window-id]
    [(:session-lower-bound window-id)
     (:session-upper-bound window-id)]))

(defmulti windowing-builder
  "Given a window, return the concrete type to perform
   operations against."
  (fn [window]
    (:window/type window)))

(defmethod windowing-builder :fixed
  [window] map->FixedWindow)

(defmethod windowing-builder :sliding
  [window] map->SlidingWindow)

(defmethod windowing-builder :global
  [window] map->GlobalWindow)

(defmethod windowing-builder :session
  [window] map->SessionWindow)
