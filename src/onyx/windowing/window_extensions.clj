(ns onyx.windowing.window-extensions
  (:require [onyx.windowing.units :refer [to-standard-units coerce-key] :as units]
            [onyx.windowing.window-id :as wid]
            [onyx.static.default-vals :as d]))

(defn window-id-impl-extents [window w-range w-slide segment]
  (let [window-id (:window/id window)
        units (units/standard-units-for (last (:window/range window)))
        min-value (or (:window/min-value window) 0)]
    (wid/wids min-value w-range w-slide (:window/window-key window) segment)))

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

(deftype FixedWindow [window]
  IWindow
  (speculate-update [this extents segment]
    extents)

  (extents [this all-extents segment]
    (let [w-range (apply units/to-standard-units (:window/range window))]
      (window-id-impl-extents window w-range w-range segment)))

  (merge-extents [this extents super-agg-fn segment]
    extents)

  (uniform-units [this segment]
    (let [units (units/standard-units-for (last (:window/range window)))
          k (:window/window-key window)]
      (update segment k units/coerce-key units)))

  (bounds [this window-id]
    (let [win-min (or (:window/min-value window) (get d/defaults :onyx.windowing/min-value))
          w-range (apply to-standard-units (:window/range window))]
      [(wid/extent-lower win-min w-range w-range window-id)
       (wid/extent-upper win-min w-range window-id)])))

(deftype SlidingWindow [window]
  IWindow
  (speculate-update [this extents segment]
    extents)

  (extents [this all-extents segment]
    (let [w-range (apply units/to-standard-units (:window/range window))
          w-slide (apply units/to-standard-units (:window/slide window))]
      (window-id-impl-extents window w-range w-slide segment)))

  (merge-extents [this extents super-agg-fn segment]
    extents)

  (uniform-units [this segment]
    (let [units (units/standard-units-for (last (:window/range window)))
          k (:window/window-key window)]
      (update segment k units/coerce-key units)))

  (bounds [this window-id]
    (let [win-min (or (:window/min-value window) (get d/defaults :onyx.windowing/min-value))
          w-range (apply to-standard-units (:window/range window))
          w-slide (apply to-standard-units (or (:window/slide window) (:window/range window)))]
      [(wid/extent-lower win-min w-range w-slide window-id)
       (wid/extent-upper win-min w-slide window-id)])))

(deftype GlobalWindow [window]
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
    [Double/NEGATIVE_INFINITY Double/POSITIVE_INFINITY]))

(defn super-aggregate [window super-agg-fn extents extent-1 all-extents]
  (reduce
   (fn [all e]
     (super-agg-fn all (get extents e) window))
   (get extents extent-1) all-extents))

(deftype SessionWindow [window]
  IWindow
  (speculate-update [this extents segment]
    (let [window-key (:window/window-key window)
          session-key (:window/session-key window)
          gap (apply units/to-standard-units (:window/timeout-gap window))]
      (reduce-kv
       (fn [all s v]
         (if (and (= (:session-key s) (get segment session-key))
                  (>= (get segment window-key) (- (:session-lower-bound s) gap))
                  (<= (get segment window-key) (+ (:session-upper-bound s) gap)))
           (if (>= (get segment window-key) (:session-upper-bound s))
             (assoc all (update s :session-upper-bound + gap) v)
             (assoc all (update s :session-lower-bound - gap) v))
           (assoc all s v)))
       {}
       extents)))

  (extents [this all-extents segment]
    (let [window-key (:window/window-key window)
          session-key (:window/session-key window)
          sessions (filter
                    (fn [s]
                      (and (= (:session-key s) (get segment session-key))
                           (>= (get segment window-key) (:session-lower-bound s))
                           (<= (get segment window-key) (:session-upper-bound s))))
                    all-extents)]
      (if (seq sessions)
        sessions
        [{:session-key (get segment session-key)
          :session-lower-bound (get segment window-key)
          :session-upper-bound (get segment window-key)}])))

  (merge-extents [this extents super-agg-fn segment]
    (let [session-key (get segment (:window/session-key window))]
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
              (recur (rest ks) (assoc results (first ks) (or (get extents (first ks)) (get results (first ks)))))))))))

  (uniform-units [this segment]
    (let [units (units/standard-units-for (last (:window/timeout-gap window)))
          k (:window/window-key window)]
      (update segment k units/coerce-key units)))

  (bounds [this window-id]
    [(:session-lower-bound window-id)
     (:session-upper-bound window-id)]))

(defmulti windowing-record
  "Given a window, return the concrete type to perform
   operations against."
  (fn [window]
    (:window/type window)))

(defmethod windowing-record :fixed
  [window] (FixedWindow. window))

(defmethod windowing-record :sliding
  [window] (SlidingWindow. window))

(defmethod windowing-record :global
  [window] (GlobalWindow. window))

(defmethod windowing-record :session
  [window] (SessionWindow. window))
