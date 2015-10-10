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
  (extents [this segment]
    "Given a segment, return the window identifier extents
     that it belongs to.")
  (uniform-units [this segment]
    "Given a segment, return an updated segment with any needed changes in units.")

  (bounds [this window-id]
    "Returns a vector of two elements. The first is the lower bound that this window
     id accepts, and the second is the upper."))

(deftype FixedWindow [window]
  IWindow
  (extents [this segment]
    (let [w-range (apply units/to-standard-units (:window/range window))]
      (window-id-impl-extents window w-range w-range segment)))

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
  (extents [this segment]
    (let [w-range (apply units/to-standard-units (:window/range window))
          w-slide (apply units/to-standard-units (:window/slide window))]
      (window-id-impl-extents window w-range w-slide segment)))

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
  (extents [this segment]
    ;; Always return the same window ID, the actual number
    ;; doesn't matter - as long as its constant.
    [1])

  (uniform-units [this segment]
    segment)

  (bounds [this window-id]
    ;; Everything is in bounds.
    [Double/NEGATIVE_INFINITY Double/POSITIVE_INFINITY]))

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
