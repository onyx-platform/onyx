(ns onyx.windowing.window-extensions
  (:require [onyx.windowing.units :as units]
            [onyx.windowing.window-id :as wid]))

(defn window-id-impl-extents [window w-range w-slide message]
  (let [window-id (:window/id window)
        units (units/standard-units-for (last (:window/range window)))
        min-value (or (:window/min-value window) 0)]
    (wid/wids min-value w-range w-slide (:window/window-key window) message)))

(defprotocol IWindow
  (extents [this message]
    "Given a message, return the window identifier extents
     that it belongs to.")
  (uniform-units [this message]
    "Given a message, return an updated message with any needed
     changes in units. Note that a message is note a segment.
     The message is a larger data type which also contains a segment."))

(deftype FixedWindow [window]
  IWindow
  (extents [this message]
    (let [w-range (apply units/to-standard-units (:window/range window))]
      (window-id-impl-extents window w-range w-range message)))

  (uniform-units [this message]
    (let [units (units/standard-units-for (last (:window/range window)))
          k (:window/window-key window)]
      (update (:message message) k units/coerce-key units))))

(deftype SlidingWindow [window]
  IWindow
  (extents [this message]
    (let [w-range (apply units/to-standard-units (:window/range window))
          w-slide (apply units/to-standard-units (:window/slide window))]
      (window-id-impl-extents window w-range w-slide message)))

  (uniform-units [this message]
    (let [units (units/standard-units-for (last (:window/range window)))
          k (:window/window-key window)]
      (update (:message message) k units/coerce-key units))))

(deftype GlobalWindow [window]
  IWindow
  (extents [this message]
    ;; Always return the same window ID, the actual number
    ;; doesn't matter - as long as its constant.
    [1])

  (uniform-units [this message]
    message))

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
