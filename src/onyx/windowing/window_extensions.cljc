(ns onyx.windowing.window-extensions
  (:require [onyx.windowing.units :refer [to-standard-units coerce-key] :as units]
            [onyx.windowing.window-id :as wid]
            [onyx.static.default-vals :as d]))

(defn window-id-impl-extents [units min-value w-range w-slide window-time]
  (let [min-value (or min-value 0)]
    (wid/wids min-value w-range w-slide window-time)))

(defprotocol IWindow

  (extent-operations [this all-extents segment time-index]
    "Given a segment time and all extents, return the vector of operations that should be performed on the windows.
     Operations take the form [action arg1 arg2].
     Support actions are:
     [:merge-extents extent1 extent2 merged-extent]
     [:alter-extents old-extent new-extent]
     [:update extent]")

  (time-index [this segment]
    "Given a segment, return the coerced window time for the window key.")

  (bounds [this window-id]
    "Returns a vector of two elements. The first is the lower bound that this window
     id accepts, and the second is the upper."))

(defrecord FixedWindow 
  [id task type init window-key min-value range w-range units slide timeout-gap doc window]
  IWindow

  (extent-operations [this extents _ time-index]
    (map (fn [extent] 
           [:update extent])
         (window-id-impl-extents units min-value w-range w-range time-index)))

  (time-index [this segment]
    (units/coerce-key (get segment window-key) units))

  (bounds [this window-id]
    (let [win-min (or min-value (get d/default-vals :onyx.windowing/min-value))]
      [(wid/extent-lower win-min w-range w-range window-id)
       (wid/extent-upper win-min w-range window-id)])))

(defrecord SlidingWindow 
  [id task type init window-key min-value range slide units w-range w-slide timeout-gap doc window]
  IWindow

  (extent-operations [this _ _ time-index]
    (map (fn [extent] 
           [:update extent])
         (window-id-impl-extents units min-value w-range w-slide time-index)))

  (time-index [this segment]
    (units/coerce-key (get segment window-key) units))

  (bounds [this window-id]
    (let [win-min (or min-value (get d/default-vals :onyx.windowing/min-value))]
      [(wid/extent-lower win-min w-range w-slide window-id)
       (wid/extent-upper win-min w-slide window-id)])))

(defrecord GlobalWindow 
  [id task type init window-key min-value range slide timeout-gap doc window]
  IWindow

  (extent-operations [this _ _ time-index]
    ;; Always return the same window ID, the actual number
    ;; doesn't matter - as long as its constant.
    [[:update 1]])

  (time-index [this segment] 0)

  (bounds [this window-id]
    ;; Everything is in bounds.
    #?(:clj  [Double/NEGATIVE_INFINITY Double/POSITIVE_INFINITY])
    #?(:cljs [(.-NEGATIVE_INFINITY js/Number) (.-POSITIVE_INFINITY js/Number)])))

(defn bounding-extents 
  "Find the extents with the closest lower bounds."
  [extents session-time]
  (loop [extent (first extents) 
         vs (rest extents)
         closest-below [#?(:clj Long/MAX_VALUE
                           :cljs (.-POSITIVE_INFINITY js/Number)) 
                        nil]
         closest-above [#?(:clj Long/MAX_VALUE
                           :cljs (.-POSITIVE_INFINITY js/Number)) 
                        nil]]
    (if (nil? extent)
      [(second closest-below)
       (second closest-above)]
      (let [[session-lower-bound] extent
            lower-distance (- session-time session-lower-bound)
            new-closest-below (if (and (<= session-lower-bound session-time)
                                       (< lower-distance (first closest-below)))
                                [lower-distance extent] 
                                closest-below)
            upper-distance (- session-lower-bound session-time)
            new-closest-above (if (and (>= session-lower-bound session-time)
                                       (< upper-distance (first closest-above)))
                                [upper-distance extent]
                                closest-above)]
        (recur (first vs) 
               (rest vs)
               new-closest-below
               new-closest-above)))))

(defrecord SessionWindow 
  [id task type init window-key min-value range slide gap timeout-gap units doc window]
  IWindow
  (extent-operations [this all-extents _ time-index]
    (let [[below-extent above-extent] (bounding-extents @all-extents time-index)
          [below-lower below-upper] below-extent
          [above-lower above-upper] above-extent 
          below-contains? (and below-upper (>= below-upper (- time-index gap)))
          above-contains? (and above-lower (>= (+ time-index gap) above-lower))]
      (cond ;; matches point exactly
            (and below-extent above-extent (= below-extent above-extent))
            [[:update below-extent]]

            (and below-contains? above-contains?)
            [[:merge-extents
              [below-lower below-upper] 
              [above-lower above-upper]
              [below-lower above-upper]]
             [:update [below-lower above-upper]]]

            (and below-contains? (> time-index below-upper))
            [[:alter-extents 
              [below-lower below-upper] 
              [below-lower time-index]]
             [:update [below-lower time-index]]]

            below-contains?
            [[:update [below-lower (max below-upper time-index)]]]

            (and above-contains? (< time-index above-lower))
            [[:alter-extents 
              [above-lower above-upper] 
              [time-index above-upper]]
             [:update [time-index above-upper]]]

            above-contains?
            [[:update [above-lower (max time-index above-upper)]]]

            ;; no windows matched
            :else
            [[:update [time-index time-index]]])))

  (time-index [this segment]
    (units/coerce-key (get segment window-key) units))

  (bounds [this window-id]
    window-id))

(defmulti extent-serializer
  "Given a window, return the type of extent serializer"
  (fn [window]
    (:window/type window)))

(defmethod extent-serializer :fixed
  [window] 
  :long)

(defmethod extent-serializer :sliding
  [window] 
  :long)

(defmethod extent-serializer :global
  [window] 
  :nil)

(defmethod extent-serializer :session
  [window] 
  :long-long)

(defmulti windowing-builder
  "Given a window, return the concrete type to perform
   operations against."
  (fn [window]
    (:window/type window)))

(defmethod windowing-builder :fixed
  [window] 
  (fn [{:keys [range] :as m}] 
    (-> m
        (assoc :units (units/standard-units-for (last range)))
        (assoc :w-range (apply units/to-standard-units range))
        (map->FixedWindow))))

(defmethod windowing-builder :sliding
  [window] 
  (fn [{:keys [range slide] :as m}] 
    (-> m
        (assoc :units (units/standard-units-for (last range)))
        (assoc :w-range (apply units/to-standard-units range))
        (assoc :w-slide (apply to-standard-units (or slide range)))
        (map->SlidingWindow))))

(defmethod windowing-builder :global
  [window] map->GlobalWindow)

(defmethod windowing-builder :session
  [window] 
  (fn [{:keys [timeout-gap] :as m}]
    (-> m
        (assoc :units (units/standard-units-for (last timeout-gap)))
        (assoc :gap (apply units/to-standard-units timeout-gap))   
        map->SessionWindow)))
