(ns onyx.windowing.window-id)

(defmulti to-standard-units
  (fn [v unit] unit))

(defmethod to-standard-units :millisecond
  [v unit] (to-standard-units v :milliseconds))

(defmethod to-standard-units :milliseconds
  [v unit] v)

(defmethod to-standard-units :second
  [v unit] (to-standard-units v :seconds))

(defmethod to-standard-units :seconds
  [v unit]
  (to-standard-units (* 1000 v) :milliseconds))

(defmethod to-standard-units :minute
  [v unit] (to-standard-units v :minutes))

(defmethod to-standard-units :minutes
  [v unit]
  (to-standard-units (* 60 v) :seconds))

(defmethod to-standard-units :hour
  [v unit] (to-standard-units v :hours))

(defmethod to-standard-units :hours
  [v unit]
  (to-standard-units (* 60 v) :minutes))

(defmethod to-standard-units :day
  [v unit] (to-standard-units v :days))

(defmethod to-standard-units :days
  [v unit]
  (to-standard-units (* 24 v) :hours))

(defmethod to-standard-units :element
  [v unit] (to-standard-units v :elements))

(defmethod to-standard-units :elements
  [v unit] v)

(defprotocol ICoerceKey
  (coerce-key [_ units]))

(extend-type java.sql.Timestamp
  ICoerceKey
  (coerce-key [this units]
    (to-standard-units (.getTime this) units)))

(extend-type java.lang.Long
  ICoerceKey
  (coerce-key [this units]
    (to-standard-units this units)))

(extend-type java.lang.Integer
  ICoerceKey
  (coerce-key [this units]
    (to-standard-units this units)))
