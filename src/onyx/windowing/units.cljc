(ns onyx.windowing.units)

(defmulti standard-units-for
  (fn [unit] unit))

(defmethod standard-units-for :milliseconds
  [unit] :milliseconds)

(defmethod standard-units-for :seconds
  [unit] :milliseconds)

(defmethod standard-units-for :minutes
  [unit] :milliseconds)

(defmethod standard-units-for :hours
  [unit] :milliseconds)

(defmethod standard-units-for :days
  [unit] :milliseconds)

(defmethod standard-units-for :elements
  [unit] :elements)

(defmulti to-standard-units
  (fn [v unit] unit))

(defmethod to-standard-units :milliseconds
  [v unit] v)

(defmethod to-standard-units :seconds
  [v unit]
  (to-standard-units (* 1000 v) :milliseconds))

(defmethod to-standard-units :minutes
  [v unit]
  (to-standard-units (* 60 v) :seconds))

(defmethod to-standard-units :hours
  [v unit]
  (to-standard-units (* 60 v) :minutes))

(defmethod to-standard-units :days
  [v unit]
  (to-standard-units (* 24 v) :hours))

(defmethod to-standard-units :elements
  [v unit] v)

;;; Pluralized versions for readability

(defmethod standard-units-for :millisecond
  [unit] (standard-units-for :milliseconds))

(defmethod standard-units-for :second
  [unit] (standard-units-for :seconds))

(defmethod standard-units-for :minute
  [unit] (standard-units-for :minutes))

(defmethod standard-units-for :hour
  [unit] (standard-units-for :hours))

(defmethod standard-units-for :day
  [unit] (standard-units-for :days))

(defmethod standard-units-for :element
  [unit] (standard-units-for :elements))

(defmethod to-standard-units :millisecond
  [v unit] (to-standard-units v :milliseconds))

(defmethod to-standard-units :second
  [v unit] (to-standard-units v :seconds))

(defmethod to-standard-units :minute
  [v unit] (to-standard-units v :minutes))

(defmethod to-standard-units :hour
  [v unit] (to-standard-units v :hours))

(defmethod to-standard-units :day
  [v unit] (to-standard-units v :days))

(defmethod to-standard-units :element
  [v unit] (to-standard-units v :elements))

(defprotocol ICoerceKey
  (coerce-key [_ units]))

#?(:clj
   (extend-type java.sql.Timestamp
     ICoerceKey
     (coerce-key [this units]
       (to-standard-units (.getTime this) units))))

#?(:clj
   (extend-type java.util.Date
          ICoerceKey
          (coerce-key [this units]
            (to-standard-units (.getTime this) units))))

#?(:cljs
   (extend-type js/Date
          ICoerceKey
          (coerce-key [this units]
            (to-standard-units (.getTime this) units))))

#?(:clj
   (extend-type java.lang.Long
          ICoerceKey
          (coerce-key [this units]
            (to-standard-units this units))))

#?(:clj
   (extend-type java.lang.Integer
          ICoerceKey
          (coerce-key [this units]
            (to-standard-units this units))))

#?(:cljs
   (extend-type number
          ICoerceKey
          (coerce-key [this units]
            (to-standard-units this units))))
