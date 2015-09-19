(ns onyx.windowing.aggregation)

(defmulti aggregation-fn-init
  (fn [operation] operation))

(defmulti aggregation-fn
  (fn [operation] operation))

(defmethod aggregation-fn-init :conj
  [operation]
  [])

(defmethod aggregation-fn-init :count
  [operation]
  0)

(defmethod aggregation-fn-init :sum
  [operation]
  0)

(defmethod aggregation-fn-init :default
  [operation]
  (throw
   (ex-info
    (format "No initial value defined for %s, init value must be suppled by :window/init in window specification" operation)
    {:operation operation})))

(defmethod aggregation-fn :conj
  [operation]
  (fn [state window segment]
    (let [state (or state (aggregation-fn-init operation))]
      (conj state segment))))

(defmethod aggregation-fn :count
  [operation]
  (fn [state window segment]
    (let [state (or state (aggregation-fn-init operation))]
      (inc state))))

(defmethod aggregation-fn :sum
  [operation]
  (fn [state window segment]
    (let [state (or state (aggregation-fn-init operation))]
      (+ state (get segment (:window/sum-key window))))))

(defmethod aggregation-fn :min
  [operation]
  (fn [state window segment]
    (let [state (or state (:window/init window))]
      (min state (get segment (:window/min-key window))))))

(defmethod aggregation-fn :max
  [operation]
  (fn [state window segment]
    (let [state (or state (:window/init window))]
      (max state (get segment (:window/max-key window))))))

(defmethod aggregation-fn :average
  [operation]
  (fn [state window segment]
    (let [state (or state (:window/init window))
          n (inc state)]
      {:n n
       :average (/ (+ state (get segment (:window/average-key window))) n)})))

(defmethod aggregation-fn :default
  [operation]
  (throw (ex-info (format "No aggregation function named %s is defined." operation) {:operation operation})))
