(ns onyx.windowing.aggregation)

(defn set-value-aggregation-apply-log
  [state v]
  v)

(defn conj-aggregation-apply-log
  [state v]
  (conj state v))

(defn conj-aggregation-fn-init []
  [])

(defn conj-aggregation-fn [state window segment]
  [:conj segment])

(defn count-aggregation-fn-init []
  0)

(defn count-aggregation-fn
  [state window segment]
  (let [state (or state (count-aggregation-fn-init))]
    [:set-value (inc state)]))

(defn sum-aggregation-fn-init []
  0)

(defn sum-aggregation-fn [state window segment]
  (let [state (or state (sum-aggregation-fn-init))]
    [:set-value (+ state (get segment (:window/sum-key window)))]))

(defn min-aggregation-fn
  [state window segment]
    (let [state (or state (:window/init window))]
      [:set-value (min state (get segment (:window/min-key window)))]))

(defn max-aggregation-fn
  [state window segment]
    (let [state (or state (:window/init window))]
      [:set-value (max state (get segment (:window/max-key window)))]))

(defn average-aggregation-fn
  [state window segment]
    (let [state (or state (:window/init window))
          n (inc state)]
      [:set-value {:n n
                   :average (/ (+ state (get segment (:window/average-key window))) n)}]))

(def aggregation-resolve 
  {:conj conj-aggregation-fn
   :count count-aggregation-fn
   :sum sum-aggregation-fn
   :min min-aggregation-fn
   :max max-aggregation-fn
   :average average-aggregation-fn})

(def apply-log-resolve
  {:conj conj-aggregation-apply-log
   :set-value set-value-aggregation-apply-log})
