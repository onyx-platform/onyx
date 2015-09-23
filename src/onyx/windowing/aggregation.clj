(ns onyx.windowing.aggregation)

(defn set-value-aggregation-apply-log [state v]
  v)

(defn conj-aggregation-apply-log [state v]
  (conj state v))

(defn conj-aggregation-fn-init []
  [])

(defn conj-aggregation-fn [state window segment]
  [:conj segment])

(defn count-aggregation-fn-init []
  0)

(defn count-aggregation-fn [state window segment]
  [:set-value (inc state)])

(defn sum-aggregation-fn-init []
  0)

(defn sum-aggregation-fn [state window segment]
  [:set-value (+ state (get segment (:window/sum-key window)))])

(defn min-aggregation-fn [state window segment]
  [:set-value (min state (get segment (:window/min-key window)))])

(defn max-aggregation-fn [state window segment]
  [:set-value (max state (get segment (:window/max-key window)))])

(defn average-aggregation-fn [state window segment]
  (let [sum (+ (:sum state)
               (get segment (:window/average-key window)))
        n (inc (:n state))]
    [:set-value {:n n :sum sum :average (/ sum n)}]))

(def init-resolve 
  {:conj conj-aggregation-fn-init
   :count count-aggregation-fn-init
   :sum sum-aggregation-fn-init})

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
