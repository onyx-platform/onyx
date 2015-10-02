(ns onyx.windowing.aggregation
  (:refer-clojure :exclude [min max count conj]))

(defn set-value-aggregation-apply-log [state [t v]]
  (case t 
    :set-value v)) 

(defn conj-aggregation-apply-log [state [t v]]
  (case t
    :conj (clojure.core/conj state v)))

(defn conj-aggregation-fn-init [window]
  [])

(defn sum-aggregation-fn-init [window]
  0)

(defn count-aggregation-fn-init [window]
  0)

(defn conj-aggregation-fn [state window segment]
  [:conj segment])

(defn sum-aggregation-fn [state window segment]
  [:set-value (+ state (get segment (:window/sum-key window)))])

(defn count-aggregation-fn [state window segment]
  [:set-value (inc state)])

(defn min-aggregation-fn [state window segment]
  [:set-value (clojure.core/min state (get segment (:window/min-key window)))])

(defn max-aggregation-fn [state window segment]
  [:set-value (clojure.core/max state (get segment (:window/max-key window)))])

(defn average-aggregation-fn [state window segment]
  (let [sum (+ (:sum state)
               (get segment (:window/average-key window)))
        n (inc (:n state))]
    [:set-value {:n n :sum sum :average (/ sum n)}]))

(def conj
  {:aggregation/init conj-aggregation-fn-init
   :aggregation/fn conj-aggregation-fn
   :aggregation/log-resolve conj-aggregation-apply-log})

(def sum
  {:aggregation/init sum-aggregation-fn-init
   :aggregation/fn sum-aggregation-fn
   :aggregation/log-resolve set-value-aggregation-apply-log})

(def count
  {:aggregation/init count-aggregation-fn-init
   :aggregation/fn count-aggregation-fn
   :aggregation/log-resolve set-value-aggregation-apply-log})

(def min
  {:aggregation/fn min-aggregation-fn
   :aggregation/log-resolve set-value-aggregation-apply-log})

(def max
  {:aggregation/fn max-aggregation-fn
   :aggregation/log-resolve set-value-aggregation-apply-log})

(def average
  {:aggregation/fn average-aggregation-fn
   :aggregation/log-resolve set-value-aggregation-apply-log})
