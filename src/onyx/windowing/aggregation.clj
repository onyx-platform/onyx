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

(defn average-aggregation-fn-init [window]
  {:sum 0 :n 0})

(defn conj-aggregation-fn [state window segment]
  [:conj segment])

(defn sum-aggregation-fn [state window segment]
  (let [k (second (:window/aggregation window))]
    [:set-value (+ state (get segment k))]))

(defn count-aggregation-fn [state window segment]
  [:set-value (inc state)])

(defn min-aggregation-fn [state window segment]
  (let [k (second (:window/aggregation window))]
    [:set-value (clojure.core/min state (get segment k))]))

(defn max-aggregation-fn [state window segment]
  (let [k (second (:window/aggregation window))]
    [:set-value (clojure.core/max state (get segment k))]))

(defn average-aggregation-fn [state window segment]
  (let [k (second (:window/aggregation window))
        sum (+ (:sum state)
               (get segment k))
        n (inc (:n state))]
    [:set-value {:n n :sum sum :average (/ sum n)}]))

(defn conj-super-aggregation [state-1 state-2 window]
  (concat state-1 state-2))

(defn sum-super-aggregation [state-1 state-2 window]
  (+ state-1 state-2))

(defn count-super-aggregation [state-1 state-2 window]
  (+ state-1 state-2))

(defn min-super-aggregation [state-1 state-2 window]
  (clojure.core/min state-1 state-2))

(defn max-super-aggregation [state-1 state-2 window]
  (clojure.core/max state-1 state-2))

(defn average-super-aggregation [state-1 state-2 window]
  (let [n* (+ (:n state-1) (:n state-2))
        sum* (+ (:sum state-1) (:sum state-2))]
    {:n n*
     :sum sum*
     :average (/ sum* n*)}))

(def conj
  {:aggregation/init conj-aggregation-fn-init
   :aggregation/fn conj-aggregation-fn
   :aggregation/apply-state-update conj-aggregation-apply-log
   :aggregation/super-aggregation-fn conj-super-aggregation})

(def sum
  {:aggregation/init sum-aggregation-fn-init
   :aggregation/fn sum-aggregation-fn
   :aggregation/apply-state-update set-value-aggregation-apply-log
   :aggregation/super-aggregation-fn sum-super-aggregation})

(def count
  {:aggregation/init count-aggregation-fn-init
   :aggregation/fn count-aggregation-fn
   :aggregation/apply-state-update set-value-aggregation-apply-log
   :aggregation/super-aggregation-fn count-super-aggregation})

(def min
  {:aggregation/fn min-aggregation-fn
   :aggregation/apply-state-update set-value-aggregation-apply-log
   :aggregation/super-aggregation-fn min-super-aggregation})

(def max
  {:aggregation/fn max-aggregation-fn
   :aggregation/apply-state-update set-value-aggregation-apply-log
   :aggregation/super-aggregation-fn max-super-aggregation})

(def average
  {:aggregation/init average-aggregation-fn-init
   :aggregation/fn average-aggregation-fn
   :aggregation/apply-state-update set-value-aggregation-apply-log
   :aggregation/super-aggregation-fn average-super-aggregation})
