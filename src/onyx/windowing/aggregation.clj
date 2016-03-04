(ns onyx.windowing.aggregation
  (:require [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [onyx.schema :refer [Window Event]]
            [schema.core :as s])
  (:refer-clojure :exclude [min max count conj]))

(defn set-value-aggregation-apply-log [window state v]
  ;; Log command is not needed for single transition type
  v)

(defn conj-aggregation-apply-log [window state v]
  ;; Log command is not needed for single transition type
  (clojure.core/conj state v))

(defn conj-aggregation-fn-init [window]
  [])

(defn sum-aggregation-fn-init [window]
  0)

(defn count-aggregation-fn-init [window]
  0)

(defn average-aggregation-fn-init [window]
  {:sum 0 :n 0})

(defn conj-aggregation-fn [window state segment]
  ;; Log command is not needed for single transition type
  segment)

(defn sum-aggregation-fn [window state segment]
  (let [k (second (:window/aggregation window))]
    (+ state (get segment k))))

(defn count-aggregation-fn [window state segment]
  (inc state))

(defn min-aggregation-fn [window state segment]
  (let [k (second (:window/aggregation window))]
    (clojure.core/min state (get segment k))))

(defn max-aggregation-fn [window state segment]
  (let [k (second (:window/aggregation window))]
    (clojure.core/max state (get segment k))))

(defn average-aggregation-fn [window state segment]
  (let [k (second (:window/aggregation window))
        sum (+ (:sum state)
               (get segment k))
        n (inc (:n state))]
    {:n n :sum sum :average (/ sum n)}))

(defn conj-super-aggregation [window state-1 state-2]
  (into state-1 state-2))

(defn sum-super-aggregation [window state-1 state-2]
  (+ state-1 state-2))

(defn count-super-aggregation [window state-1 state-2]
  (+ state-1 state-2))

(defn min-super-aggregation [window state-1 state-2]
  (clojure.core/min state-1 state-2))

(defn max-super-aggregation [window state-1 state-2]
  (clojure.core/max state-1 state-2))

(defn average-super-aggregation [window state-1 state-2]
  (let [n* (+ (:n state-1) (:n state-2))
        sum* (+ (:sum state-1) (:sum state-2))]
    {:n n*
     :sum sum*
     :average (/ sum* n*)}))

(def conj
  {:aggregation/init conj-aggregation-fn-init
   :aggregation/create-state-update conj-aggregation-fn
   :aggregation/apply-state-update conj-aggregation-apply-log
   :aggregation/super-aggregation-fn conj-super-aggregation})

(def sum
  {:aggregation/init sum-aggregation-fn-init
   :aggregation/create-state-update sum-aggregation-fn
   :aggregation/apply-state-update set-value-aggregation-apply-log
   :aggregation/super-aggregation-fn sum-super-aggregation})

(def count
  {:aggregation/init count-aggregation-fn-init
   :aggregation/create-state-update count-aggregation-fn
   :aggregation/apply-state-update set-value-aggregation-apply-log
   :aggregation/super-aggregation-fn count-super-aggregation})

(def min
  {:aggregation/create-state-update min-aggregation-fn
   :aggregation/apply-state-update set-value-aggregation-apply-log
   :aggregation/super-aggregation-fn min-super-aggregation})

(def max
  {:aggregation/create-state-update max-aggregation-fn
   :aggregation/apply-state-update set-value-aggregation-apply-log
   :aggregation/super-aggregation-fn max-super-aggregation})

(def average
  {:aggregation/init average-aggregation-fn-init
   :aggregation/create-state-update average-aggregation-fn
   :aggregation/apply-state-update set-value-aggregation-apply-log
   :aggregation/super-aggregation-fn average-super-aggregation})
