(ns onyx.windowing.aggregation
  (:refer-clojure :exclude [min max count conj]))

(defn set-value-aggregation-apply-log [window state v]
  ;; Log command is not needed for single transition type
  v)

(defn conj-aggregation-apply-log [window state v]
  ;; Log command is not needed for single transition type
  (clojure.core/conj state v))

(defn conj-aggregation-fn-init [window]
  [])

(defn conj-aggregation-fn [window segment]
  ;; Log command is not needed for single transition type
  segment)

(defn conj-super-aggregation [window state-1 state-2]
  (into state-1 state-2))

(def ^:export conj
  {:aggregation/init conj-aggregation-fn-init
   :aggregation/create-state-update conj-aggregation-fn
   :aggregation/apply-state-update conj-aggregation-apply-log
   :aggregation/super-aggregation-fn conj-super-aggregation})

(defn collect-by-key-aggregation-fn-init [window]
  {})

(defn collect-by-key-aggregation-apply-log [window state segment]
  (let [k (get segment (second (:window/aggregation window)))]
    (update state k (fnil clojure.core/conj #{}) segment)))

(defn collect-by-key-aggregation-fn [window segment]
  segment)

(defn collect-by-key-super-aggregation [window state-1 state-2]
  (merge state-1 state-2))

(def ^:export collect-by-key
  {:aggregation/init collect-by-key-aggregation-fn-init
   :aggregation/create-state-update collect-by-key-aggregation-fn
   :aggregation/apply-state-update collect-by-key-aggregation-apply-log
   :aggregation/super-aggregation-fn collect-by-key-super-aggregation})

(defn collect-key-value-aggregation-apply-log [window state v]
  (clojure.core/conj state v))

(defn collect-key-value-aggregation-fn-init [window]
  [])

(defn collect-key-value-aggregation-fn [window segment]
  (get segment (second (:window/aggregation window))))

(defn collect-key-value-super-aggregation [window state-1 state-2]
  (merge state-1 state-2))

(def ^:export collect-key-value
  {:aggregation/init collect-key-value-aggregation-fn-init
   :aggregation/create-state-update collect-key-value-aggregation-fn
   :aggregation/apply-state-update collect-key-value-aggregation-apply-log
   :aggregation/super-aggregation-fn collect-key-value-super-aggregation})

(defn sum-aggregation-fn-init [window]
  0)

(defn sum-create-state-update-fn [window segment]
  (let [k (second (:window/aggregation window))]
    (get segment k)))

(defn sum-aggregation-function [_ state v]
  (+ state v))

(defn sum-super-aggregation [window state-1 state-2]
  (+ state-1 state-2))

(def ^:export sum
  {:aggregation/init sum-aggregation-fn-init
   :aggregation/create-state-update sum-create-state-update-fn
   :aggregation/apply-state-update sum-aggregation-function
   :aggregation/super-aggregation-fn sum-super-aggregation})

(defn count-aggregation-fn-init [window]
  0)

(defn count-aggregation-apply-fn [window state _]
  (inc state))

(defn count-aggregation-create-state-update [_ _])

(defn count-super-aggregation [window state-1 state-2]
  (+ state-1 state-2))

(def ^:export count
  {:aggregation/init count-aggregation-fn-init
   :aggregation/create-state-update count-aggregation-create-state-update
   :aggregation/apply-state-update count-aggregation-apply-fn
   :aggregation/super-aggregation-fn count-super-aggregation})

(defn min-aggregation-create-fn [window segment]
  (let [k (second (:window/aggregation window))]
    (get segment k)))

(defn min-aggregation-apply-fn [window state v]
  (clojure.core/min state v))

(defn min-super-aggregation [window state-1 state-2]
  (clojure.core/min state-1 state-2))

(def ^:export min
  {:aggregation/create-state-update min-aggregation-create-fn
   :aggregation/apply-state-update min-aggregation-apply-fn
   :aggregation/super-aggregation-fn min-super-aggregation})

(defn max-aggregation-fn [window segment]
  (let [k (second (:window/aggregation window))]
    (get segment k)))

(defn max-create-aggregation-apply-fn [window state v]
  (clojure.core/max state v))

(defn max-super-aggregation [window state-1 state-2]
  (clojure.core/max state-1 state-2))

(def ^:export max
  {:aggregation/create-state-update max-aggregation-fn
   :aggregation/apply-state-update max-create-aggregation-apply-fn
   :aggregation/super-aggregation-fn max-super-aggregation})

(defn average-aggregation-fn-init [window]
  {:sum 0 :n 0})

(defn average-aggregation-apply-fn [window state v]
  (let [sum (+ (:sum state) v)
        n (inc (:n state))]
    {:n n :sum sum :average (/ sum n)}))

(defn average-aggregation-create-fn [window segment]
  (let [k (second (:window/aggregation window))]
    (get segment k)))

(defn average-super-aggregation [window state-1 state-2]
  (let [n* (+ (:n state-1) (:n state-2))
        sum* (+ (:sum state-1) (:sum state-2))]
    {:n n*
     :sum sum*
     :average (/ sum* n*)}))

(def ^:export average
  {:aggregation/init average-aggregation-fn-init
   :aggregation/create-state-update average-aggregation-create-fn
   :aggregation/apply-state-update average-aggregation-apply-fn
   :aggregation/super-aggregation-fn average-super-aggregation})
