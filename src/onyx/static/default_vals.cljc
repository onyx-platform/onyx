(ns onyx.static.default-vals
  (:require [onyx.information-model :refer [model]]))

(def default-vals
  "Indexes all keys to their defaul values for a direct look up."
  (reduce
   (fn [result section]
     (reduce-kv
      (fn [all k v]
        (assoc all k (:default v)))
      result
      (:model section)))
   {}
   (vals model)))

(defn arg-or-default [k opts]
  {:post [(not (nil? %))]}
  (let [v (get opts k (get default-vals k))]
    (when (nil? v)
      (throw (ex-info (format "Default for config key %s not found." k) {:key k})))
    v))
