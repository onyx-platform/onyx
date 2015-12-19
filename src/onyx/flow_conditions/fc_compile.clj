(ns onyx.flow-conditions.fc-compile
  (:require [onyx.peer.operation :refer [kw->fn]]))

(defn pred-fn? [expr]
  (and (keyword? expr)
       (not= :and expr)
       (not= :or expr)
       (not= :not expr)))

(defn build-pred-fn [expr entry]
  (if (pred-fn? expr)
    (fn [xs] (apply (kw->fn expr) xs))
    (let [[op & more :as full-expr] expr]
      (cond (= op :and)
            (do (assert (> (count more) 1) ":and takes at least two predicates")
                (fn [xs]
                  (every? identity (map (fn [token] ((build-pred-fn token entry) xs)) more))))

            (= op :or)
            (do (assert (> (count more) 1) ":or takes at least two predicates")
                (fn [xs]
                  (some identity (map (fn [token] ((build-pred-fn token entry) xs)) more))))

            (= op :not)
            (do (assert (= 1 (count more)) ":not only takes one predicate")
                (fn [xs]
                  (not ((build-pred-fn (first more) entry) xs))))

            :else
            (fn [xs]
              (apply (kw->fn op) (concat xs (map (fn [arg] (get entry arg)) more))))))))

