(ns onyx.flow-conditions.fc-compile
  (:require [clojure.set :refer [subset?]]
            [onyx.static.util :refer [kw->fn]]))

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

(defn egress-tasks [workflow task]
  (map second (filter #(= (first %) task) workflow)))

(defn only-relevant-branches [flow-conditions workflow task]
  (filter #(or (= (:flow/from %) task)
               (and (= (:flow/from %) :all)
                    (or (= (:flow/to %) :all)
                        (subset? (into #{} (:flow/to %))
                                 (into #{} (egress-tasks workflow task))))))
          flow-conditions))

(defn compile-flow-conditions [flow-conditions workflow task-name f]
  (let [branches (only-relevant-branches flow-conditions workflow task-name)
        conditions (filter f branches)]
    (map
     (fn [condition]
       (assoc condition :flow/predicate (build-pred-fn (:flow/predicate condition) condition)))
     conditions)))

(defn compile-fc-happy-path [flow-conditions workflow task-name]
  (compile-flow-conditions flow-conditions workflow task-name
                           (comp not :flow/thrown-exception?)))

(defn compile-fc-exception-path [flow-conditions workflow task-name]
  (compile-flow-conditions flow-conditions workflow task-name
                           :flow/thrown-exception?))
