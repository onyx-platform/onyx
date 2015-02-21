(ns onyx.planning
  (:require [com.stuartsierra.dependency :as dep]
            [onyx.extensions :as extensions]
            [onyx.peer.operation :refer [kw->fn]])
  (:import [java.util UUID]))

(defn only [coll]
  (assert (not (next coll)))
  (if-let [result (first coll)]
    result
    (assert false)))

(defn find-task [catalog task-name]
  (let [matches (filter #(= task-name (:onyx/name %)) catalog)]
    (only matches)))

(defn onyx-queue-name []
  (str "onyx." (UUID/randomUUID)))

(defn ingress-queues-from-parents [parents task-name]
  (into {} (map #(hash-map (:name %) (get (:egress-queues %) task-name)) parents)))

(defn egress-queues-from-children [elements]
  (into {} (map #(hash-map (:onyx/name %) (onyx-queue-name)) elements)))

(defmulti create-task
  (fn [catalog task-name parents children-names]
    (:onyx/type (find-task catalog task-name))))

(defmethod create-task :default
  [catalog task-name parents children-names]
  (let [element (find-task catalog task-name)
        children (map (partial find-task catalog) children-names)]
    (extensions/create-io-task element parents children)))

(defn onyx-function-task [catalog task-name parents children-names]
  (let [element (find-task catalog task-name)
        children (map (partial find-task catalog) children-names)]
    {:id (UUID/randomUUID)
     :name (:onyx/name element)
     :ingress-queues (ingress-queues-from-parents parents task-name)
     :egress-queues (egress-queues-from-children children)
     :consumption (:onyx/consumption element)}))

(defmethod create-task :function
  [catalog task-name parents children-names]
  (onyx-function-task catalog task-name parents children-names))

(defmethod create-task :grouper
  [catalog task-name parents children-names]
  (onyx-function-task catalog task-name parents children-names))

(defmethod create-task :aggregator
  [catalog task-name parents children-names]
  (onyx-function-task catalog task-name parents children-names))

(defmethod extensions/create-io-task :input
  [element parent children]
  {:id (UUID/randomUUID)
   :name (:onyx/name element)
   :ingress-queues {:self (onyx-queue-name)}
   :egress-queues (egress-queues-from-children children)
   :consumption (:onyx/consumption element)})

(defmethod extensions/create-io-task :output
  [element parents children]
  (let [task-name (:onyx/name element)]
    {:id (UUID/randomUUID)
     :name (:onyx/name element)
     :ingress-queues (ingress-queues-from-parents parents task-name)
     :egress-queues {:self (onyx-queue-name)}
     :consumption (:onyx/consumption element)}))

(defn to-dependency-graph [workflow]
  (reduce (fn [g edge]
            (apply dep/depend g (reverse edge)))
          (dep/graph) workflow))

(defn discover-tasks [catalog workflow]
  (let [dag (to-dependency-graph workflow)
        sorted-dag (dep/topo-sort dag)]
    (map-indexed
     #(assoc %2 :phase %1)
     (reduce
      (fn [tasks element]
        (let [parents (dep/immediate-dependencies dag element)
              children (dep/immediate-dependents dag element)
              parent-entries (filter #(some #{(:name %)} parents) tasks)]
          (conj tasks (create-task catalog element parent-entries children))))
      []
      sorted-dag))))

(defn unpack-map-workflow
  ([workflow] (vec (unpack-map-workflow workflow [])))
  ([workflow result]
     (let [roots (keys workflow)]
       (if roots
         (concat result
                 (mapcat
                  (fn [k]
                    (let [child (get workflow k)]
                      (if (map? child)
                        (concat (map (fn [x] [k x]) (keys child))
                                (unpack-map-workflow child result))
                        [[k child]])))
                  roots))
         result))))

(defn pred-fn? [expr]
  (and (keyword? expr)
       (not= :and expr)
       (not= :or expr)
       (not= :not expr)))

(defn build-pred-fn [expr]
  (if (pred-fn? expr)
    (fn [xs] (apply (kw->fn expr) xs))
    (let [[op & more :as full-expr] expr]
      (cond (= op :and)
            (do (assert (> (count more) 1) ":and takes at least two predicates")
                (fn [xs]
                  (every? identity (map (fn [token] ((build-pred-fn token) xs)) more))))

            (= op :or)
            (do (assert (> (count more) 1) ":or takes at least two predicates")
                (fn [xs]
                  (some identity (map (fn [token] ((build-pred-fn token) xs)) more))))

            (= op :not)
            (do (assert (= 1 (count more)) ":not only takes one predicate")
                (fn [xs]
                  (not ((build-pred-fn (first more)) xs))))

            :else (throw (ex-info "Unknown routing composition operator" {:op op :more more}))))))

