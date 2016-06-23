(ns onyx.static.planning
  (:require [com.stuartsierra.dependency :as dep]
            [taoensso.timbre :as timbre :refer [debug info]])
  (:import [java.util UUID]))

(defmulti create-io-task
  (fn [task-ids element parents children]
    (:onyx/type element)))

(defn only [coll]
  (when (next coll)
    (throw (ex-info "More than one element in collection, expected count of 1" {:coll coll})))
  (if-let [result (first coll)]
    result
    (throw (ex-info "Zero elements in collection, expected exactly one" {:coll coll}))))

(defn find-task [catalog task-name]
  (let [matches (filter #(= task-name (:onyx/name %)) catalog)]
    (only matches)))

(defn egress-ids-from-children [task-ids elements]
  (into {}
        (map (juxt identity task-ids)
             (map :onyx/name elements))))

(defn grouping-task? [task-map]
  (boolean (or (:onyx/group-by-fn task-map)
               (:onyx/group-by-key task-map))))

(defmulti create-task
  (fn [task-ids catalog task-name parents children-names]
    (:onyx/type (find-task catalog task-name))))

(defmethod create-task :default
  [task-ids catalog task-name parents children-names]
  (let [element (find-task catalog task-name)
        children (map (partial find-task catalog) children-names)]
    (create-io-task task-ids element parents children)))

(defn onyx-function-task [task-ids catalog task-name parents children-names]
  (let [element (find-task catalog task-name)
        children (map (partial find-task catalog) children-names)
        element-name (:onyx/name element)
        task-id (task-ids element-name)]
    {:id task-id
     :name element-name
     :egress-ids (egress-ids-from-children task-ids children)}))

(defmethod create-task :function
  [task-ids catalog task-name parents children-names]
  (onyx-function-task task-ids catalog task-name parents children-names))

(defmethod create-task :grouper
  [task-ids catalog task-name parents children-names]
  (onyx-function-task task-ids catalog task-name parents children-names))

(defmethod create-task :aggregator
  [task-ids catalog task-name parents children-names]
  (onyx-function-task task-ids catalog task-name parents children-names))

(defmethod create-io-task :input
  [task-ids element parent children]
  {:id (task-ids (:onyx/name element))
    :name (:onyx/name element)
    :egress-ids (egress-ids-from-children task-ids children)})

(defmethod create-io-task :output
  [task-ids element parents children]
  (let [task-name (:onyx/name element)]
    {:id (task-ids task-name)
     :name task-name}))

(defn to-dependency-graph [workflow]
  (reduce (fn [g edge]
            (apply dep/depend g (reverse edge)))
          (dep/graph) workflow))

(defn remove-dupes [coll]
  (map last (vals (group-by :name coll))))

(defn gen-task-ids [nodes]
  (into {} (map (juxt identity identity) nodes)))

(defn discover-tasks [catalog workflow]
  (let [dag (to-dependency-graph workflow)
        sorted-dag (dep/topo-sort dag)
        task-ids (gen-task-ids sorted-dag)]
    (remove-dupes
     (reduce
      (fn [tasks element]
        (let [parents (dep/immediate-dependencies dag element)
              children (dep/immediate-dependents dag element)
              parent-entries (filter #(some #{(:name %)} parents) tasks)]
          (conj tasks (create-task task-ids catalog element parent-entries children))))
      []
      sorted-dag))))
