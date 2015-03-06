(ns onyx.planning
  (:require [com.stuartsierra.dependency :as dep])
  (:import [java.util UUID]))

(defmulti create-io-task
  (fn [element parents children]
    (:onyx/type element)))

(defn only [coll]
  (assert (not (next coll)))
  (if-let [result (first coll)]
    result
    (assert false)))

(defn find-task [catalog task-name]
  (let [matches (filter #(= task-name (:onyx/name %)) catalog)]
    (only matches)))

(defn egress-ids-from-children [elements]
  (into {} (map #(hash-map (:onyx/name %) (java.util.UUID/randomUUID)) elements)))

(defmulti create-task
  (fn [catalog task-name parents children-names]
    (:onyx/type (find-task catalog task-name))))

(defmethod create-task :default
  [catalog task-name parents children-names]
  (let [element (find-task catalog task-name)
        children (map (partial find-task catalog) children-names)]
    (create-io-task element parents children)))

(defn onyx-function-task [catalog task-name parents children-names]
  (let [element (find-task catalog task-name)
        children (map (partial find-task catalog) children-names)]
    (if (= (count (into #{} (map (comp (:onyx/name element) :egress-ids) parents))) 1)
      [{:id (get (:egress-ids (first parents)) (:onyx/name element))
        :name (:onyx/name element)
        :egress-ids (egress-ids-from-children children)}]
      (concat
       [{:id (get (:egress-ids (first parents)) (:onyx/name element))
         :name (:onyx/name element)
         :egress-ids (egress-ids-from-children children)}]
       (map #(assoc-in % [:egress-ids (:onyx/name element)]
                       (get (:egress-ids (first parents)) (:onyx/name element)))
            (rest parents))))))

(defmethod create-task :function
  [catalog task-name parents children-names]
  (onyx-function-task catalog task-name parents children-names))

(defmethod create-task :grouper
  [catalog task-name parents children-names]
  (onyx-function-task catalog task-name parents children-names))

(defmethod create-task :aggregator
  [catalog task-name parents children-names]
  (onyx-function-task catalog task-name parents children-names))

(defmethod create-io-task :input
  [element parent children]
  [{:id (UUID/randomUUID)
    :name (:onyx/name element)
    :egress-ids (egress-ids-from-children children)}])

(defmethod create-io-task :output
  [element parents children]
  (let [task-name (:onyx/name element)]
    [{:id (get (:egress-ids (first parents)) (:onyx/name element))
      :name (:onyx/name element)}]))

(defn to-dependency-graph [workflow]
  (reduce (fn [g edge]
            (apply dep/depend g (reverse edge)))
          (dep/graph) workflow))

(defn remove-dupes [coll]
  (map last (vals (group-by :name coll))))

(defn discover-tasks [catalog workflow]
  (let [dag (to-dependency-graph workflow)
        sorted-dag (dep/topo-sort dag)]
    (remove-dupes
     (reduce
      (fn [tasks element]
        (let [parents (dep/immediate-dependencies dag element)
              children (dep/immediate-dependents dag element)
              parent-entries (filter #(some #{(:name %)} parents) tasks)]
          (concat tasks (create-task catalog element parent-entries children))))
      []
      sorted-dag))))

