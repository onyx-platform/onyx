(ns onyx.coordinator.planning
  (:require [onyx.coordinator.extensions :as extensions])
  (:import [java.util UUID]))

(defn only
  "Like first, but throws unless exactly one item."
  [coll]
  (assert (not (next coll)))
  (if-let [result (first coll)]
    result
    (assert false)))

(defn find-task [catalog task-name]
  (let [matches (filter #(= task-name (:onyx/name %)) catalog)]
    (only matches)))

(defn egress-queues-to-children [task-names]
  (map
   (fn [x]
     (if (= :queue (:onyx/type x))
       (:ingress-queue (extensions/create-io-task x nil))
       (str (UUID/randomUUID))))
   task-names))

(defmulti create-task
  (fn [catalog task-name parent children-names]
    (:onyx/type (find-task catalog task-name))))

(defmethod create-task :queue
  [catalog task-name parent children-names]
  (let [element (find-task catalog task-name)]
    (extensions/create-io-task element parent)))

(defmethod create-task :transformer
  [catalog task-name parent children-names]
  (let [element (find-task catalog task-name)
        children (map (partial find-task catalog) children-names)]
    {:name (:onyx/name element)
     :ingress-queue (:egress-queue parent)
     :egress-queue (egress-queues-to-children children)}))

(defn children [tree]
  (if (map? tree)
    (keys tree)
    (vector tree)))

(defn discover-tasks
  ([catalog workflow] (distinct (discover-tasks catalog workflow [] nil 1)))
  ([catalog workflow tasks parent phase]
     (if (keyword? workflow) (conj tasks (create-task catalog workflow parent []))
         (let [roots (keys workflow)]
           (mapcat
            (fn [root]
              (let [child-tree (get workflow root)
                    root-task (create-task catalog root parent (children child-tree))
                    new-tasks (conj tasks root-task)]
                (discover-tasks catalog child-tree new-tasks root-task (inc phase))))
            roots)))))

