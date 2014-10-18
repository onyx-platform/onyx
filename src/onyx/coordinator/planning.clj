(ns onyx.coordinator.planning
  (:require [onyx.extensions :as extensions])
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

(defn egress-queues-to-children [elements]
  (into {} (map #(hash-map (:onyx/name %) (onyx-queue-name)) elements)))

(defmulti create-task
  (fn [catalog task-name parent children-names phase]
    (:onyx/type (find-task catalog task-name))))

(defmethod create-task :default
  [catalog task-name parent children-names phase]
  (let [element (find-task catalog task-name)
        children (map (partial find-task catalog) children-names)]
    (extensions/create-io-task element parent children phase)))

(defn onyx-internal-task [catalog task-name parent children-names phase]
  (let [element (find-task catalog task-name)
        children (map (partial find-task catalog) children-names)]
    {:id (UUID/randomUUID)
     :name (:onyx/name element)
     :ingress-queues (get (:egress-queues parent) task-name)
     :egress-queues (egress-queues-to-children children)
     :phase phase
     :consumption (:onyx/consumption element)}))

(defmethod create-task :transformer
  [catalog task-name parent children-names phase]
  (onyx-internal-task catalog task-name parent children-names phase))

(defmethod create-task :grouper
  [catalog task-name parent children-names phase]
  (onyx-internal-task catalog task-name parent children-names phase))

(defmethod create-task :aggregator
  [catalog task-name parent children-names phase]
  (onyx-internal-task catalog task-name parent children-names phase))

(defmethod extensions/create-io-task :input
  [element parent children phase]
  {:id (UUID/randomUUID)
   :name (:onyx/name element)
   :ingress-queues (onyx-queue-name)
   :egress-queues (egress-queues-to-children children)
   :phase phase
   :consumption (:onyx/consumption element)})

(defmethod extensions/create-io-task :output
  [element parent children phase]
  {:id (UUID/randomUUID)
   :name (:onyx/name element)
   :ingress-queues (get (:egress-queues parent) (:onyx/name element))
   :egress-queues {:self (onyx-queue-name)}
   :phase phase
   :consumption (:onyx/consumption element)})

(defn children [tree]
  (if (map? tree)
    (keys tree)
    (vector tree)))

(defn discover-tasks
  ([catalog workflow] (distinct (discover-tasks catalog workflow [] nil 1)))
  ([catalog workflow tasks parent phase]
     (if (keyword? workflow) (conj tasks (create-task catalog workflow parent [] phase))
         (let [roots (keys workflow)]
           (mapcat
            (fn [root]
              (let [child-tree (get workflow root)
                    root-task (create-task catalog root parent (children child-tree) phase)
                    new-tasks (conj tasks root-task)]
                (discover-tasks catalog child-tree new-tasks root-task (inc phase))))
            roots)))))

(defn unpack-map-workflow
  ([workflow] (unpack-map-workflow workflow []))
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

