(ns onyx.peer.grouping
  (:require [onyx.peer.operation :refer [resolve-fn]]
            [clj-tuple :as t]))

(defn task-map->grouping-fn [task-map]
  (if-let [group-key (:onyx/group-by-key task-map)]
    (cond (keyword? group-key)
          group-key
          (sequential? group-key)
          #(select-keys % group-key)
          :else
          #(get % group-key))
    (if-let [group-fn (:onyx/group-by-fn task-map)]
      (resolve-fn {:onyx/fn (:onyx/group-by-fn task-map)}))))

(defn compile-grouping-fn
  "Compiles outgoing grouping task info into a task->group-fn map
  for quick lookup and group fn calls"
  [catalog egress-tasks]
  (->> catalog
       (map (juxt :onyx/name task-map->grouping-fn))
       (filter (fn [[n f]]
                 (and f egress-tasks (egress-tasks n))))
       (into (t/hash-map))))

(defn grouped-task? [task-map]
  (or (:onyx/group-by-key task-map)
      (:onyx/group-by-fn task-map)))
