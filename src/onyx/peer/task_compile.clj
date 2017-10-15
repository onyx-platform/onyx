(ns ^:no-doc onyx.peer.task-compile
  (:require [clojure.set :refer [subset?]]
            [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [schema.core :as s]
            [onyx.schema :refer [Trigger Window TriggerState WindowExtension Event]]
            [onyx.flow-conditions.fc-compile :as fc]
            [onyx.lifecycles.lifecycle-compile :as lc]
            [onyx.peer.transform :as t]
            [onyx.peer.grouping :as g]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.static.validation :as validation]
            [onyx.static.logging :as logging]
            [onyx.refinements]
            [onyx.peer.operation :refer [resolve-fn]]
            [onyx.windowing.window-compile :as wc]))

(defn resolve-watermark-fn [task-map]
  (if-let [fkw (:onyx/assign-watermark-fn task-map)]
    (let [f (resolve-fn {:onyx/fn fkw})] 
      (fn [segment] 
        (or (f segment) 0)))))

(s/defn filter-triggers 
  [windows :- [WindowExtension]
   triggers :- [Trigger]]
  (filter #(some #{(:trigger/window-id %)}
                 (map :id windows))
          triggers))

(defn flow-conditions->event-map 
  [{:keys [onyx.core/flow-conditions onyx.core/workflow onyx.core/task] :as event}]
  (-> event
      (assoc :compiled-norm-fcs (fc/compile-fc-happy-path flow-conditions workflow task))
      (assoc :compiled-ex-fcs (fc/compile-fc-exception-path flow-conditions workflow task)))) 

(s/defn windowed-task? [event]
  (boolean 
   (or (not-empty (:onyx.core/windows event))
       (not-empty (:onyx.core/triggers event)))))

(defn task->event-map
  [{:keys [onyx.core/serialized-task onyx.core/task-map] :as event}] 
  (-> event 
      (assoc :grouping-fn (g/task-map->grouping-fn task-map))
      (assoc :assign-watermark-fn (resolve-watermark-fn task-map))
      (assoc :egress-tasks (:egress-tasks serialized-task))))

(defn task-params->event-map [{:keys [onyx.core/peer-opts onyx.core/task-map] :as event}]
  (let [fn-params (:onyx.peer/fn-params peer-opts)
        params (into (vec (get fn-params (:onyx/name task-map)))
                     (map (fn [param] (get task-map param))
                          (:onyx/params task-map)))]
    (assoc event :onyx.core/params params)))
