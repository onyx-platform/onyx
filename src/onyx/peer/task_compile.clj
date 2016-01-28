(ns ^:no-doc onyx.peer.task-compile
  (:require [clojure.set :refer [subset?]]
            [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [onyx.peer.operation :refer [kw->fn]]
            [onyx.flow-conditions.fc-compile :as fc]
            [onyx.lifecycles.lifecycle-compile :as lc]
            [onyx.peer.grouping :as g]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.windowing.window-compile :as wc]))

(defn filter-triggers [triggers windows]
  (filter #(some #{(:trigger/window-id %)}
                 (map :window/id windows))
          triggers))

(defn resolve-triggers [triggers]
  (map
   #(assoc %
      :trigger/id (random-uuid)
      :trigger/sync-fn (kw->fn (:trigger/sync %)))
   triggers))

(defn resolve-window-triggers [triggers windows event]
  (merge
   event
   {:onyx.core/triggers (resolve-triggers (filter-triggers triggers windows))}))

(defn windows->event-map [windows event]
  (assoc event :onyx.core/windows (wc/resolve-windows windows)))

(defn flow-conditions->event-map [{:keys [onyx.core/flow-conditions onyx.core/workflow onyx.core/task] :as event}]
  (update event 
          :onyx.core/compiled 
          (fn [compiled] 
            (-> compiled
                (assoc :flow-conditions flow-conditions)
                (assoc :compiled-norm-fcs (fc/compile-fc-happy-path flow-conditions workflow task))
                (assoc :compiled-ex-fcs (fc/compile-fc-exception-path flow-conditions workflow task)))))) 

(defn task->event-map [{:keys [onyx.core/task-map onyx.core/id onyx.core/pipeline onyx.core/job-id 
                               onyx.core/catalog onyx.core/serialized-task onyx.core/messenger 
                               onyx.core/monitoring onyx.core/state onyx.core/peer-replica-view] :as event}]
  (update event 
          :onyx.core/compiled 
          (fn [compiled] 
            (-> compiled
                (assoc :pipeline pipeline)
                (assoc :messenger messenger)
                (assoc :monitoring monitoring)
                (assoc :job-id job-id)
                (assoc :id id)
                (assoc :state state)
                (assoc :bulk? (:onyx/bulk? task-map))
                (assoc :fn (:onyx.core/fn event))
                (assoc :task-type (:onyx/type task-map))
                (assoc :peer-replica-view peer-replica-view)
                (assoc :grouping-fn (g/task-map->grouping-fn task-map))
                (assoc :task->group-by-fn (g/compile-grouping-fn catalog (:egress-ids serialized-task)))
                (assoc :egress-ids (keys (:egress-ids serialized-task)))))))

(defn lifecycles->event-map [{:keys [onyx.core/lifecycles onyx.core/task] :as event}]
  (update event 
          :onyx.core/compiled 
          (fn [compiled] 
            (-> compiled
                (assoc :compiled-start-task-fn
                       (lc/compile-start-task-functions lifecycles task))
                (assoc :compiled-before-task-start-fn
                       (lc/compile-before-task-start-functions lifecycles task))
                (assoc :compiled-before-batch-fn
                       (lc/compile-before-batch-task-functions lifecycles task))
                (assoc :compiled-after-read-batch-fn
                       (lc/compile-after-read-batch-task-functions lifecycles task))
                (assoc :compiled-after-batch-fn
                       (lc/compile-after-batch-task-functions lifecycles task))
                (assoc :compiled-after-task-fn
                       (lc/compile-after-task-functions lifecycles task))
                (assoc :compiled-after-ack-segment-fn
                       (lc/compile-after-ack-segment-functions lifecycles task))
                (assoc :compiled-after-retry-segment-fn
                       (lc/compile-after-retry-segment-functions lifecycles task))
                (assoc :compiled-handle-exception-fn
                       (lc/compile-handle-exception-functions lifecycles task))))))

(defn task-params->event-map [{:keys [onyx.core/peer-opts onyx.core/task-map] :as event}]
  (let [fn-params (:onyx.peer/fn-params peer-opts)
        params (into (vec (get fn-params (:onyx/name task-map)))
                     (map (fn [param] (get task-map param))
                          (:onyx/params task-map)))]
    (assoc event :onyx.core/params params)))
