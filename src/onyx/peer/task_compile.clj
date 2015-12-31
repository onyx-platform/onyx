(ns ^:no-doc onyx.peer.task-compile
  (:require [clojure.set :refer [subset?]]
            [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [onyx.peer.operation :refer [kw->fn]]
            [onyx.flow-conditions.fc-compile :as fc]
            [onyx.lifecycles.lifecycle-compile :as lc]
            [onyx.windowing.window-compile :as wc]))

(defn filter-triggers [triggers windows]
  (filter #(some #{(:trigger/window-id %)}
                 (map :window/id windows))
          triggers))

(defn resolve-triggers [triggers]
  (map
   #(assoc %
      :trigger/id (java.util.UUID/randomUUID)
      :trigger/sync-fn (kw->fn (:trigger/sync %)))
   triggers))

(defn windows->event-map [event windows]
  (assoc event :onyx.core/windows (wc/resolve-windows windows)))

(defn flow-conditions->event-map [event flow-conditions task-name]
  (let [workflow (:onyx.core/workflow event)]
    (-> event
        (assoc :onyx.core/compiled-norm-fcs
               (fc/compile-fc-happy-path flow-conditions workflow task-name))
        (assoc :onyx.core/compiled-ex-fcs
               (fc/compile-fc-exception-path flow-conditions workflow task-name)))))

(defn lifecycles->event-map [event lifecycles task-name]
  (-> event
      (assoc :onyx.core/compiled-start-task-fn
             (lc/compile-start-task-functions lifecycles task-name))
      (assoc :onyx.core/compiled-before-task-start-fn
             (lc/compile-before-task-start-functions lifecycles task-name))
      (assoc :onyx.core/compiled-before-batch-fn
             (lc/compile-before-batch-task-functions lifecycles task-name))
      (assoc :onyx.core/compiled-after-read-batch-fn
             (lc/compile-after-read-batch-task-functions lifecycles task-name))
      (assoc :onyx.core/compiled-after-batch-fn
             (lc/compile-after-batch-task-functions lifecycles task-name))
      (assoc :onyx.core/compiled-after-task-fn
             (lc/compile-after-task-functions lifecycles task-name))
      (assoc :onyx.core/compiled-after-ack-segment-fn
             (lc/compile-after-ack-segment-functions lifecycles task-name))
      (assoc :onyx.core/compiled-after-retry-segment-fn
             (lc/compile-after-retry-segment-functions lifecycles task-name))
      (assoc :onyx.core/compiled-handle-exception-fn
             (lc/compile-handle-exception-functions lifecycles task-name))))

(defn task-params->event-map [event peer-config task-map]
  (let [fn-params (:onyx.peer/fn-params peer-config)
        params (into (vec (get fn-params (:onyx/name task-map)))
                     (map (fn [param] (get task-map param))
                          (:onyx/params task-map)))]
    (assoc event :onyx.core/params params)))
