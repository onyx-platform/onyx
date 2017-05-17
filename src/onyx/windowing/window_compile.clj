(ns ^:no-doc onyx.windowing.window-compile
  (:require [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [onyx.static.validation :as validation]
            [onyx.windowing.window-extensions :as w]
            [onyx.windowing.aggregation :as a]
            [onyx.state.state-extensions :as st]
            [onyx.schema :refer [TriggerState Trigger Window Event WindowState]]
            [onyx.peer.window-state :as ws]
            [onyx.types :refer [map->TriggerState]]
            [onyx.static.util :refer [kw->fn]]
            [onyx.static.planning :refer [only]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.peer.grouping :as g]
            [schema.core :as s]))

(defn filter-windows [windows task]
  (filter #(= (:window/task %) task) windows))

(defn compacted-reset? [entry]
  (and (map? entry)
       (= (:type entry) :compacted)))

(defn unpack-compacted [state {:keys [filter-snapshot extent-state]} event]
  (-> state
      (assoc :state extent-state)
      (update :filter st/restore-filter event filter-snapshot)))

(defn resolve-window-init [window calls]
  (if-not (:aggregation/init calls)
    (let [init (:window/init window)]
      (when-not init
        (throw (ex-info "No :window/init supplied, this is required for this aggregation" {:window window})))
      (constantly (:window/init window)))
    (:aggregation/init calls)))

(defn filter-ns-key-map [m ns-str]
  (->> m
       (filter (fn [[k _]] (= ns-str (namespace k))))
       (map (fn [[k v]] [(keyword (name k)) v]))
       (into {})))

(s/defn resolve-trigger :- TriggerState
  [{:keys [trigger/sync trigger/emit trigger/refinement trigger/on trigger/window-id] :as trigger} :- Trigger]
  (let [refinement-calls (var-get (kw->fn refinement))
        trigger-calls (var-get (kw->fn on))]
    (validation/validate-refinement-calls refinement-calls)
    (validation/validate-trigger-calls trigger-calls)
    (let [trigger (update trigger :trigger/id #(or % (random-uuid)))
          f-init-state (:trigger/init-state trigger-calls)
          f-init-locals (:trigger/init-locals trigger-calls)
          sync-fn (if sync (kw->fn sync))
          emit-fn (if emit (kw->fn emit))
          locals (f-init-locals trigger)]
      (-> trigger
          (filter-ns-key-map "trigger")
          (into locals)
          (assoc :trigger trigger)
          (assoc :sync-fn sync-fn)
          (assoc :emit-fn emit-fn)
          (assoc :state (atom (f-init-state trigger)))
          (assoc :init-state f-init-state)
          (assoc :next-trigger-state (:trigger/next-state trigger-calls))
          (assoc :trigger-fire? (:trigger/trigger-fire? trigger-calls))
          (assoc :create-state-update (:refinement/create-state-update refinement-calls))
          (assoc :apply-state-update (:refinement/apply-state-update refinement-calls))
          map->TriggerState))))

(defn new-ungrouped-window [m]
  (-> m
      (assoc :emitted (atom []))
      (assoc :state (atom {}))
      (ws/map->WindowUngrouped)))

(defn new-grouped-window [task-map m]
  (let [shared-trigger-emit (atom [])
        ungrouped (assoc (ws/map->WindowUngrouped m) :emitted shared-trigger-emit)] 
    (assoc (ws/map->WindowGrouped m)
           :emitted shared-trigger-emit
           :grouping-fn (g/task-map->grouping-fn task-map)
           :new-window-state-fn (fn [] 
                                  (assoc ungrouped
                                         :state (atom {})
                                         :trigger-states
                                         (mapv (fn [ts] 
                                                  (assoc ts :state (atom ((:init-state ts) (:trigger ts)))))
                                                (:trigger-states ungrouped)))))))

(defn build-window-state [task-map m]
  (if (g/grouped-task? task-map)
    (new-grouped-window task-map m)
    (new-ungrouped-window m)))

(s/defn resolve-window-state :- WindowState
  [window :- Window all-triggers :- [Trigger] task-map]
  (let [agg (:window/aggregation window)
        agg-var (if (sequential? agg) (first agg) agg)
        calls (var-get (kw->fn agg-var))
        _ (validation/validate-state-aggregation-calls calls)
        init-fn (resolve-window-init window calls)
        window-triggers (->> all-triggers 
                             (filter #(= (:window/id window) (:trigger/window-id %)))
                             (mapv resolve-trigger))]
    (build-window-state task-map 
                        {:window-extension (-> window
                                               (filter-ns-key-map "window")
                                               ((w/windowing-builder window))
                                               (assoc :window window))
                         :trigger-states window-triggers
                         :window window
                         :state (atom {}) 
                         :init-fn init-fn
                         :create-state-update (:aggregation/create-state-update calls)
                         :super-agg-fn (:aggregation/super-aggregation-fn calls)
                         :apply-state-update (:aggregation/apply-state-update calls)})))
