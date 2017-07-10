(ns ^:no-doc onyx.windowing.window-compile
  (:require [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [onyx.static.validation :as validation]
            [onyx.windowing.window-extensions :as w]
            [onyx.windowing.aggregation :as a]
            [onyx.state.state-extensions :as st]
            [onyx.schema :refer [TriggerState Trigger Window Event WindowState]]
            [onyx.peer.window-state :as ws]
            [onyx.types :refer [map->TriggerState]]
            [onyx.state.memory]
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
  [indexes {:keys [trigger/sync trigger/emit trigger/refinement trigger/on trigger/id trigger/window-id] :as trigger} :- Trigger]
  (let [refinement-calls (var-get (kw->fn refinement))
        trigger-calls (var-get (kw->fn on))]
    (validation/validate-refinement-calls refinement-calls)
    (validation/validate-trigger-calls trigger-calls)
    (let [f-init-state (:trigger/init-state trigger-calls)
          f-init-locals (:trigger/init-locals trigger-calls)
          sync-fn (if sync (kw->fn sync))
          emit-fn (if emit (kw->fn emit))
          locals (f-init-locals trigger)]
      (-> trigger
          (filter-ns-key-map "trigger")
          (into locals)
          (assoc :trigger trigger)
          (assoc :idx (or (get indexes [id window-id]) (throw (Exception.))))
          (assoc :id (:trigger/id trigger))
          (assoc :sync-fn sync-fn)
          (assoc :emit-fn emit-fn)
          (assoc :init-state f-init-state)
          (assoc :next-trigger-state (:trigger/next-state trigger-calls))
          (assoc :trigger-fire? (:trigger/trigger-fire? trigger-calls))
          (assoc :create-state-update (:refinement/create-state-update refinement-calls))
          (assoc :apply-state-update (:refinement/apply-state-update refinement-calls))
          map->TriggerState))))

(defn resolve-window-extension [window]
  (-> window
      (filter-ns-key-map "window")
      ((w/windowing-builder window))
      (assoc :window window)))

(s/defn build-window-executor :- WindowState
  [{:keys [window/id] :as window} :- Window all-triggers :- [Trigger] state-store indexes task-map]
  (let [agg (:window/aggregation window)
        agg-var (if (sequential? agg) (first agg) agg)
        calls (var-get (kw->fn agg-var))
        _ (validation/validate-state-aggregation-calls calls)
        init-fn (resolve-window-init window calls)
        triggers (->> all-triggers 
                      (filter #(= (:window/id window) (:trigger/window-id %)))
                      (map (partial resolve-trigger indexes))
                      (map (juxt :idx identity))
                      (into {}))]
    (ws/map->WindowExecutor
     {:id id 
      :idx (get indexes id)
      :window-extension (resolve-window-extension window)
      :triggers triggers
      :emitted (atom [])
      :window window
      :state-store state-store
      :init-fn init-fn
      :create-state-update (:aggregation/create-state-update calls)
      :super-agg-fn (:aggregation/super-aggregation-fn calls)
      :apply-state-update (:aggregation/apply-state-update calls)})))
