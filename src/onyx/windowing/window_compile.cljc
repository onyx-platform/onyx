(ns ^:no-doc onyx.windowing.window-compile
  (:require [onyx.windowing.window-extensions :as w]
            [onyx.windowing.aggregation :as a]
            #?(:clj [taoensso.timbre :refer [info error warn trace fatal] :as timbre])
            #?(:clj [onyx.static.validation :as validation])
            #?(:clj [onyx.schema :refer [TriggerState Trigger Window Event WindowState]])
            #?(:clj [schema.core :as s])
            [onyx.peer.window-state :as ws]
            [onyx.types :refer [map->TriggerState]]
            [onyx.state.memory]
            [onyx.static.util :as u]
            [onyx.peer.grouping :as g]))

(defn filter-windows [windows task]
  (filter #(= (:window/task %) task) windows))

(defn resolve-window-init [window calls]
  (if-not (:aggregation/init calls)
    (let [init (:window/init window)]
      (when-not init
        (throw (ex-info "No :window/init supplied, this is required for this aggregation" {:window window})))
      (constantly (:window/init window)))
    (:aggregation/init calls)))

(defn resolve-var [v]
  #?(:clj (var-get v))
  #?(:cljs v))

(defn filter-ns-key-map [m ns-str]
  (->> m
       (filter (fn [[k _]] (= ns-str (namespace k))))
       (map (fn [[k v]] [(keyword (name k)) v]))
       (into {})))

(defn resolve-trigger [indices {:keys [trigger/sync trigger/emit trigger/refinement trigger/on
                                       trigger/id trigger/window-id trigger/state-context
                                       trigger/pre-evictor trigger/post-evictor] :as trigger}]
  (let [refinement-calls (when refinement (resolve-var (u/kw->fn refinement)))
        trigger-calls (resolve-var (u/kw->fn on))]
    #?(:clj (when refinement-calls (validation/validate-refinement-calls refinement-calls)))
    #?(:clj (validation/validate-trigger-calls trigger-calls))
    (let [f-init-state (:trigger/init-state trigger-calls)
          sync-fn (if sync (u/kw->fn sync))
          emit-fn (if emit (u/kw->fn emit))
          locals (if-let [f-init-locals (:trigger/init-locals trigger-calls)]
                   (f-init-locals trigger))
          state-context-trigger? (or (empty? state-context) (boolean (some #{:trigger-state} state-context)))
          state-context-window? (boolean (some #{:window-state} state-context))]
      (when (and (not f-init-state)
                 (not state-context-window?))
        (throw (ex-info "Trigger calls for this trigger does not include a :trigger/init-state fn, and thus must use `:trigger/state-context [:window-state]`" 
                        trigger)))
      (-> trigger
          (filter-ns-key-map "trigger")
          (into locals)
          (assoc :trigger trigger)
          (assoc :state-context-window? state-context-window?)
          (assoc :state-context-trigger? state-context-trigger?)
          (assoc :pre-evictor pre-evictor)
          (assoc :post-evictor post-evictor)
          (assoc :idx (or (get indices [id window-id]) (throw (ex-info "Could not find state index for window id." {}))))
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

(defn build-window-executor [{:keys [window/id window/storage-strategy] :as window} all-triggers state-store indices task-map]
  (let [agg (:window/aggregation window)
        agg-var (if (sequential? agg) (first agg) agg)
        calls (resolve-var (u/kw->fn agg-var))
        f-init-locals (:aggregation/init-locals calls identity)
        locals (f-init-locals window)
        window (into window locals)
        #?@(:clj (_ (validation/validate-state-aggregation-calls calls)))
        init-fn (resolve-window-init window calls)
        triggers (->> all-triggers 
                      (filter #(= (:window/id window) (:trigger/window-id %)))
                      (map (partial resolve-trigger indices))
                      (map (juxt :idx identity))
                      (into {}))]
    (ws/map->WindowExecutor
     {:id id 
      :idx (get indices id)
      :window-extension (resolve-window-extension window)
      :triggers triggers
      :emitted (atom [])
      :window window
      :grouped? (g/grouped-task? task-map)
      :incremental? (or (empty? storage-strategy) (boolean (some #{:incremental} storage-strategy)))
      :store-extents? (boolean (some #{:extents} storage-strategy))
      :ordered-log? (boolean (some #{:ordered-log} storage-strategy))
      :state-store state-store
      :init-fn init-fn
      :create-state-update (:aggregation/create-state-update calls)
      :super-agg-fn (:aggregation/super-aggregation-fn calls)
      :apply-state-update (:aggregation/apply-state-update calls)})))
