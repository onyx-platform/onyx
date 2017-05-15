(ns ^:no-doc onyx.peer.window-state
    (:require [com.stuartsierra.component :as component]
              [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
              [schema.core :as s]
              [clojure.core.async :refer [alts!! <!! >!! <! >! timeout chan close! thread go]]
              [onyx.schema :refer [TriggerState WindowExtension Window Event]]
              [onyx.monitoring.measurements :refer [emit-latency emit-latency-value]]
              [onyx.windowing.window-extensions :as we]
              [onyx.protocol.task-state :refer :all]
              [onyx.types :refer [->MonitorEvent new-state-event]]
              [onyx.state.state-extensions :as state-extensions]
              [onyx.static.default-vals :refer [arg-or-default]]
              [onyx.static.util :refer [exception?]]))

(s/defn default-state-value 
  [init-fn window state-value]
  (or state-value (init-fn window)))

(defprotocol WindowStateKeyed
  (keyed-state [this k]))

(defprotocol StateEventReducer
  (window-id [this])
  (trigger-extent [this])
  (trigger [this])
  (triggers [this])
  (extent-state [this])
  (recover-state [this dumped])
  (aggregate-state [this])
  (apply-extents [this])
  (apply-event [this])
  (export-state [this]))

(defn rollup-result [segment]
  (cond (sequential? segment) 
        segment 
        (map? segment)
        (list segment)
        :else
        (throw (ex-info "Value returned by :trigger/emit must be either a hash-map or a sequential of hash-maps." 
                        {:value segment}))))

(defrecord WindowGrouped 
  [window-extension grouping-fn window state new-window-state-fn emitted
   init-fn create-state-update apply-state-update super-agg-fn state-event event-results]

  WindowStateKeyed
  (window-id [this]
    (:window/id window))
  (keyed-state [this k]
    (-> (get state k)
        (or (new-window-state-fn))
        (assoc :state-event (assoc state-event :group-key k))))

  StateEventReducer
  (apply-event [this]
    (let [ks (if (= :new-segment (:event-type state-event)) 
               (list (:group-key state-event))
               (keys state))] 
      (reduce (fn [t k]
                (let [kstate (apply-event (keyed-state t k))]
                  (update t :state assoc k kstate)))
              this
              ks)))

  (export-state [this]
    (doall 
      (map (fn [[k kstate]]
             (list k (export-state kstate)))
           state)))

  (recover-state [this stored]
    (assoc this 
           :state 
           (reduce (fn [state [k kstate]]
                     (assoc state 
                            k 
                            (recover-state (new-window-state-fn) kstate)))
                   state
                   stored))))

(defrecord WindowUngrouped 
  [window-extension trigger-states window state init-fn emitted
   create-state-update apply-state-update super-agg-fn state-event event-results]
  StateEventReducer
  (window-id [this]
    (:window/id window))

  (trigger-extent [this]
    (let [{:keys [trigger-state extent]} state-event 
          {:keys [sync-fn emit-fn trigger create-state-update apply-state-update]} trigger-state
          extent-state (get state extent)
          state-event (assoc state-event :extent-state extent-state)
          entry (create-state-update trigger extent-state state-event)
          new-extent-state (apply-state-update trigger extent-state entry)
          state-event (-> state-event
                          (assoc :next-state new-extent-state)
                          (assoc :trigger-update entry))
          emit-segment (when emit-fn 
                         (emit-fn (:task-event state-event) 
                                  window trigger state-event extent-state))]
      (when sync-fn 
        (sync-fn (:task-event state-event) window trigger state-event extent-state))
      (when emit-segment 
        (swap! emitted (fn [em] (into em (rollup-result emit-segment)))))
      (assoc this :state (assoc state extent new-extent-state))))

  (trigger [this]
    (let [{:keys [trigger-index trigger-state]} state-event
          {:keys [trigger next-trigger-state trigger-fire? fire-all-extents?]} trigger-state 
          state-event (assoc state-event :window window)
          new-trigger-state (next-trigger-state trigger (:state trigger-state) state-event)
          fire-all? (or fire-all-extents? (not= (:event-type state-event) :segment))
          fire-extents (if fire-all? 
                         (keys state)
                         (:extents state-event))]
      (reduce (fn [t extent] 
                (let [[lower-bound upper-bound] (we/bounds window-extension extent)
                      state-event (-> state-event
                                      (assoc :lower-bound lower-bound)
                                      (assoc :upper-bound upper-bound))]
                  (if (trigger-fire? trigger new-trigger-state state-event)
                    (trigger-extent (assoc t 
                                           :state-event 
                                           (assoc state-event :extent extent)))   
                    t)))
              (assoc-in this [:trigger-states trigger-index :state] new-trigger-state)
              fire-extents)))

  (export-state [this]
    (list state (mapv :state trigger-states)))

  (recover-state [this [state trigger-states]]
    (-> this
        (assoc :state state)
        (update :trigger-states
                (fn [ts]
                  (mapv (fn [t ts]
                          (assoc t :state ts))
                        ts
                        trigger-states)))))

  (triggers [this]
    (reduce (fn [t [trigger-index trigger-state]] 
              (trigger (assoc t :state-event (-> state-event
                                                 (assoc :trigger-index trigger-index)
                                                 (assoc :trigger-state trigger-state)))))
            this
            (map-indexed list trigger-states)))

  (extent-state [this]
    (let [{:keys [extent segment]} state-event
          extent-state (->> (get state extent)
                            (default-state-value init-fn window))
          transition-entry (create-state-update window extent-state segment)
          new-extent-state (apply-state-update window extent-state transition-entry)]
      (assoc this :state (assoc state extent new-extent-state))))

  (apply-extents [this]
    (let [segment-coerced (we/uniform-units window-extension (:segment state-event))
          state* (we/speculate-update window-extension state segment-coerced)
          extents (we/extents window-extension (keys state*) segment-coerced)]
      (-> this 
          (assoc :state state*)
          (assoc :state-event (assoc state-event
                                     :extents extents
                                     :segment-coerced segment-coerced)))))

  (aggregate-state [this]
    (reduce (fn [t extent] 
              (extent-state (assoc t :state-event (assoc state-event :extent extent))))
            this
            (:extents state-event)))

  (apply-event [this]
    (if (= (:event-type state-event) :new-segment)
      (let [merge-extents-fn (fn [this]
                               (let [{:keys [segment segment-coerced]} state-event
                                     extents (we/merge-extents window-extension (:state this) super-agg-fn segment-coerced)]
                                 (assoc this :state extents)))] 
        (-> this 
            apply-extents
            aggregate-state
            merge-extents-fn
            triggers))
      (triggers this))))

(defn fire-state-event [windows-state state-event]
  (mapv (fn [ws]
          (apply-event (assoc ws 
                              :state-event state-event
                              :state-results [])))
        windows-state))

(defn process-segment
  [state state-event]
  (let [{:keys [grouping-fn onyx.core/monitoring onyx.core/results] :as event} (get-event state)
        grouped? (not (nil? grouping-fn))
        state-event* (assoc state-event :grouped? grouped?)
        windows-state (get-windows-state state)
        updated-states (reduce 
                        (fn [windows-state* segment]
                          (if (exception? segment)
                            windows-state*
                            (let [state-event** (cond-> (assoc state-event* :segment segment)
                                                  grouped? (assoc :group-key (grouping-fn segment)))]
                              (fire-state-event windows-state* state-event**))))
                        windows-state
                        (mapcat :leaves (:tree results)))
        emitted (doall (mapcat (comp deref :emitted) updated-states))]
    (run! (fn [w] (reset! (:emitted w) [])) windows-state)
    (-> state 
        (set-windows-state! updated-states)
        (update-event! (fn [e] (update e :onyx.core/triggered into emitted))))))

(defn process-event [state state-event]
  (set-windows-state! state (fire-state-event (get-windows-state state) state-event)))

(defn assign-windows [state event-type]
  (let [messenger (get-messenger state)
        event (get-event state)
        state-event (new-state-event event-type event)] 
    (if (= :new-segment event-type)
      (process-segment state state-event)
      (process-event state state-event))))
