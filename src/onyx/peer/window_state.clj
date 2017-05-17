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

(defprotocol StateEventReducer
  (window-id [this])
  (trigger-extent! [this state-event trigger-record extent])
  (trigger [this state-event trigger-record])
  (triggers [this state-event])
  (extent-state [this state-event])
  (recover-state [this dumped])
  (aggregate-state [this state-event])
  (apply-extents [this state-event])
  (apply-event [this state-event])
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
   init-fn create-state-update apply-state-update super-agg-fn event-results]

  StateEventReducer
  (window-id [this]
    (:window/id window))

  (apply-event [this state-event]
    (let [ks (if (= :new-segment (:event-type state-event)) 
               (list (:group-key state-event))
               (keys @state))] 
      (swap! state 
             (fn [ss]
               (reduce (fn [st k]
                         (let [kstate (-> (get st k)
                                          (or (new-window-state-fn))
                                          (apply-event (assoc state-event :group-key k)))]
                           (assoc ss k kstate)))
                       ss 
                       ks)))
      this))

  (export-state [this]
    (doall 
      (map (fn [[k kstate]]
             (list k (export-state kstate)))
           @state)))

  (recover-state [this stored]
    (swap! state
           (fn [s] 
              (reduce (fn [s* [k kstate]]
                        (assoc s* 
                               k 
                               (recover-state (new-window-state-fn) kstate)))
                      s
                      stored)))
    this))

(defrecord WindowUngrouped 
  [window-extension trigger-states window state init-fn emitted
   create-state-update apply-state-update super-agg-fn event-results]
  StateEventReducer
  (window-id [this]
    (:window/id window))

  (trigger-extent! [this state-event trigger-record extent]
    (let [{:keys [sync-fn emit-fn trigger create-state-update apply-state-update]} trigger-record
          extent-state (get @state extent)
          state-event (-> state-event
                          (assoc :extent extent)
                          (assoc :extent-state extent-state))
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
      (swap! state (fn [st] (assoc st extent new-extent-state)))))

  (trigger [this state-event trigger-record]
    (let [{:keys [trigger trigger-fire? fire-all-extents?]} trigger-record 
          state-event (-> state-event 
                          (assoc :window window) 
                          (assoc :trigger-state trigger-record))
          trigger-state (:state trigger-record)
          next-trigger-state-fn (:next-trigger-state trigger-record)
          new-trigger-state (next-trigger-state-fn trigger @trigger-state state-event)
          fire-all? (or fire-all-extents? (not= (:event-type state-event) :segment))
          fire-extents (if fire-all? 
                         (keys @state)
                         (:extents state-event))]
      (reset! trigger-state new-trigger-state)
      (run! (fn [extent] 
              (let [bounds (we/bounds window-extension extent)
                    state-event (-> state-event
                                    (assoc :lower-bound (first bounds))
                                    (assoc :upper-bound (second bounds)))]
                (when (trigger-fire? trigger new-trigger-state state-event)
                  (trigger-extent! this state-event trigger-record extent))))
            fire-extents)
      this))

  (export-state [this]
    (list @state (mapv (comp deref :state) trigger-states)))

  (recover-state [this [state trigger-states]]
    (reset! (:state this) state)
    (mapv (fn [t ts]
            (reset! (:state t) ts)
            t)
          (:trigger-states this)
          trigger-states)
    this)

  (triggers [this state-event]
    (reduce (fn [t trigger-state] 
              (trigger t state-event trigger-state))
            this
            trigger-states)
    state-event)

  (extent-state [this state-event]
    (let [{:keys [extent segment]} state-event]
      (swap! state
             update
             extent
             (fn [extent-state]
               (let [extent-state* (default-state-value init-fn window extent-state)
                     transition-entry (create-state-update window extent-state* segment)]
                 (apply-state-update window extent-state* transition-entry))))))

  (aggregate-state [this state-event]
    (run! (fn [extent] 
           (extent-state this (assoc state-event :extent extent)))
          (:extents state-event))
    state-event)

  (apply-extents [this state-event]
    (let [segment-coerced (we/uniform-units window-extension (:segment state-event))
          new-state (swap! state #(we/speculate-update window-extension % segment-coerced))
          extents (we/extents window-extension (keys new-state) segment-coerced)]
      (-> state-event
          (assoc :extents extents)
          (assoc :segment-coerced segment-coerced))))

  (apply-event [this state-event]
    (if (= (:event-type state-event) :new-segment)
      (let [merge-extents-fn (fn [{:keys [state] :as t} 
                                  {:keys [segment-coerced] :as state-event}]
                               (swap! state #(we/merge-extents window-extension % super-agg-fn segment-coerced))
                               state-event)] 
        (->> state-event
             (apply-extents this)
             (aggregate-state this)
             (merge-extents-fn this)
             (triggers this)))
      (triggers this state-event))
    this))

(defn fire-state-event [windows-state state-event]
  (mapv (fn [ws]
          (apply-event ws state-event))
        windows-state))

(defn process-segment
  [state state-event]
  (let [{:keys [grouping-fn onyx.core/results] :as event} (get-event state)
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
  (let [state-event (new-state-event event-type (get-event state))] 
    (if (= :new-segment event-type)
      (process-segment state state-event)
      (process-event state state-event))))
