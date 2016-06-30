(ns ^:no-doc onyx.peer.window-state
    (:require [com.stuartsierra.component :as component]
              [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
              [schema.core :as s]
              [clojure.core.async :refer [alts!! <!! >!! <! >! timeout chan close! thread go]]
              [onyx.schema :refer [TriggerState WindowExtension Window Event]]
              [onyx.monitoring.measurements :refer [emit-latency emit-latency-value]]
              [onyx.windowing.window-extensions :as we]
              [onyx.lifecycles.lifecycle-invoke :as lc]
              [onyx.types :refer [->Ack ->Results ->MonitorEvent dec-count! inc-count! map->Event map->Compiled new-state-event]]
              [onyx.state.ack :as st-ack]
              [onyx.state.state-extensions :as state-extensions]
              [onyx.static.default-vals :refer [defaults arg-or-default]]))

(s/defn default-state-value 
  [init-fn window state-value]
  (or state-value (init-fn window)))

(defprotocol WindowStateKeyed
  (keyed-state [this k]))

(defprotocol StateEventReducer
  (trigger-extent [this])
  (trigger [this])
  (triggers [this])
  (extent-state [this])
  (apply-extents [this])
  (apply-event [this])
  (aggregate-state [this])
  (log-entries [this])
  (state [this])
  (play-trigger-entry [this entry])
  (play-triggers-entry [this entry])
  (play-extent-entry [this entry])
  (play-aggregation-entry [this entry])
  (play-entry [this entry]))

(defn state-event->log-entry [{:keys [log-type] :as state-event}]
  (case log-type
    :trigger (list log-type (:trigger-index state-event) (:extent state-event) (:trigger-update state-event))
    :aggregation (list log-type (:extent state-event) (:aggregation-update state-event))))

(defn clean 
  "Used to clean up the window state so we don't have recursive event printing
  problems and excess memory usage"
  [window-state]
  (assoc window-state :event-results nil :state-event nil))

(defrecord WindowGrouped 
  [window-extension trigger-states grouping-fn window state new-window-state-fn
   init-fn create-state-update apply-state-update super-agg-fn state-event event-results]

  WindowStateKeyed
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
                  (-> t 
                      (update :state assoc k (clean kstate))
                      (update :event-results conj kstate))))
              this
              ks)))

  (log-entries [this]
    (->> event-results
         (map (juxt (comp :group-key :state-event) log-entries))
         (remove (comp empty? second))
         (doall)))

  (state [this]
    state)

  (play-entry [this entry]
    (reduce (fn [t [k e]]
              (assoc-in t 
                        [:state k] 
                        (play-entry (keyed-state t k) e)))
            this
            entry)))

(defrecord WindowUngrouped 
  [window-extension trigger-states window state init-fn 
   create-state-update apply-state-update super-agg-fn state-event event-results]
  StateEventReducer
  (play-trigger-entry [this [trigger-index extent transition-entry]]
    (let [{:keys [trigger apply-state-update] :as trigger-state} (trigger-states trigger-index)]
      (assoc this 
             :state 
             (update state 
                     extent
                     (fn [extent-state] 
                       (apply-state-update trigger extent-state transition-entry))))))

  (play-aggregation-entry [this [extent transition-entry]]
    (assoc this 
           :state 
           (update state 
                   extent 
                   (fn [extent-state] 
                     (apply-state-update window extent-state transition-entry)))))

  (play-entry [this entries]
    (reduce (fn [t [entry-type & rst]]
              (case entry-type
                :trigger (play-trigger-entry t rst)
                :aggregation (play-aggregation-entry t rst)))
            this
            entries))

  (trigger-extent [this]
    (let [{:keys [trigger-state extent]} state-event 
          {:keys [sync-fn trigger create-state-update apply-state-update]} trigger-state
          extent-state (get state extent)
          state-event (assoc state-event :extent-state extent-state)
          entry (create-state-update trigger extent-state state-event)
          new-extent-state (apply-state-update trigger extent-state entry)
          state-event (-> state-event
                          (assoc :next-state new-extent-state)
                          (assoc :trigger-update entry))]
      (sync-fn (:task-event state-event) window trigger state-event extent-state)
      (assoc this 
             :state (assoc state extent new-extent-state)
             :event-results (if (= extent-state new-extent-state)
                              event-results
                              (conj event-results state-event)))))

  (trigger [this]
    (let [{:keys [trigger-index trigger-state]} state-event
          {:keys [trigger next-trigger-state trigger-fire? fire-all-extents?]} trigger-state 
          state-event (assoc state-event :window window)
          new-trigger-state (next-trigger-state trigger (:state trigger-state) state-event)
          ;; TODO, scope this via :trigger/scope 
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

  (triggers [this]
    ;; index by trigger index in order to store the trigger index in the log entry
    (reduce (fn [t [trigger-index trigger-state]] 
              (trigger (assoc t :state-event (-> state-event
                                                 (assoc :log-type :trigger)
                                                 (assoc :trigger-index trigger-index)
                                                 (assoc :trigger-state trigger-state)))))
            this
            (map-indexed list trigger-states)))

  (extent-state [this]
    (let [{:keys [extent segment]} state-event
          extent-state (->> (get state extent)
                            (default-state-value init-fn window))
          transition-entry (create-state-update window extent-state segment)
          new-extent-state (apply-state-update window extent-state transition-entry)
          new-state-event (-> state-event
                              (assoc :next-extent-state new-extent-state)
                              (assoc :log-type :aggregation)
                              (assoc :aggregation-update transition-entry))]
      (assoc this 
             :state (assoc state extent new-extent-state)
             :event-results (conj event-results new-state-event))))
  
  (state [this]
    state)

  (log-entries [this]
    (doall (map state-event->log-entry event-results)))

  (apply-extents [this]
    (let [{:keys [segment]} state-event
          segment-coerced (we/uniform-units window-extension segment)
          state* (we/speculate-update window-extension state segment-coerced)
          state** (we/merge-extents window-extension state* super-agg-fn segment-coerced)
          extents (we/extents window-extension (keys state**) segment-coerced)]
      (-> this 
          (assoc :state state**)
          (assoc :state-event (assoc state-event :extents extents)))))

  (aggregate-state [this]
    (reduce (fn [t extent] 
              (extent-state (assoc t :state-event (assoc state-event :extent extent))))
            this
            (:extents state-event)))

  (apply-event [this]
    (if (= (:event-type state-event) :new-segment)
      (-> this 
          apply-extents
          aggregate-state
          triggers)
      (triggers this))))

(defn clean-windows-states 
  "Cleans window states of anything they no longer require after reduction 
  e.g. event maps, log entries"
  [windows-state]
  (mapv clean windows-state))

(defn fire-state-event [windows-state state-event]
  (mapv (fn [ws]
          (apply-event (assoc ws 
                              :state-event state-event
                              :state-results [])))
        windows-state))

(defn process-segment
  [{:keys [peer-replica-view acking-state grouping-fn monitoring messenger uniqueness-task? uniqueness-key] :as compiled}
   {:keys [task-event] :as state-event}]
  (let [{:keys [onyx.core/windows-state onyx.core/filter-state onyx.core/state-log onyx.core/results]} task-event
        grouped? (not (nil? grouping-fn))
        state-event* (assoc state-event :grouped? grouped?)
        start-time (System/currentTimeMillis)
        rs (doall
             (mapcat 
               (fn [leaf fused-ack]
                 (map 
                   (fn [message]
                     (let [segment (:message message)
                           state-event** (cond-> (assoc state-event* :segment segment)
                                           grouped? (assoc :group-key (grouping-fn segment)))
                           unique-id (if uniqueness-task? (get segment uniqueness-key))
                           process? (not (and uniqueness-task? 
                                              (state-extensions/filter? @filter-state task-event unique-id)))]
                       ;; Always update the filter, to freshen up the fact that the id has been re-seen
                       (when uniqueness-task? 
                         (swap! filter-state state-extensions/apply-filter-id task-event unique-id))
                       (if process?
                         (let [_ (st-ack/prepare acking-state unique-id fused-ack)
                               updated (swap! windows-state fire-state-event state-event**)
                               _ (swap! windows-state clean-windows-states)] 
                           (list (fn [] (st-ack/ack acking-state unique-id fused-ack))
                                 (list unique-id (doall (map log-entries updated)))))
                         (do
                           (st-ack/defer acking-state unique-id fused-ack)
                           (list (fn []))))))
                   (:leaves leaf)))
               (:tree results)
               (:acks results)))
        ack-fns (doall (map first rs))
        success-fn (fn [] 
                     (run! (fn [f] (f)) ack-fns)
                     (emit-latency-value :window-log-write-entry 
                                         monitoring 
                                         (- (System/currentTimeMillis) start-time)))
        log-entry (keep second rs)]
    (when-not (empty? log-entry)
      (state-extensions/store-log-entry state-log task-event success-fn log-entry))))

(defn process-event [compiled {:keys [task-event] :as state-event}]
  (let [{:keys [onyx.core/windows-state onyx.core/state-log]} task-event
        new-ws (swap! windows-state fire-state-event state-event)
        log-entry (remove empty? (map log-entries new-ws))]
    (when-not (empty? log-entry) 
      (state-extensions/store-log-entry state-log 
                                        task-event 
                                        (fn []) 
                                        ;; nil filter-id as this is not in response to a segment
                                        (list nil log-entry)))))

(defn process-state 
  [compiled {:keys [event-type task-event] :as state-event}]
  (if (= event-type :new-segment) 
    (process-segment compiled state-event)
    (process-event compiled state-event)))

(defn process-state-loop
  [{:keys [onyx.core/state-ch onyx.core/compiled
           onyx.core/peer-opts] :as event} ex-f]
  (try 
    (let [timer-resolution (arg-or-default :onyx.peer/trigger-timer-resolution peer-opts)] 
      (loop [timer-tick-ch (timeout timer-resolution)]
        (let [[[event-type task-event ack-batch] ch] (alts!! [timer-tick-ch state-ch] :priority true)] 
          (cond (= ch state-ch)
                (when event-type 
                  (lc/invoke-assign-windows process-state compiled (new-state-event event-type task-event))
                  ;; It's safe to ack the batch as it has been processed by the process event loop,
                  ;; which has its own acking infrastructure
                  (ack-batch)
                  (recur timer-tick-ch))

                (= ch timer-tick-ch)
                (do 
                  (lc/invoke-assign-windows process-state compiled (new-state-event :timer-tick event))
                  (recur (timeout timer-resolution)))))))
    (catch Throwable t
      (ex-f t)
      (error t "Error in process state loop."))))
