(ns onyx.triggers
  (:require [onyx.windowing.units :refer [coerce-key to-standard-units standard-units-for]]
            [onyx.peer.operation :refer [kw->fn]]
            [onyx.schema :refer [Trigger Window Event Function StateEvent]]
            [schema.core :as s]
            [taoensso.timbre :refer [fatal info]]))

;;; State helper functions

(s/defn next-fire-time 
  [{:keys [trigger/period] :as trigger} :- Trigger]
  (if (= (standard-units-for (second period)) :milliseconds)
    (let [ms (apply to-standard-units period)]
      (+ (System/currentTimeMillis) ms))
    (throw (ex-info ":trigger/period must be a unit that can be converted to :milliseconds" {:trigger trigger}))))

(def TimerState
  {:fire-time s/Int :fire? s/Bool})

(defn exceeds-watermark? [window upper-extent-bound segment]
  (let [watermark (get segment (:window/window-key window))]
    (>= (coerce-key watermark :milliseconds) upper-extent-bound)))

(defn exceeds-percentile-watermark? [window trigger lower-extent-bound upper-extent-bound segment]
  (let [watermark (get segment (:window/window-key window))
        pct (:trigger/watermark-percentage trigger)
        offset (* (- upper-extent-bound lower-extent-bound) pct)]
    (>= (coerce-key watermark :milliseconds) (+ lower-extent-bound offset))))

;;; State initialization functions

(def segment-init-state (constantly 0))

(s/defn timer-init-state 
  [trigger :- Trigger]
  {:fire? false :fire-time (next-fire-time trigger)})

(s/defn punctuation-init-state 
  [{:keys [trigger/pred] :as trigger} :- Trigger]
  {:pred-fn (kw->fn pred) :fire? false})

(s/defn watermark-init-state
  [trigger :- Trigger]
  ;; Intentionally return nil - this trigger is stateless.
  )

(s/defn percentile-watermark-init-state
  [trigger :- Trigger]
  ;; Intentionally return nil - this trigger is stateless.
  )

;;; State transition functions

(s/defn segment-next-state 
  [{:keys [trigger/threshold] :as trigger}
   state :- s/Int 
   {:keys [event-type] :as state-event} :- StateEvent]
  (if (= event-type :new-segment)
    (inc (mod state (first threshold)))
    state))

(s/defn timer-next-state 
  [{:keys [trigger/period] :as trigger} :- Trigger 
   {:keys [fire-time] :as state} :- TimerState
   {:keys [event-type] :as state-event} :- StateEvent]
  (let [fire? (or (> (System/currentTimeMillis) fire-time)
                  (boolean (#{:job-completed} event-type)))] 
    {:fire? fire?
     :fire-time (if fire? (next-fire-time trigger) fire-time)}))

(s/defn punctuation-next-state
  [trigger :- Trigger {:keys [pred-fn]} state-event :- StateEvent]
  {:pred-fn pred-fn :fire? (pred-fn trigger state-event)})

(s/defn watermark-next-state
  [trigger :- Trigger
   state
   {:keys [event-type] :as state-event} :- StateEvent]
  ;; Intentionally return nil - this trigger is stateless.
  )

(s/defn percentile-watermark-next-state
  [trigger :- Trigger
   state
   state-event :- StateEvent]
  ;; Intentionally return nil - this trigger is stateless.
  )

;;; Fire predicate functions

(s/defn segment-fire?
  [{:keys [trigger/threshold] :as trigger} :- Trigger 
   trigger-state :- s/Int 
   {:keys [event-type] :as state-event} :- StateEvent]
  (or (and (= event-type :new-segment) 
           (= trigger-state (first threshold)))
      (= event-type :job-completed)))

(s/defn timer-fire?
  [{:keys [trigger/period]} :- Trigger state :- TimerState {:keys [event-type] :as state-event} :- StateEvent]
  (:fire? state))

(s/defn punctuation-fire?
  [trigger :- Trigger trigger-state state-event :- StateEvent]
  (:fire? trigger-state))

(s/defn watermark-fire?
  [trigger :- Trigger trigger-state {:keys [upper-bound event-type segment window] :as state-event} :- StateEvent]
  ;; If this was stimulated by a new segment, check if it should fire.
  ;; Otherwise if this was a completed task, always fire.
  (or (and segment (exceeds-watermark? window upper-bound segment))
      (= event-type :job-completed)))

(s/defn percentile-watermark-fire? 
  [trigger :- Trigger trigger-state {:keys [lower-bound upper-bound event-type segment window]} :- StateEvent]
  ;; If this was stimulated by a new segment, check if it should fire.
  ;; Otherwise if this was a completed task, always fire.
  (or (and segment (exceeds-percentile-watermark? window trigger lower-bound upper-bound segment))
      (= event-type :job-completed)))

;;; Top level vars to bundle the functions together
(def segment
  {:trigger/init-state segment-init-state
   :trigger/next-state segment-next-state 
   :trigger/trigger-fire? segment-fire?})

(def timer
  {:trigger/init-state timer-init-state
   :trigger/next-state timer-next-state 
   :trigger/trigger-fire? timer-fire?})

(def punctuation
  {:trigger/init-state punctuation-init-state
   :trigger/next-state punctuation-next-state 
   :trigger/trigger-fire? punctuation-fire?})

(def watermark
  {:trigger/init-state watermark-init-state
   :trigger/next-state watermark-next-state
   :trigger/trigger-fire? watermark-fire?})

(def percentile-watermark
  {:trigger/init-state percentile-watermark-init-state
   :trigger/next-state percentile-watermark-next-state
   :trigger/trigger-fire? percentile-watermark-fire?})
