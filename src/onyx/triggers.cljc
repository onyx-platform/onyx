(ns onyx.triggers
  (:require [onyx.windowing.units :refer [coerce-key to-standard-units standard-units-for]]
            [onyx.static.util :refer [kw->fn now]]))

;;; State helper functions

(defn next-fire-time 
  [{:keys [trigger/period] :as trigger}]
  (if (= (standard-units-for (second period)) :milliseconds)
    (let [ms (apply to-standard-units period)]
      (+ (now) ms))
    (throw (ex-info ":trigger/period must be a unit that can be converted to :milliseconds" {:trigger trigger}))))

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

(defn timer-init-state 
  [trigger]
  {:fire? false :fire-time (next-fire-time trigger)})

(defn punctuation-init-state 
  [{:keys [trigger/pred] :as trigger}]
  {:pred-fn (kw->fn pred) :fire? false})

(defn watermark-init-state
  [trigger]
  ;; Intentionally return nil - this trigger is stateless.
  )

(defn percentile-watermark-init-state
  [trigger]
  ;; Intentionally return nil - this trigger is stateless.
  )

;;; State transition functions

(defn segment-next-state 
  [{:keys [trigger/threshold]} state {:keys [event-type]}]
  (if (= event-type :new-segment)
    (inc (mod state (first threshold)))
    state))

(defn timer-next-state 
  [{:keys [trigger/period] :as trigger}
   {:keys [fire-time] :as state}
   {:keys [event-type] :as state-event}]
  (let [fire? (or (> (System/currentTimeMillis) fire-time)
                  (boolean (#{:job-completed :recovered} event-type)))] 
    {:fire? fire?
     :fire-time (if fire? (next-fire-time trigger) fire-time)}))

(defn punctuation-next-state
  [trigger {:keys [pred-fn]} state-event]
  {:pred-fn pred-fn :fire? (pred-fn trigger state-event)})

(defn watermark-next-state
  [trigger state state-event]
  ;; Intentionally return nil - this trigger is stateless.
  )

(defn percentile-watermark-next-state
  [trigger state state-event]
  ;; Intentionally return nil - this trigger is stateless.
  )

;;; Fire predicate functions
(defn segment-fire?
  [{:keys [trigger/threshold] :as trigger}
   trigger-state
   {:keys [event-type] :as state-event}]
  (or (and (= event-type :new-segment) 
           (= trigger-state (first threshold)))
      (#{:job-completed :recovered} event-type)))

(defn timer-fire?
  [trigger state state-event]
  (:fire? state))

(defn punctuation-fire?
  [trigger state state-event]
  (:fire? state))

(defn watermark-fire?
  [trigger trigger-state {:keys [upper-bound event-type segment window] :as state-event}]
  ;; If this was stimulated by a new segment, check if it should fire.
  ;; Otherwise if this was a completed task, always fire.
  (or (and segment (exceeds-watermark? window upper-bound segment))
      (#{:job-completed :recovered} event-type)))

(defn percentile-watermark-fire? 
  [trigger trigger-state {:keys [lower-bound upper-bound event-type segment window]}]
  ;; If this was stimulated by a new segment, check if it should fire.
  ;; Otherwise if this was a completed task, always fire.
  (or (and segment (exceeds-percentile-watermark? window trigger lower-bound upper-bound segment))
      (#{:job-completed :recovered} event-type)))

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
