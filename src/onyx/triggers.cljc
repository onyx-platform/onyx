(ns onyx.triggers
  (:require [onyx.windowing.units :refer [coerce-key to-standard-units standard-units-for]]
            [onyx.static.util :refer [kw->fn now ms->ns]]))

;;; State helper functions
(defn next-fire-time
  [{:keys [trigger/period] :as trigger}]
  (if (= (standard-units-for (second period)) :milliseconds)
    (let [ms (apply to-standard-units period)]
      ;; use monotonically increasing clock for Java
      #?(:clj (+ (System/nanoTime) (ms->ns ms)))
      ;; cljs clock is susceptible to time changes
      #?(:cljs (+ (now) ms)))
    (throw (ex-info ":trigger/period must be a unit that can be converted to :milliseconds" 
                    {:trigger trigger}))))

(defn exceeds-watermark? [window upper-extent-bound segment]
  (let [watermark (get segment (:window/window-key window))]
    (>= (coerce-key watermark :milliseconds) upper-extent-bound)))

(defn exceeds-percentile-watermark? [window trigger lower-extent-bound upper-extent-bound segment]
  (let [watermark (get segment (:window/window-key window))
        pct (:trigger/watermark-percentage trigger)
        offset (* (- upper-extent-bound lower-extent-bound) pct)]
    (>= (coerce-key watermark :milliseconds) (+ lower-extent-bound offset))))

;;; State initialization functions

(defn segment-init-state [_]
  0)

(defn timer-init-state
  [trigger]
  [false (next-fire-time trigger)])

(defn punctuation-init-state
  [trigger]
  {:fire? false})

;; Init local functions

(defn segment-init-locals [trigger]
  {})

(defn timer-init-locals [trigger]
  {})

(defn punctuation-init-locals [trigger]
  {:pred-fn (kw->fn (:trigger/pred trigger))})

(defn percentile-watermark-init-locals [trigger]
  {})

;;; State transition functions

(defn segment-next-state
  [{:keys [trigger/threshold]} state {:keys [event-type] :as state-event}]
  (if (= event-type :new-segment)
    (inc (mod state (first threshold)))
    state))

(defn timer-next-state
  [{:keys [trigger/period] :as trigger}
   [_ fire-time]
   {:keys [event-type] :as state-event}]
  (let [fire? (or (> #?(:clj (System/nanoTime))
                     #?(:cljs (now))
                     fire-time)
                  (boolean (#{:job-completed :recovered} event-type)))]
    [fire? (if fire? (next-fire-time trigger) fire-time)]))

(defn punctuation-next-state
  [trigger state {:keys [trigger-state] :as state-event}]
  (let [{:keys [pred-fn]} trigger-state]
    {:fire? (pred-fn trigger state-event)}))

;;; Fire predicate functions
(defn segment-fire?
  [{:keys [trigger/threshold] :as trigger}
   trigger-state
   {:keys [event-type] :as state-event}]
  (or (and (= event-type :new-segment)
           (= trigger-state (first threshold)))
      (#{:job-completed :recovered} event-type)))

(defn timer-fire?
  [trigger [fire? _] state-event]
  fire?)

(defn punctuation-fire?
  [trigger state state-event]
  (:fire? state))

(defn watermark-init-locals [{:keys [trigger/delay]}]
  {:delay (if delay (apply to-standard-units delay) 0)})

(defn watermark-fire?
  [_ _ {:keys [event-type upper-bound watermarks trigger-state] :as state-event}]
  (or (= :job-completed event-type) 
      (and (= :watermark event-type)
           (> (:input watermarks) (+ upper-bound (:delay trigger-state))))))

(defn percentile-watermark-fire?
  [trigger _ {:keys [lower-bound upper-bound event-type segment window]}]
  ;; If this was stimulated by a new segment, check if it should fire.
  ;; Otherwise if this was a completed task, always fire.
  (or (and segment (exceeds-percentile-watermark? window trigger lower-bound upper-bound segment))
      (#{:job-completed :recovered} event-type)))

;;; Top level vars to bundle the functions together
(def ^:export segment
  {:trigger/init-state segment-init-state
   :trigger/init-locals segment-init-locals
   :trigger/next-state segment-next-state
   :trigger/trigger-fire? segment-fire?})

(def ^:export timer
  {:trigger/init-state timer-init-state
   :trigger/init-locals timer-init-locals
   :trigger/next-state timer-next-state
   :trigger/trigger-fire? timer-fire?})

(def ^:export punctuation
  {:trigger/init-state punctuation-init-state
   :trigger/init-locals punctuation-init-locals
   :trigger/next-state punctuation-next-state
   :trigger/trigger-fire? punctuation-fire?})

(def ^:export watermark
  {:trigger/init-locals watermark-init-locals
   :trigger/trigger-fire? watermark-fire?})

(def ^:export percentile-watermark
  {:trigger/init-locals percentile-watermark-init-locals
   :trigger/trigger-fire? percentile-watermark-fire?})
