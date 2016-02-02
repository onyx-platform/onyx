(ns ^:no-doc onyx.windowing.window-compile
  (:require [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [onyx.static.validation :as validation]
            [onyx.windowing.window-extensions :as w]
            [onyx.windowing.aggregation :as a]
            [onyx.state.state-extensions :as s]
            [onyx.peer.operation :refer [kw->fn]]
            [onyx.peer.grouping :as g]))

(defn filter-windows [windows task]
  (filter #(= (:window/task %) task) windows))

(defn compacted-reset? [entry]
  (and (map? entry)
       (= (:type entry) :compacted)))

(defn unpack-compacted [state {:keys [filter-snapshot extent-state]} event]
  (-> state
      (assoc :state extent-state)
      (update :filter s/restore-filter event filter-snapshot)))

(defn resolve-window-init [window calls]
  (if-not (:aggregation/init calls)
    (let [init (:window/init window)]
      (when-not init
        (throw (ex-info "No :window/init supplied, this is required for this aggregation" {:window window})))
      (constantly (:window/init window)))
    (:aggregation/init calls)))

(defn resolve-windows [windows]
  (map
   (fn [window]
     (let [agg (:window/aggregation window)
           agg-var (if (sequential? agg) (first agg) agg)
           calls (var-get (kw->fn agg-var))]
       (validation/validate-state-aggregation-calls calls)
       (assoc window
              :aggregate/record (w/windowing-record window)
              :aggregate/init (resolve-window-init window calls)
              :aggregate/fn (:aggregation/fn calls)
              :aggregate/super-agg-fn (:aggregation/super-aggregation-fn calls)
              :aggregate/apply-state-update (:aggregation/apply-state-update calls))))
   windows))

(defn compile-apply-window-entry-fn [{:keys [onyx.core/task-map onyx.core/windows] :as event}]
  (let [grouped-task? (g/grouped-task? task-map)
        get-state-fn (if grouped-task? 
                       (fn [ext-state grp-key] 
                         (get ext-state grp-key)) 
                       (fn [ext-state grp-key] 
                         ext-state))
        set-state-fn (if grouped-task?
                       (fn [ext-state grp-key new-value] 
                         (assoc ext-state grp-key new-value))
                       (fn [ext-state grp-key new-value] 
                         new-value))
        apply-window-entries 
        (fn [state [window-entries {:keys [window/id aggregate/apply-state-update] :as window}]]
          (reduce (fn [state* [extent extent-entry grp-key]]
                    (if (nil? extent-entry)
                      ;; Destructive triggers turn the state to nil,
                      ;; prune these out of the window state to avoid
                      ;; inflating memory consumption.
                      (update-in state* [:state id] dissoc extent)
                      (update-in state* 
                                 [:state id extent]
                                 (fn [ext-state] 
                                   (let [state-value (a/default-state-value (get-state-fn ext-state grp-key) window)
                                         new-state-value (apply-state-update state-value extent-entry)] 
                                     (set-state-fn ext-state grp-key new-state-value))))))
                  state
                  window-entries))
        extents-fn (fn [state log-entry] 
                     (reduce apply-window-entries 
                             state 
                             (map list (rest log-entry) windows)))]
    (fn [state entry]
      ;if (compacted-reset? entry)
      ;  (unpack-compacted state entry event)
        (let [unique-id (first entry)
              _ (trace "Playing back entries for segment with id:" unique-id)
              new-state (extents-fn state entry)]
          (if unique-id
            (update new-state :filter s/apply-filter-id event unique-id)
            new-state)))))
