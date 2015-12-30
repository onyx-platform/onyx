(ns ^:no-doc onyx.peer.task-compile
  (:require [clojure.set :refer [subset?]]
            [onyx.peer.operation :refer [kw->fn] :as operation]
            [onyx.static.planning :refer [find-task]]
            [onyx.static.validation :as validation]
            [onyx.flow-conditions.fc-compile :refer [build-pred-fn]]
            [onyx.windowing.aggregation :as agg]
            [onyx.windowing.window-extensions :as w]
            [onyx.state.state-extensions :as state-extensions]
            [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [clj-tuple :as t]))

(defn egress-tasks [workflow task]
  (map second (filter #(= (first %) task) workflow)))

(defn only-relevant-branches [flow-conditions workflow task]
  (filter #(or (= (:flow/from %) task)
               (and (= (:flow/from %) :all)
                    (subset? (into #{} (:flow/to %))
                             (into #{} (egress-tasks workflow task)))))
          flow-conditions))

(defn compile-flow-conditions [flow-conditions workflow task-name f]
  (let [branches (only-relevant-branches flow-conditions workflow task-name)
        conditions (filter f branches)]
    (map
     (fn [condition]
       (assoc condition :flow/predicate (build-pred-fn (:flow/predicate condition) condition)))
     conditions)))

(defn compile-fc-happy-path [flow-conditions workflow task-name]
  (compile-flow-conditions flow-conditions workflow task-name
                           (comp not :flow/thrown-exception?)))

(defn compile-fc-exception-path [flow-conditions workflow task-name]
  (compile-flow-conditions flow-conditions workflow task-name
                           :flow/thrown-exception?))

(defn resolve-lifecycle-calls [calls]
  (let [calls-map (var-get (kw->fn calls))]
    (try
      (validation/validate-lifecycle-calls calls-map)
      (catch Throwable t
        (let [e (ex-info (str "Error validating lifecycle map. " (.getCause t)) calls-map )]
          (error e)
          (throw e))))
    calls-map))

(defn select-applicable-lifecycles [lifecycles task-name]
  (filter #(or (= (:lifecycle/task %) :all)
               (= (:lifecycle/task %) task-name)) lifecycles))

(defn compile-start-task-functions [lifecycles task-name]
  (let [matched (select-applicable-lifecycles lifecycles task-name)
        fs
        (remove
         nil?
         (map
          (fn [lifecycle]
            (let [calls-map (resolve-lifecycle-calls (:lifecycle/calls lifecycle))]
              (when-let [g (:lifecycle/start-task? calls-map)]
                (fn [x] (g x lifecycle)))))
          matched))]
    (fn [event]
      (if (seq fs)
        (every? true? ((apply juxt fs) event))
        true))))

(defn compile-lifecycle-functions [lifecycles task-name kw]
  (let [matched (select-applicable-lifecycles lifecycles task-name)]
    (reduce
     (fn [f lifecycle]
       (let [calls-map (resolve-lifecycle-calls (:lifecycle/calls lifecycle))]
         (if-let [g (get calls-map kw)]
           (comp (fn [x] (merge x (g x lifecycle))) f)
           f)))
     identity
     matched)))

(defn compile-ack-retry-lifecycle-functions [lifecycles task-name kw]
  (let [matched (select-applicable-lifecycles lifecycles task-name)
        fns (keep (fn [lifecycle]
                    (let [calls-map (resolve-lifecycle-calls (:lifecycle/calls lifecycle))]
                      (if-let [g (get calls-map kw)]
                        (vector lifecycle g))))
                  matched)]
    (reduce (fn [g [lifecycle f]]
              (fn [event message-id rets]
                (g event message-id rets)
                (f event message-id rets lifecycle)))
            (fn [event message-id rets])
            fns)))

(defn compile-before-task-start-functions [lifecycles task-name]
  (compile-lifecycle-functions lifecycles task-name :lifecycle/before-task-start))

(defn compile-before-batch-task-functions [lifecycles task-name]
  (compile-lifecycle-functions lifecycles task-name :lifecycle/before-batch))

(defn compile-after-read-batch-task-functions [lifecycles task-name]
  (compile-lifecycle-functions lifecycles task-name :lifecycle/after-read-batch))

(defn compile-after-batch-task-functions [lifecycles task-name]
  (compile-lifecycle-functions lifecycles task-name :lifecycle/after-batch))

(defn compile-after-task-functions [lifecycles task-name]
  (compile-lifecycle-functions lifecycles task-name :lifecycle/after-task-stop))

(defn compile-after-ack-segment-functions [lifecycles task-name]
  (compile-ack-retry-lifecycle-functions lifecycles task-name :lifecycle/after-ack-segment))

(defn compile-after-retry-segment-functions [lifecycles task-name]
  (compile-ack-retry-lifecycle-functions lifecycles task-name :lifecycle/after-retry-segment))

(defn task-map->grouping-fn [task-map]
  (if-let [group-key (:onyx/group-by-key task-map)]
    (cond (keyword? group-key)
          group-key
          (sequential? group-key)
          #(select-keys % group-key)
          :else
          #(get % group-key))
    (if-let [group-fn (:onyx/group-by-fn task-map)]
      (operation/resolve-fn {:onyx/fn (:onyx/group-by-fn task-map)}))))

(defn compile-grouping-fn
  "Compiles outgoing grouping task info into a task->group-fn map
  for quick lookup and group fn calls"
  [catalog egress-ids]
  (->> catalog
       (map (juxt :onyx/name task-map->grouping-fn))
       (filter (fn [[n f]]
                 (and f egress-ids (egress-ids n))))
       (into (t/hash-map))))

(defn filter-windows [windows task]
  (filter #(= (:window/task %) task) windows))

(defn filter-triggers [triggers windows]
  (filter #(some #{(:trigger/window-id %)}
                 (map :window/id windows))
          triggers))

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

(defn resolve-triggers [triggers]
  (map
   #(assoc %
      :trigger/id (java.util.UUID/randomUUID)
      :trigger/sync-fn (kw->fn (:trigger/sync %)))
   triggers))

(defn compacted-reset? [entry]
  (and (map? entry)
       (= (:type entry) :compacted)))

(defn unpack-compacted [state {:keys [filter-snapshot extent-state]} event]
  (-> state
      (assoc :state extent-state)
      (update :filter state-extensions/restore-filter event filter-snapshot)))

(defn compile-apply-window-entry-fn [{:keys [onyx.core/task-map onyx.core/windows] :as event}]
  (let [grouped-task? (operation/grouped-task? task-map)
        id->apply-state-update (into {}
                                     (map (juxt :window/id :aggregate/apply-state-update)
                                          windows))
        extents-fn (fn [state entry] 
                     (reduce (fn [state' [window-entries {:keys [window/id] :as window}]]
                               (reduce (fn [state'' [extent entry grp-key]]
                                         (let [state'''
                                               (update-in state'' 
                                                          [:state id extent]
                                                          (fn [ext-state] 
                                                            (let [state-value (-> (if grouped-task? (get ext-state grp-key) ext-state)
                                                                                  (agg/default-state-value window))
                                                                  apply-fn (id->apply-state-update id)
                                                                  _ (assert apply-fn (str "Apply fn does not exist for window-id " id))
                                                                  new-state-value (apply-fn state-value entry)] 
                                                              (if grouped-task?
                                                                (assoc ext-state grp-key new-state-value)
                                                                new-state-value))))]
                                           ;; Destructive triggers turn the state to nil,
                                           ;; prune these out of the window state to avoid
                                           ;; inflating memory consumption.
                                           (if (nil? (get (get (:state state''') id) extent))
                                             (update-in state''' [:state id] dissoc extent)
                                             state''')))
                                       state'
                                       window-entries))
                             state
                             (map list (rest entry) windows)))]
    (fn [state entry]
      (if (compacted-reset? entry)
        (unpack-compacted state entry event)
        (let [unique-id (first entry)
              _ (trace "Playing back entries for segment with id:" unique-id)
              new-state (extents-fn state entry)]
          (if unique-id
            (update new-state :filter state-extensions/apply-filter-id event unique-id)
            new-state))))))

(defn flow-conditions->event-map [event flow-conditions task-name]
  (let [workflow (:onyx.core/workflow event)]
    (-> event
        (assoc :onyx.core/compiled-norm-fcs
               (compile-fc-happy-path flow-conditions workflow task-name))
        (assoc :onyx.core/compiled-ex-fcs
               (compile-fc-exception-path flow-conditions workflow task-name)))))

(defn lifecycles->event-map [event lifecycles task-name]
  (-> event
      (assoc :onyx.core/compiled-start-task-fn
             (compile-start-task-functions lifecycles task-name))
      (assoc :onyx.core/compiled-before-task-start-fn
             (compile-before-task-start-functions lifecycles task-name))
      (assoc :onyx.core/compiled-before-batch-fn
             (compile-before-batch-task-functions lifecycles task-name))
      (assoc :onyx.core/compiled-after-read-batch-fn
             (compile-after-read-batch-task-functions lifecycles task-name))
      (assoc :onyx.core/compiled-after-batch-fn
             (compile-after-batch-task-functions lifecycles task-name))
      (assoc :onyx.core/compiled-after-task-fn
             (compile-after-task-functions lifecycles task-name))
      (assoc :onyx.core/compiled-after-ack-segment-fn
             (compile-after-ack-segment-functions lifecycles task-name))
      (assoc :onyx.core/compiled-after-retry-segment-fn
             (compile-after-retry-segment-functions lifecycles task-name))))
