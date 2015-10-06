(ns ^:no-doc onyx.peer.task-compile
  (:require [onyx.peer.operation :refer [kw->fn] :as operation]
            [onyx.static.planning :refer [find-task build-pred-fn]]
            [onyx.static.validation :as validation]
            [onyx.windowing.aggregation :as agg]
            [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [clj-tuple :as t]))

(defn only-relevant-branches [flow task]
  (filter #(= (:flow/from %) task) flow))

(defn compile-flow-conditions [flow-conditions task-name f]
  (let [conditions (filter f (only-relevant-branches flow-conditions task-name))]
    (map
     (fn [condition]
       (assoc condition :flow/predicate (build-pred-fn (:flow/predicate condition) condition)))
     conditions)))

(defn compile-fc-norms [flow-conditions task-name]
  (compile-flow-conditions flow-conditions task-name (comp not :flow/thrown-exception?)))

(defn compile-fc-exs [flow-conditions task-name]
  (compile-flow-conditions flow-conditions task-name :flow/thrown-exception?))

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

(defn compile-grouping-fn
  "Compiles grouping outgoing grouping task info into a task->group-fn map
  for quick lookup and group fn calls"
  [catalog egress-ids]
  (merge (->> catalog
              (filter (fn [entry]
                        (and (:onyx/group-by-key entry)
                             egress-ids
                             (egress-ids (:onyx/name entry)))))
              (map (fn [entry]
                     (let [group-key (:onyx/group-by-key entry)
                           group-fn (cond (keyword? group-key)
                                          group-key
                                          (sequential? group-key)
                                          #(select-keys % group-key)
                                          :else
                                          #(get % group-key))]
                       [(:onyx/name entry) group-fn])))
              (into (t/hash-map)))
         (->> catalog
              (filter (fn [entry]
                        (and (:onyx/group-by-fn entry)
                             egress-ids
                             (egress-ids (:onyx/name entry)))))
              (map (fn [entry]
                     [(:onyx/name entry)
                      (operation/resolve-fn {:onyx/fn (:onyx/group-by-fn entry)})]))
              (into (t/hash-map)))))

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
       (assoc window
              :window/agg-init (resolve-window-init window calls)
              :window/agg-fn (:aggregation/fn calls)
              :window/log-resolve (:aggregation/log-resolve calls))))
   windows))

(defn resolve-triggers [triggers]
  (map
   #(assoc %
      :trigger/id (java.util.UUID/randomUUID)
      :trigger/sync-fn (kw->fn (:trigger/sync %)))
   triggers))
