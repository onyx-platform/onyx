(ns ^:no-doc onyx.peer.window-state
    (:require [onyx.state.protocol.db :as st]
              [onyx.windowing.window-extensions :as we]
      #?(:clj [onyx.protocol.task-state :as ts])
              [onyx.types :as t]
              [onyx.static.util :as u]))

(defn default-state-value 
  [init-fn window state-value]
  (or state-value (init-fn window)))

(defprotocol StateEventReducer
  (window-id [this])
  (trigger-extent! [this state-event trigger-record extent])
  (trigger [this state-event trigger-record])
  (segment-triggers! [this state-event])
  (all-triggers! [this state-event])
  (apply-extents [this state-event])
  (apply-event [this state-event]))

(defn rollup-result [segment]
  (cond (sequential? segment) 
        segment 
        (map? segment)
        (list segment)
        :else
        (throw (ex-info "Value returned by :trigger/emit must be either a hash-map or a sequential of hash-maps." 
                        {:value segment}))))

(defn trigger-state-index-id [{:keys [trigger/id trigger/window-id]}]
  [id window-id])

(defn state-indices 
  ([{:keys [onyx.core/windows onyx.core/triggers]}]
   (state-indices windows triggers))
  ([windows triggers]
   (assert (= 1 (count (distinct (map :window/task windows)))))
   #?(:clj (into {} 
                 (map vector 
                      (into (vec (sort (map :window/id windows)))
                            (sort (map trigger-state-index-id triggers))) 
                      (map short (range Short/MIN_VALUE Short/MAX_VALUE))))
      :cljs (into {}
                  (map (juxt identity identity)
                       (into (vec (sort (map :window/id windows)))
                             (sort (map  (fn [{:keys [trigger/id trigger/window-id]}]
                                           [id window-id])
                                        triggers))))))))

(defrecord WindowExecutor [window-extension grouped? triggers window id idx state-store 
                           init-fn emitted create-state-update apply-state-update super-agg-fn event-results]
  StateEventReducer
  (window-id [this]
    (:window/id window))

  (trigger-extent! [this state-event trigger-record extent]
    (let [{:keys [sync-fn emit-fn trigger create-state-update apply-state-update]} trigger-record
          group-id (:group-id state-event)
          extent-state (st/get-extent state-store idx group-id extent)
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
      (st/put-extent! state-store idx group-id extent new-extent-state)))

  (trigger [this state-event trigger-record]
    (let [{:keys [trigger trigger-fire? fire-all-extents?]} trigger-record 
          state-event (-> state-event 
                          (assoc :window window) 
                          (assoc :trigger-state trigger-record))
          group-id (:group-id state-event)
          trigger-idx (:idx trigger-record)
          trigger-state (st/get-trigger state-store trigger-idx group-id)
          ;; TODO, should check if key not found, not that the value was nil, as nil may be a valid trigger state
          trigger-state (if (nil? trigger-state)
                          ((:init-state trigger-record) trigger)
                          trigger-state)
          next-trigger-state-fn (:next-trigger-state trigger-record)
          new-trigger-state (next-trigger-state-fn trigger trigger-state state-event)
          fire-all? (or fire-all-extents? (not= (:event-type state-event) :segment))
          fire-extents (if fire-all? 
                         (st/group-extents state-store idx group-id)
                         (:extents state-event))]
      (st/put-trigger! state-store trigger-idx group-id new-trigger-state)
      (run! (fn [extent] 
              (let [[lower upper] (we/bounds window-extension extent)
                    state-event (-> state-event
                                    (assoc :lower-bound lower)
                                    (assoc :upper-bound upper))]
                (when (trigger-fire? trigger new-trigger-state state-event)
                  (trigger-extent! this state-event trigger-record extent))))
            fire-extents)
      this))

  (segment-triggers! [this state-event]
    (run! (fn [[_ trigger-state]] 
            (trigger this state-event trigger-state))
          triggers)
    state-event)

  (all-triggers! [this state-event]
    (run! (fn [[trigger-idx group-bytes group-key]] 
            ;; FIXME: following when-let is a hacky workaround to work around the fact
            ;; that we are not just retrieving our triggers, but are instead retrieving ALL triggers
            ;; st/trigger-keys should take a trigger-idx, and return only the keys related to that trigger
            (when-let [record (get triggers trigger-idx)] 
              (trigger this
                       (-> state-event
                           (assoc :group-id group-bytes)
                           (assoc :group-key group-key))
                       record)))
          (st/trigger-keys state-store))
    state-event)

  (apply-extents [this state-event]
    (let [{:keys [segment group-id]} state-event
          segment-time (we/segment-time window-extension segment)
          operations (we/extent-operations window-extension 
                                           (st/group-extents state-store idx group-id)
                                           segment
                                           segment-time)
          updated-extents (distinct (map second (filter (fn [[op]] (= op :update)) operations)))]
      (run! (fn [[action :as args]] 
              (case action
                :update (let [extent (second args)
                              extent-state (st/get-extent state-store idx group-id extent)
                              extent-state (default-state-value init-fn window extent-state)
                              transition-entry (create-state-update window extent-state segment)
                              new-extent-state (apply-state-update window extent-state transition-entry)]
                          (st/put-extent! state-store idx group-id extent new-extent-state))

                :merge-extents 
                (let [[_ extent-1 extent-2 extent-merged] args
                      agg-merged (super-agg-fn window-extension
                                               (st/get-extent state-store idx group-id extent-1)
                                               (st/get-extent state-store idx group-id extent-2))]
                  (st/put-extent! state-store idx group-id extent-merged agg-merged)
                  (st/delete-extent! state-store idx group-id extent-1)
                  (st/delete-extent! state-store idx group-id extent-2))

                :alter-extents (let [[_ from-extent to-extent] args
                                     v (st/get-extent state-store idx group-id from-extent)]
                                 (st/put-extent! state-store idx group-id to-extent v)
                                 (st/delete-extent! state-store idx group-id from-extent))))
            operations)
      (assoc state-event :extents updated-extents)))

  (apply-event [this state-event]
    (if (= (:event-type state-event) :new-segment)
      (->> state-event
           (apply-extents this)
           (segment-triggers! this))
      (all-triggers! this state-event))
    this))

(defn fire-state-event [windows-state state-event]
  (mapv (fn [ws]
          (apply-event ws state-event))
        windows-state))

#?(:clj 
   (defn process-segment
     [state state-event]
     (let [{:keys [grouping-fn onyx.core/results] :as event} (ts/get-event state)
           state-store (ts/get-state-store state)
           grouped? (not (nil? grouping-fn))
           state-event* (assoc state-event :grouped? grouped?)
           windows-state (ts/get-windows-state state)
           updated-states (reduce 
                           (fn [windows-state* segment]
                             (if (u/exception? segment)
                               windows-state*
                               (let [state-event** (if grouped?
                                                     (let [group-key (grouping-fn segment)]
                                                       (-> state-event* 
                                                           (assoc :segment segment)
                                                           (assoc :group-id (st/group-id state-store group-key))
                                                           (assoc :group-key group-key)))
                                                     (assoc state-event* :segment segment))]
                                 (fire-state-event windows-state* state-event**))))
                           windows-state
                           (mapcat :leaves (:tree results)))
           emitted (doall (mapcat (comp deref :emitted) updated-states))]
       (run! (fn [w] (reset! (:emitted w) [])) windows-state)
       (-> state 
           (ts/set-windows-state! updated-states)
           (ts/update-event! (fn [e] (update e :onyx.core/triggered into emitted)))))))

#?(:clj 
   (defn process-event [state state-event]
     (ts/set-windows-state! state (fire-state-event (ts/get-windows-state state) state-event))))

#?(:clj (defn assign-windows [state event-type]
          (let [state-event (t/new-state-event event-type (ts/get-event state))] 
            (if (= :new-segment event-type)
              (process-segment state state-event)
              (process-event state state-event)))))
