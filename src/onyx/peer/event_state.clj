(ns ^:no-doc onyx.peer.event-state
  (:require [onyx.messaging.messenger-replica :as ms]
            [taoensso.timbre :refer [info error warn trace fatal]]
            [onyx.windowing.window-compile :as wc]
            [onyx.peer.window-state :as ws]
            [onyx.plugin.onyx-input :as oi]
            [onyx.extensions :as extensions]))

(defn required-input-checkpoints [replica job-id]
  (let [recover-tasks (set (get-in replica [:input-tasks job-id]))] 
    (->> (get-in replica [:task-slot-ids job-id])
         (filter (fn [[task-id _]] (get recover-tasks task-id)))
         (mapcat (fn [[task-id peer->slot]]
                   (map (fn [[_ slot-id]]
                          [task-id slot-id :input])
                        peer->slot)))
         set)))

(defn required-state-checkpoints [replica job-id]
  (let [recover-tasks (set (get-in replica [:state-tasks job-id]))] 
    (->> (get-in replica [:task-slot-ids job-id])
         (filter (fn [[task-id _]] (get recover-tasks task-id)))
         (mapcat (fn [[task-id peer->slot]]
                   (map (fn [[_ slot-id]]
                          [task-id slot-id :state])
                        peer->slot)))
         set)))

(defn max-completed-checkpoints [{:keys [log job-id checkpoints] :as event} replica]
  (let [required (clojure.set/union (required-input-checkpoints replica job-id)
                                    (required-state-checkpoints replica job-id))] 
    (->> (extensions/read-checkpoints log job-id)
         (filter (fn [[k v]]
                   (= required (set (keys v)))))
         (sort-by key)
         last)))

(defn recover-checkpoint
  [{:keys [job-id task-id slot-id] :as event} prev-replica next-replica checkpoint-type]
  (assert (= slot-id (get-in next-replica [:task-slot-ids job-id task-id (:id event)])))
  (let [[[rv e] checkpoints] (max-completed-checkpoints event next-replica)]
    (info "Recovering:" rv e)
    (get checkpoints [task-id slot-id checkpoint-type]))
  ; (if (some #{job-id} (:jobs prev-replica))
  ;   (do 
  ;    (when (not= (required-checkpoints prev-replica job-id)
  ;                (required-checkpoints next-replica job-id))
  ;      (throw (ex-info "Slots for input tasks must currently be stable to allow checkpoint resume" {})))
  ;    (let [[[rv e] checkpoints] (max-completed-checkpoints event next-replica)]
  ;      (get checkpoints [task-id slot-id]))))
  )

(defn restore-windows
  [{:keys [log-prefix task-map windows-state windows triggers state-log] :as event} 
   old-replica replica]
  (if (:windowed-task? event)
    (let [stored (recover-checkpoint event old-replica replica :state)] 
      (-> event
          ;; TODO: Reset windows / triggers
          (assoc :windows-state (mapv #(wc/resolve-window-state % triggers task-map) windows))
          (update :windows-state
                  (fn [windows-state] 
                    (mapv (fn [ws stored]
                            (info "Recovered " (ws/recover-state ws stored))
                            (if stored
                              (ws/recover-state ws stored) 
                              ws))
                          windows-state
                          (or stored (repeat nil)))))
          ;; Restore via playback
          ; (update :windows-state
          ;         (fn [windows-state] 
          ;           (mapv (fn [ws entries]
          ;                   (-> ws 
          ;                       ;; Restore the accumulated log as a hack
          ;                       ;; To allow us to dump the full state each time
          ;                       (assoc :event-results (mapv ws/log-entry->state-event entries))
          ;                       (ws/play-entry entries)))
          ;                 windows-state
          ;                 (or stored (repeat [])))))
          ))
    event))

(defn next-event 
  [{:keys [job-id] :as event} old-replica replica messenger pipeline barriers windows-states]
  (let [old-version (get-in old-replica [:allocation-version job-id])
        new-version (get-in replica [:allocation-version job-id])]
    (if (= old-version new-version) 
      (-> event
          (assoc :replica replica)
          (assoc :messenger messenger)
          (assoc :barriers barriers)
          (assoc :windows-state windows-states)
          (assoc :pipeline pipeline))
      (-> event
          (assoc :reset-messenger? true)
          (assoc :replica replica)
          (assoc :messenger (ms/new-messenger-state! messenger event old-replica replica))
          ;; I don't think this needs to be reset here
          ;(assoc :barriers {})
          (restore-windows old-replica replica)
          (assoc :pipeline (if (= :input (:task-type event)) 
                             ;; Do this above, and only once
                             (let [checkpoint (recover-checkpoint event old-replica replica :input)]
                               (info "Recovering checkpoint " checkpoint)
                               (oi/recover pipeline checkpoint))
                             pipeline))))))
