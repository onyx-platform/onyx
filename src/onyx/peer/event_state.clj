(ns ^:no-doc onyx.peer.event-state
  (:require [onyx.messaging.messenger-state :as ms]
            [taoensso.timbre :refer [info error warn trace fatal]]
            [onyx.messaging.messenger :as m]
            [onyx.windowing.window-compile :as wc]
            [onyx.types :refer [->EventState]]
            [onyx.peer.coordinator :as coordinator]
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

(defn next-windows-state
  [{:keys [log-prefix task-map windows triggers] :as event} old-replica replica]
  (if (:windowed-task? event)
    (let [stored (recover-checkpoint event old-replica replica :state)] 
      (-> windows
          (mapv #(wc/resolve-window-state % triggers task-map))
          (mapv (fn [stored ws]
                  (if stored
                    (let [recovered (ws/recover-state ws stored)] 
                      (info "Recovered state" recovered)
                      recovered) 
                    ws))
                (or stored (repeat nil))))
      ;(update :windows-state
      ;         (fn [windows-state] 
      ;           (mapv (fn [ws entries]
      ;                   (-> ws 
      ;                       ;; Restore the accumulated log as a hack
      ;                       ;; To allow us to dump the full state each time
      ;                       (assoc :event-results (mapv ws/log-entry->state-event entries))
      ;                       (ws/play-entry entries)))
      ;                 windows-state
      ;                 (or stored (repeat [])))))
      )))


;; Put the fast forward to first barrier logic in the messenger
;; Ensure it has access to the kill-ch (required anyway)
;; Make the forward to first barrier return the epoch and the replica to restore from

;; LEASEE

(defn initialisation-barrier [messenger]
  (loop []
    (let [new-messenger (m/receive-messages messenger 1)]
      (assert (empty? (:messages new-messenger)))
      (if (m/all-barriers-seen? new-messenger)
        (m/emit-barrier new-messenger)




        ))))


(defn next-state 
  [{:keys [job-id] :as event} prev-state replica]
  (let [old-replica (:replica prev-state)
        old-version (get-in old-replica [:allocation-version job-id])
        new-version (get-in replica [:allocation-version job-id])]
    (if (= old-version new-version) 
      (assoc event :state (assoc prev-state :replica replica))
      ;; If new version do the get next barrier here?
      ;; then can do a rewind properly
      ;; Setup new messenger, new coordinator here
      ;; Then spin receiving until you can emit a barrier
      ;; If you hit shutdown 
      (let [next-messenger (ms/next-messenger-state! (:messenger prev-state) event old-replica replica)
            next-coordinator (coordinator/next-state (:coordinator prev-state) old-replica replica)
            next-windows-state (next-windows-state event old-replica replica)
            next-pipeline (if (= :input (:task-type event)) 
                            ;; Do this above, and only once
                            (let [checkpoint (recover-checkpoint event old-replica replica :input)]
                              (info "Recovering checkpoint " checkpoint)
                              (oi/recover (:pipeline prev-state) checkpoint))
                            (:pipeline prev-state))
            next-state (->EventState replica next-messenger next-coordinator next-pipeline {} next-windows-state)]
        (assoc event :state next-state)))))
