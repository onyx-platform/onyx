(ns ^:no-doc onyx.peer.event-state
  (:require [onyx.messaging.messenger-state :as ms]
            [taoensso.timbre :refer [info error warn trace fatal]]
            [onyx.messaging.messenger :as m]
            [onyx.windowing.window-compile :as wc]
            [onyx.types :refer [->EventState]]
            [onyx.peer.coordinator :as coordinator]
            [onyx.peer.window-state :as ws]
            [onyx.plugin.onyx-input :as oi]
            [clojure.core.async :refer [chan >!! <!! close! alts!! timeout go promise-chan]]
            [onyx.extensions :as extensions]))

(defn recover-stored-checkpoint
  [{:keys [log job-id task-id slot-id] :as event} checkpoint-type recover]
  (let [checkpointed (-> (extensions/read-checkpoints log job-id)
                         (get recover)
                         (get [task-id slot-id checkpoint-type]))]
    (if-not (= :beginning checkpointed)
      checkpointed))
  ; (if (some #{job-id} (:jobs prev-replica))
  ;   (do 
  ;    (when (not= (required-checkpoints prev-replica job-id)
  ;                (required-checkpoints next-replica job-id))
  ;      (throw (ex-info "Slots for input tasks must currently be stable to allow checkpoint resume" {})))
  ;    (let [[[rv e] checkpoints] (max-completed-checkpoints event next-replica)]
  ;      (get checkpoints [task-id slot-id]))))
  )

(defn next-windows-state
  [{:keys [log-prefix task-map windows triggers] :as event} recover]
  (if (:windowed-task? event)
    (let [stored (recover-stored-checkpoint event :state recover)]
      (println "NEW STATE:" stored)
      (->> windows
           (mapv (fn [window] (wc/resolve-window-state window triggers task-map)))
           (mapv (fn [stored ws]
                   ;(println "STORED" stored)
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

(defn next-pipeline-state [pipeline event recover]
  (if (= :input (:task-type event)) 
    ;; Do this above, and only once
    (let [stored (recover-stored-checkpoint event :input recover)]
      (info "Recovering checkpoint " stored)
      (oi/recover pipeline stored))
    pipeline))

(defn fetch-recover [event messenger]
  (loop []
    (if-let [recover (m/poll-recover messenger)]
      recover
      (if (first (alts!! [(:kill-ch event) (:task-kill-ch event)] :default true))
        (do
         (Thread/sleep 50)
         (recur))))))

(defn recover-state [{:keys [job-id task-type] :as event} prev-state replica next-messenger next-coordinator recover]
  (let [old-replica (:replica prev-state)
        next-messenger (if (= task-type :output)
                         (m/emit-barrier-ack next-messenger)
                         (m/emit-barrier next-messenger {:recover recover}))
        ;_ (println "RECOVER " recover task-type)
        windows-state (next-windows-state event recover)
        next-pipeline (next-pipeline-state (:pipeline prev-state) event recover)
        next-state (->EventState :processing replica next-messenger next-coordinator next-pipeline {} windows-state)]
    (assoc event :state next-state)))

(defn try-recover [event prev-state replica next-messenger next-coordinator]
  (if-let [recover (fetch-recover event next-messenger)]
    (recover-state event prev-state replica next-messenger next-coordinator recover)
    (assoc event :state (assoc prev-state 
                               :replica replica
                               :state :recover 
                               :messenger next-messenger 
                               :coordinator next-coordinator))))

(defn next-state-from-replica [{:keys [job-id task-type] :as event} prev-state replica]
  ;; If new version do the get next barrier here?
  ;; then can do a rewind properly
  ;; Setup new messenger, new coordinator here
  ;; Then spin receiving until you can emit a barrier
  ;; If you hit shutdown 
  (let [old-replica (:replica prev-state)
        next-messenger (ms/next-messenger-state! (:messenger prev-state) event old-replica replica)
        ;; Coordinator must be transitioned before recovery, as the coordinator
        ;; emits the barrier with the recovery information in 
        next-coordinator (coordinator/next-state (:coordinator prev-state) old-replica replica)]
    (try-recover event prev-state replica next-messenger next-coordinator)))

(defmulti next-state 
  (fn [{:keys [job-id task-type] :as event} prev-state replica]
    (let [old-replica (:replica prev-state)
          old-version (get-in old-replica [:allocation-version job-id])
          new-version (get-in replica [:allocation-version job-id])]
      [(:state prev-state)
       (= old-version new-version)])))

(defmethod next-state [:initial true] [event prev-state replica]
  (throw (Exception. (str "Invalid state" (pr-str replica) (pr-str (:replica prev-state))))))

(defmethod next-state [:initial false] [event prev-state replica]
  (next-state-from-replica event prev-state replica))

(defmethod next-state [:processing true] [event prev-state replica]
  (assoc event :state (assoc prev-state :replica replica)))

(defmethod next-state [:processing false] [event prev-state replica]
  (next-state-from-replica event prev-state replica))

(defmethod next-state [:recover true] [event prev-state replica]
  (try-recover event prev-state replica (:messenger prev-state) (:coordinator prev-state)))

(defmethod next-state [:recover false] [event prev-state replica]
  (next-state-from-replica event prev-state replica))
