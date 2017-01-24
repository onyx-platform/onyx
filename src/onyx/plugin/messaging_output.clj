(ns onyx.plugin.messaging-output
  (:require [clojure.core.async :refer [alts!! <!! >!! <! >! poll! timeout chan close! thread go]]
            [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.flow-conditions.fc-routing :as r]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.messaging.protocols.publisher :as pub]
            [onyx.messaging.serialize :as sz]
            [onyx.peer.constants :refer [ALL_PEERS_SLOT]]
            [onyx.peer.grouping :as g]
            [onyx.plugin.protocols.plugin :as op]
            [onyx.plugin.protocols.output :as oo]
            [onyx.protocol.task-state :refer :all]
            [clj-tuple :as t])
  (:import [org.agrona.concurrent UnsafeBuffer IdleStrategy BackoffIdleStrategy]
           [onyx.serialization MessageEncoder MessageDecoder MessageEncoder$SegmentsEncoder]))

;; TODO, generate a slot selector fn on task start
(defn select-slot [job-task-id-slots hash-group route]
  (if (empty? hash-group)
    ALL_PEERS_SLOT
    (if-let [hsh (get hash-group route)]
      ;; TODO: slow, not precomputed
      (let [n-slots (inc (apply max (vals (get job-task-id-slots route))))] 
        (mod hsh n-slots))    
      ALL_PEERS_SLOT)))

(defn offer-segments [replica-version epoch publishers buffer 
                      batch {:keys [dst-task-id slot-id] :as task-slot}]
  (let [_ (sz/put-message-type buffer 0 sz/message-id)
        encoder (.replicaVersion ^MessageEncoder (sz/wrap-message-encoder buffer 1) replica-version) 
        length (sz/add-segment-payload! encoder batch)] 
    ;; TODO: switch to int2object map lookup
    (loop [pubs (shuffle (get publishers [dst-task-id slot-id]))]
      ;; TODO: retry offers a few times for perf.
      (when-let [^Publisher publisher (first pubs)]
        (let [encoder (.destId encoder (pub/short-id publisher))
              ret (pub/offer! publisher buffer (inc length) epoch)]
          ;(println "Offer segments to " dst-task-id "batch" batch ret (pub/short-id publisher))
          (debug "Offer segment" [:ret ret :dst-task-id dst-task-id :slot-id slot-id 
                                  :batch batch :pub (pub/info publisher)])
          (if (neg? ret)
            (recur (rest pubs))
            task-slot))))))

;; TODO: split out destinations for retry, may need to switch destinations, can do every thing in a single offer
;; TODO: be smart about sending messages to multiple co-located tasks
;; TODO: send more than one message at a time
;; Optimise
(defn send-messages [messenger buffer prepared]
  (let [replica-version (m/replica-version messenger)
        epoch (m/epoch messenger)
        publishers (m/task-slot->publishers messenger)] 
    (loop [batches prepared]
      (when-let [[task-slot batch] (first batches)] 
        (if (offer-segments replica-version epoch publishers buffer batch task-slot)
          (recur (rest batches))
          ;; blocked, return - state will be blocked
          ;; TODO, each time it's blocked, should we send a heartbeat to the remaining
          ;; publications? What about all the peers that don't receive messages?
          batches)))))

(defn partition-xf [task-slot write-batch-size]
  (comp (map first)
        (partition-all write-batch-size)
        (map (fn [segments]
               (list task-slot segments)))))

;; Move buffers into here
;; One big buffer with offsets should be enough.
(deftype MessengerOutput [^:unsynchronized-mutable remaining ^UnsafeBuffer buffer ^long write-batch-size]
  op/Plugin
  (start [this event] this)
  (stop [this event] this)

  oo/Output
  (synced? [this _]
    true)

  (prepare-batch [this 
                  {:keys [onyx.core/id onyx.core/job-id onyx.core/task-id 
                          onyx.core/results egress-tasks task->group-by-fn] :as event} 
                  replica]
    (let [job-task-id-slots (get-in replica [:task-slot-ids job-id])
          ;; TODO: optimise. We need a good algorithm to group into batches by task-slot
          ;; For now this is a very slow version
          output (reduce (fn [accum {:keys [leaves] :as result}]
                           (reduce (fn [accum* segment]
                                     (let [routes (r/route-data event result segment)
                                           segment* (r/flow-conditions-transform segment routes event)
                                           hash-group (g/hash-groups segment* egress-tasks task->group-by-fn)]
                                       (reduce (fn [coll route]
                                                 (conj! coll 
                                                        (list segment* 
                                                              {:slot-id (select-slot job-task-id-slots hash-group route)
                                                               :dst-task-id [job-id route]})))
                                               accum*
                                               (:flow routes))))
                                   accum
                                   leaves))
                         (transient [])
                         (:tree results))
          final-output (->> (persistent! output)
                            (group-by second)
                            ;; TODO, serialize whole buffer in here already?
                            ;; Then just retry until success
                            ;; Filter with reducers.
                            (mapcat (fn [[task-slot coll]]
                                      (sequence (partition-xf task-slot write-batch-size) coll))))]
      (set! remaining final-output)
      true))

  (write-batch [this event _ messenger]
    (let [left (send-messages messenger buffer remaining)]
      (if (empty? remaining)
        (do (set! remaining nil)
            true)
        (do (set! remaining left)
            false)))))

(defn new-messenger-output []
  ;; FIXME: should be configured via informatiom model
  (let [bs (byte-array 10000000) 
        buffer (UnsafeBuffer. bs)] 
    ;; FIXME: default write-batch-size
    (->MessengerOutput nil buffer 200)))
