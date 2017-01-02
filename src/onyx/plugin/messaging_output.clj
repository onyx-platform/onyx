(ns onyx.plugin.messaging-output
  (:require [clojure.core.async :refer [alts!! <!! >!! <! >! poll! timeout chan close! thread go]]
            [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.peer.constants :refer [ALL_PEERS_SLOT]]
            [onyx.peer.grouping :as g]
            [onyx.plugin.protocols.plugin :as op]
            [onyx.plugin.protocols.output :as oo]
            [onyx.protocol.task-state :refer :all]
            [clj-tuple :as t]))

(defn select-slot [job-task-id-slots hash-group route]
  (if (empty? hash-group)
    ALL_PEERS_SLOT
    (if-let [hsh (get hash-group route)]
      ;; TODO: slow, not precomputed
      (let [n-slots (inc (apply max (vals (get job-task-id-slots route))))] 
        (mod hsh n-slots))    
      ALL_PEERS_SLOT)))

;; TODO: split out destinations for retry, may need to switch destinations, can do every thing in a single offer
;; TODO: be smart about sending messages to multiple co-located tasks
;; TODO: send more than one message at a time
;; Optimise
(defn send-messages [messenger replica prepared]
  (loop [messages prepared]
    (when-let [[message task-slot] (first messages)] 
      (if (m/offer-segments messenger [message] task-slot)
        (recur (rest messages))
        ;; blocked, return - state will be blocked
        ;; TODO, each time it's blocked, should we send a heartbeat to the remaining
        ;; publications? What about all the peers that don't receive messages?
        messages))))

(deftype MessengerOutput [^:unsynchronized-mutable remaining]
  op/Plugin
  (start [this event] this)
  (stop [this event] this)

  oo/Output
  (synced? [this _]
    [true this])

  (prepare-batch [this 
                  {:keys [onyx.core/id onyx.core/job-id onyx.core/task-id 
                          onyx.core/results egress-tasks task->group-by-fn] :as event} 
                  replica]
    (let [;; Flatten outputs in preparation for incremental sending in write-batch
          ;; move many of this out of event
          segments (:segments results)
          grouped (group-by :flow segments)
          job-task-id-slots (get-in replica [:task-slot-ids job-id])
          ;; TODO: optimise
          output (reduce (fn [accum [flow messages]]
                           (reduce (fn [accum* {:keys [message]}]
                                     (let [hash-group (g/hash-groups message egress-tasks task->group-by-fn)
                                           ;; todo, map to short ids here
                                           task-slots (map (fn [route] 
                                                             {:src-peer-id id
                                                              :slot-id (select-slot job-task-id-slots hash-group route)
                                                              :dst-task-id [job-id route]}) 
                                                           flow)]
                                       (reduce conj! 
                                               accum* 
                                               (map (fn [task-slot]
                                                      [message task-slot]) 
                                                    task-slots))))
                                   accum
                                   messages))
                         (transient [])
                         grouped)]
      (set! remaining (persistent! output))
      [true this]))

  (write-batch [this event replica messenger]
    (let [left (send-messages messenger replica remaining)]
      (if (empty? remaining)
        (do (set! remaining nil)
            [true this])
        (do (set! remaining left)
            [false this])))))
