(ns onyx.peer.liveness
  (:require [onyx.protocol.task-state :as t :refer [evict-peer! get-messenger]]
            [onyx.messaging.protocols.status-publisher :as status-pub]
            [onyx.messaging.protocols.subscriber :as sub]
            [onyx.messaging.protocols.publisher :as pub]))

(defn upstream-timed-out-peers [subscriber liveness-timeout-ns]
  (let [curr-time (System/nanoTime)]
    (->> subscriber
         (sub/status-pubs)
         (sequence (comp (filter (fn [[peer-id spub]] 
                                   (< (+ (status-pub/get-heartbeat spub)
                                         liveness-timeout-ns)
                                      curr-time)))
                         (map key))))))

(defn downstream-timed-out-peers [publishers timeout-ms]
  (let [curr-time (System/nanoTime)] 
    (sequence (comp (mapcat pub/statuses)
                    (filter (fn [[peer-id status]] 
                              (< (+ (:heartbeat status) timeout-ms)
                                 curr-time)))
                    (map key))
              publishers)))


