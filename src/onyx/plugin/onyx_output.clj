(ns onyx.plugin.onyx-output
  (:require [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.messaging.messenger :as m]
            [clj-tuple :as t]))

(defprotocol OnyxOutput
  (write-batch [this event]))

(extend-type Object
  OnyxOutput
  (write-batch [this {:keys [results messenger task-id id egress-ids] :as event}]
    (let [task-type (:onyx/type (:task-map event))
          segments (:segments results)]
      (if-not (= task-type :output)
        ;; TODO, implement hash grouping
        ;; TODO, should map first, map rest, over all of the routes, 
        ;; filter by those with nil, then send whole batch, which could be chunked here
        {:messenger (reduce (fn [m {:keys [leaf flow hash-group]}]
                              (reduce 
                               (fn [m* route]
                                 (if route 
                                   (let [segments [(:message leaf)]
                                         task-id (get egress-ids route)]
                                     (m/send-messages m* segments [{:src-peer-id id
                                                                    :dst-task-id task-id}]))
                                   m*))
                               m
                               flow))
                            messenger
                            segments)}
        {}))))
