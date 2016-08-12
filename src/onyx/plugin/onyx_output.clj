(ns onyx.plugin.onyx-output
  (:require [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.peer.grouping :as g]
            [onyx.messaging.messenger :as m]
            [clj-tuple :as t]))

(defprotocol OnyxOutput
  (write-batch [this event]))

(defn select-slot [job-task-id-slots hash-group route]
  (if (empty? hash-group)
    -1
    (if-let [hsh (get hash-group route)]
      ;; TODO: slow, not precomputed
      (let [n-slots (inc (apply max (vals (get job-task-id-slots route))))] 
        (mod hsh n-slots))    
      -1)))

(defn send-messages [{:keys [state id job-id task-id egress-tasks task->group-by-fn]} segments]
  (let [{:keys [replica messenger]} state
        grouped (group-by :flow segments)
        job-task-id-slots (get-in replica [:task-slot-ids job-id])]
    (reduce (fn [m [flow messages]]
              (reduce (fn [m* {:keys [message]}]
                        (let [hash-group (g/hash-groups message egress-tasks task->group-by-fn)
                              destinations (doall 
                                             (map (fn [route] 
                                                    {:src-peer-id id
                                                     :slot-id (select-slot job-task-id-slots hash-group route)
                                                     :dst-task-id [job-id route]}) 
                                                  flow))]
                          ;; FIXME: must retry offer when offer fails
                          ;; TODO: be smart about sending messages to multiple co-located tasks
                          ;; TODO: send more than one message at a time
                          (m/offer-segments m [message] destinations)))
                      m
                      messages))
            messenger
            grouped)))

(extend-type Object
  OnyxOutput
  (write-batch [this {:keys [results state] :as event}]
    (let [segments (:segments results)]
      (info "Writing batch " 
            (m/replica-version (:messenger state)) (m/epoch (:messenger state)) 
            (:task-name event) (:task-type event) (vec (:segments results)))
      (send-messages event segments)
      {})))
