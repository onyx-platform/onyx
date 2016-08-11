(ns onyx.plugin.onyx-output
  (:require [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.messaging.messenger :as m]
            [clj-tuple :as t]))

(defprotocol OnyxOutput
  (write-batch [this event]))

(defn select-slot [replica job-id dst-task-id]
  (if (get-in replica [:state-tasks job-id dst-task-id])
    (rand-nth (vals (get-in replica [:task-slot-ids job-id dst-task-id])))
    ;; Replace me with constant
    -1))

(defn send-ungrouped-messages [replica id job-id task-id grouping-fn messenger segments]
  (let [grouped (group-by :flow segments)] 
    (reduce (fn [m [flow leaves]]
              (reduce (fn [m* {:keys [message]}]
                        (let [destinations (doall 
                                             (map (fn [route] 
                                                    ;(println "grouping " grouping-fn)
                                                    ;(println "GROUPING"  (:message message) (if grouping-fn (grouping-fn (:message message))))
                                                    {:src-peer-id id
                                                     ;; TODO: hash before here
                                                     ; :slot-id HASH_ROUTE
                                                     ; THIS TO FIX
                                                     ;; rename dst-slot-id
                                                     :slot-id (select-slot replica job-id route)
                                                     :dst-task-id [job-id route]}) 
                                                  flow))]
                          ;; FIXME: must retry offer when offer fails
                          ;; TODO: be smart about sending messages to multiple co-located tasks
                          ;; TODO: send more than one message at a time
                          (m/offer-segments m [message] destinations)))
                      m
                      leaves))
            messenger
            grouped)))

(extend-type Object
  OnyxOutput
  (write-batch [this {:keys [task-type task-name results state job-id task-id id grouping-fn] :as event}]
    (let [messenger (:messenger state)
          segments (:segments results)]
      (info "Writing batch " 
            (m/replica-version messenger) (m/epoch messenger) 
            task-name task-type (vec (:segments results)))
      (when-not (= task-type :output)
        (send-ungrouped-messages (:replica state) id job-id task-id grouping-fn messenger segments))
      {})))
