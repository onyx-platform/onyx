(ns onyx.plugin.onyx-output
  (:require [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.messaging.messenger :as m]
            [clj-tuple :as t]))

(defprotocol OnyxOutput
  (write-batch [this event]))

(defn send-ungrouped-messages [id job-id task-id messenger segments]
  (let [grouped (group-by :flow segments)] 
    (reduce (fn [m [flow leaves]]
              (let [segments (map :message leaves)
                    destinations (doall 
                                   (map (fn [route] 
                                          {:src-peer-id id
                                           :dst-task-id [job-id route]}) 
                                        flow))]
                ;; TODO: be smart about sending messages to multiple co-located tasks
                (m/send-segments m segments destinations)))
            messenger
            grouped)))

(extend-type Object
  OnyxOutput
  (write-batch [this {:keys [task-type task-name results state job-id task-id id grouping-fn] :as event}]
    (let [messenger (:messenger state)
          segments (:segments results)]
      (info "Writing batch " (m/replica-version messenger) (m/epoch messenger) task-name 
            task-type (vec (:segments results)))
      (if-not (= task-type :output)
        ;; TODO: implement hash grouping
        (if grouping-fn 
          (throw (Exception.))
          {:state (assoc state :messenger (send-ungrouped-messages id job-id task-id messenger segments))})
        {}))))
