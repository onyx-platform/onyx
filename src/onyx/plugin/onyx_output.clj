(ns onyx.plugin.onyx-output
  (:require [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.messaging.messenger :as m]
            [clj-tuple :as t]))

(defprotocol OnyxOutput
  (write-batch [this event]))

(defn send-ungrouped-messages [id job-id task-id messenger segments]
  (let [grouped (group-by :flow segments)] 
    (reduce (fn [m [flow leaves]]
              (reduce (fn [m* {:keys [message]}]
                        (let [destinations (doall 
                                             (map (fn [route] 
                                                    {:src-peer-id id
                                                     :dst-task-id [job-id route]}) 
                                                  flow))]
                          ;; FIXME: must retry offer when offer fails
                          ;; TODO: be smart about sending messages to multiple co-located tasks
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
      (if-not (= task-type :output)
        ;; TODO: implement hash grouping
        (if grouping-fn 
          (throw (Exception.))
          (do
            (send-ungrouped-messages id job-id task-id messenger segments)
            {}))
        {}))))
