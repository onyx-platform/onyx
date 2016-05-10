(ns onyx.plugin.onyx-output
  (:require [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.messaging.messenger :as m]
            [clj-tuple :as t]))

(defprotocol OnyxOutput
  (write-batch [this event]))

(defn send-ungrouped-messages [id task-id messenger leaves egress-ids]
  (let [grouped (group-by :flow leaves)] 
    (reduce (fn [m [flow leaves]]
              (let [segments (map :message leaves)
                    destinations (map (fn [route] 
                                        (assert route)
                                        {:src-peer-id id
                                         ;; This lookup should've been automatic as part of flow conditions
                                         ;; shouldn't require multiple layers of loookup
                                         :dst-task-id (egress-ids route)}) 
                                      flow)]
                ;; TODO: be smart about sending messages to multiple co-located tasks
                (m/send-segments m segments destinations)))
            messenger
            grouped)))

(extend-type Object
  OnyxOutput
  (write-batch [this {:keys [task-type results messenger task-id id egress-ids grouping-fn] :as event}]
    (let [leaves (:segments results)]
      (if-not (= task-type :output)
        ;; TODO: implement hash grouping
        (if grouping-fn 
          (throw (Exception.))
          {:messenger (send-ungrouped-messages id task-id messenger leaves egress-ids)})
        {}))))
