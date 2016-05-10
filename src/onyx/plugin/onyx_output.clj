(ns onyx.plugin.onyx-output
  (:require [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.messaging.messenger :as m]
            [clj-tuple :as t]))

(defprotocol OnyxOutput
  (write-batch [this event]))

(extend-type Object
  OnyxOutput
  (write-batch [this {:keys [results messenger task-id id serialized-task] :as event}]
    (let [task-type (:onyx/type (:task-map event))
          segments (:segments results)]
      (if-not (= task-type :output)
        ;; FIXME, don't actually group by route first so we can multiple message
        (let [grouped (group-by #(t/vector (:route %) (:hash-group %)) segments)]
          (info "Writing batch " (:onyx/name (:task-map event)) segments serialized-task)
          {:messenger (reduce (fn [m [[route hash-group] segs]]
                                          ;; TODO, implement hash-group
                                          (let [segs (map :message segs)
                                                task-id (get (:egress-ids serialized-task) route)]
                                            (m/send-messages m segs [{:src-peer-id id
                                                                      :dst-task-id task-id}])))
                                        messenger
                                        grouped)})
        {}))))
