(ns onyx.plugin.onyx-output
  (:require [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.messaging.messenger :as m]
            [clj-tuple :as t]))

(defprotocol OnyxOutput
  (write-batch [this event]))

(extend-type Object
  OnyxOutput
  (write-batch [this {:keys [onyx.core/results onyx.core/messenger onyx.core/state
                             onyx.core/task-state onyx.core/compiled onyx.core/job-id onyx.core/task-id onyx.core/barrier
                             onyx.core/id onyx.core/serialized-task] :as event}]
    (let [task-type (:onyx/type (:onyx.core/task-map event))
          segments (:segments results)]
      (info "Segments " (:onyx/name (:onyx.core/task-map event)) segments serialized-task)
      (if-not (= task-type :output)
        ;; FIXME, don't actually group by route first so we can multiple message
        (let [grouped (group-by #(t/vector (:route %) (:hash-group %)) segments)]
          {:onyx.core/messenger (reduce (fn [m [[route hash-group] segs]]
                                          (let [segs (map :message segs)
                                                task-id (get (:egress-ids serialized-task) route)]
                                            (m/send-messages m segs [{:src-peer-id id
                                                                      :dst-task-id task-id}])))
                                        messenger
                                        grouped)})
        {}))))
