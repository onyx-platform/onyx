(ns onyx.plugin.onyx-output
  (:require [onyx.peer.barrier :refer [emit-barrier emit-barrier?]]
            [onyx.log.commands.peer-replica-view :refer [peer-site]]
            [clj-tuple :as t]))

(defprotocol OnyxOutput
  (write-batch [this event]))

(extend-type Object
  OnyxOutput

  (write-batch [this {:keys [onyx.core/results onyx.core/messenger onyx.core/state
                             onyx.core/replica onyx.core/peer-replica-view
                             onyx.core/serialized-task onyx.core/barrier
                             onyx.core/barrier-state] :as event}]
    (let [segments (:segments (:onyx.core/results event))]
      (when-not (empty? segments)
        (let [replica-val @replica
              pick-peer-fns (:pick-peer-fns @peer-replica-view)
              grouped (group-by #(t/vector (:route %) (:hash-group %)) segments)]
          (run! (fn [[[route hash-group] segs]]
                  (let [segs (map #(assoc % :dst-task (get (:egress-ids serialized-task) route)) segs)]
                    (when-let [pick-peer-fn (get pick-peer-fns (get (:egress-ids serialized-task) route))]
                      (when-let [target (pick-peer-fn hash-group)]
                        (when-let [site (peer-site peer-replica-view target)]
                          (onyx.extensions/send-messages messenger site segs))))))
                grouped)
          (when (or (= :input (:onyx/type (:onyx.core/task-map event)))
                 (emit-barrier? replica-val (:onyx.core/compiled event)
                                @(:onyx.core/barrier-state event)
                                (:onyx.core/job-id event)
                                (:onyx.core/task-id event)
                                (:barrier-id barrier)))
            (emit-barrier event messenger replica-val peer-replica-view barrier-state)))))
    {}))
