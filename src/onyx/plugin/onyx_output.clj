(ns onyx.plugin.onyx-output
  (:require [onyx.peer.barrier :refer [emit-barrier emit-barrier?]]
            [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.log.commands.peer-replica-view :refer [peer-site]]
            [clj-tuple :as t]))

(defprotocol OnyxOutput
  (write-batch [this event]))

(extend-type Object
  OnyxOutput
  (write-batch [this {:keys [onyx.core/results onyx.core/messenger onyx.core/state
                             onyx.core/replica onyx.core/peer-replica-view onyx.core/global-watermarks
                             onyx.core/compiled onyx.core/job-id onyx.core/task-id 
                             onyx.core/id onyx.core/serialized-task onyx.core/barrier] :as event}]
    (let [replica-val @replica
          segments (:segments results)]
      (when-not (empty? segments)
        (let [pick-peer-fns (:pick-peer-fns @peer-replica-view)
              grouped (group-by #(t/vector (:route %) (:hash-group %)) segments)]
          (run! (fn [[[route hash-group] segs]]
                  (let [segs (map #(assoc %
                                          :dst-task-id (get (:egress-ids serialized-task) route)
                                          :src-task-id task-id
                                          :src-peer-id id)
                                  segs)]
                    (when-let [pick-peer-fn (get pick-peer-fns (get (:egress-ids serialized-task) route))]
                      (when-let [target (pick-peer-fn hash-group)]
                        (when-let [site (peer-site peer-replica-view target)]
                          (onyx.extensions/send-messages messenger site segs))))))
                grouped)))
      (when (emit-barrier? replica-val @global-watermarks (:ingress-ids compiled) event)
        (emit-barrier event messenger replica-val peer-replica-view)))
    {}))
