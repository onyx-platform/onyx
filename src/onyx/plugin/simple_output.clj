(ns onyx.plugin.simple-output
  (:require [onyx.types :refer [->Barrier]]
            [onyx.log.commands.peer-replica-view :refer [peer-site]]
            [onyx.log.commands.common :as common]
            [clj-tuple :as t]))

(defn emit-barrier? [replica compiled barrier-state job-id]
  (let [upstream-peers (reduce
                        (fn [result task-id]
                          (into result (get-in replica [:allocations job-id task-id])))
                        []
                        (:ingress-ids compiled))]
    (= (into #{} (keys barrier-state))
       (into #{} upstream-peers))))

(defn emit-barrier [event messenger replica-val peer-replica-view]
  (let [downstream-task-ids (vals (:egress-ids (:task @peer-replica-view)))
        downstream-peers (mapcat #(get-in replica-val [:allocations (:onyx.core/job-id event) %]) downstream-task-ids)]
    (doseq [target downstream-peers]
      (when-let [site (peer-site peer-replica-view target)]
        (let [b (->Barrier target
                           (:onyx.core/id event)
                           (:barrier-id (:onyx.core/barrier event))
                           (:onyx.core/task-id event)
                           (:task (common/peer->allocated-job (:allocations replica-val) target))
                           (:origin-peer (:onyx.core/barrier event)))]
          (onyx.extensions/send-barrier messenger site b))))
    (reset! (:onyx.core/barrier-state event) {})))

(defprotocol SimpleOutput
  (start [this])

  (stop [this])

  (write-batch [this event])

  (seal-resource [this event]))

;;; Default implementation of Output protocol for input and function tasks.
(extend-type Object
  SimpleOutput

  (start [this] this)

  (stop [this] this)

  (write-batch [this {:keys [onyx.core/results onyx.core/messenger onyx.core/state
                             onyx.core/replica onyx.core/peer-replica-view
                             onyx.core/serialized-task onyx.core/barrier] :as event}]
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
          (when (emit-barrier? replica-val (:onyx.core/compiled event) @(:onyx.core/barrier-state event) (:onyx.core/job-id event))
            (emit-barrier event messenger replica-val peer-replica-view)))))
    {})

  (seal-batch [this event]
    {}))
