(ns ^:no-doc onyx.coordinator.repair
    (:require [onyx.extensions :as extensions]))

(defn repair-planning-messages! []
  )

(defn repair-birth-messages! []
  )

(defn repair-death-messages! []
  )

(defn repair-evict-messages! []
  )

(defn repair-offer-messages! []
  )

(defn repair-revoke-messages! []
  )

(defn repair-ack-messages! [sync cb]
  (doseq [node (extensions/list-nodes sync :ack)]
    (extensions/on-change sync node cb)

    (let [node-data (extensions/read-node sync node)
          state-path (extensions/resolve-node sync :peer-state (:id node-data))
          peer-state (:content (extensions/dereference sync state-path))]
      (when (= (:state peer-state) :acking)
        (extensions/ack sync node)))))

(defn repair-exhaust-messages! []
  )

(defn repair-seal-meessages! []
  )

(defn repair-completion-messages! []
  )

(defn repair-shutdown-messages! []
  )

