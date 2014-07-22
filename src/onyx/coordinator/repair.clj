(ns ^:no-doc onyx.coordinator.repair
    (:require [onyx.extensions :as extensions]))

(defn fast-forward-log [sync bucket cb]
  (loop []
    (let [offset (extensions/next-offset sync bucket)]
      (when-let [entry (extensions/log-entry-at sync bucket offset)]
        (cb entry)
        (recur)))))

(defn fast-forward-triggers [sync bucket matching-state cb]
  (doseq [node (extensions/list-nodes sync bucket)]
    (extensions/on-change sync node cb)

    (let [node-data (extensions/read-node sync node)
          state-path (extensions/resolve-node sync :peer-state (:id node-data))
          peer-state (:content (extensions/dereference sync state-path))]
      (when (= (:state peer-state) matching-state)
        (cb node)))))

(defn repair-planning-messages! [sync cb]
  (fast-forward-log sync :planning-log cb))

(defn repair-birth-messages! [sync cb]
  (fast-forward-log sync :born-log cb))

(defn repair-evict-messages! [sync cb]
  (fast-forward-log sync :evict-log cb))

(defn repair-offer-messages! [sync cb]
  (fast-forward-log sync :offer-log cb))

(defn repair-revoke-messages! [sync cb]
  (fast-forward-log sync :revoke-log cb))

(defn repair-ack-messages! [sync cb]
  (fast-forward-triggers sync :ack :acking cb))

(defn repair-exhaust-messages! [sync cb]
  (fast-forward-log sync :exhaust-log cb))

(defn repair-seal-meessages! [sync cb]
  (fast-forward-log sync :seal-log cb))

(defn repair-completion-messages! [sync cb]
  (fast-forward-triggers sync :completion :sealing cb))

(defn repair-shutdown-messages! [sync cb]
  (fast-forward-log sync :shutdown-log cb))

