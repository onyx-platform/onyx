(ns ^:no-doc onyx.coordinator.repair
    (:require [taoensso.timbre :refer [warn]]
              [onyx.extensions :as extensions]))

(defn fast-forward-log [sync bucket cb]
  (loop [offset (extensions/next-offset sync bucket)]
    (when-let [entry (extensions/log-entry-at sync bucket offset)]
      (cb entry)
      (recur (extensions/speculate-offset sync offset)))))

(defn fast-forward-triggers
  "Add watches back to ZooKeeper. Adds the normal callbacks on nodes that
   haven't yet been touched, and calls ff-cb (fastforward-callback) on
   nodes that have been touched while the Coordinator was offline."
  [sync bucket cb ff-cb]
  (doseq [node (extensions/list-nodes sync bucket)]
    (extensions/on-change sync node cb)

    (let [node-data (extensions/read-node sync node)]
      (when (:id node-data)
        (let [state-path (extensions/resolve-node sync :peer-state (:id node-data))
              peer-state (:content (extensions/dereference sync state-path))]
          (when (extensions/touched? sync bucket node)
            (ff-cb node)))))))

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

(defn repair-ack-messages! [sync cb ff-cb]
  (fast-forward-triggers sync :ack cb ff-cb))

(defn repair-exhaust-messages! [sync cb ff-cb]
  (fast-forward-triggers sync :exhaust cb ff-cb))

(defn repair-seal-messages! [sync seal-cb]
  (fast-forward-log sync :seal-log seal-cb))

(defn repair-completion-messages! [sync cb ff-cb]
  (fast-forward-triggers sync :completion cb ff-cb))

(defn repair-shutdown-messages! [sync cb]
  (fast-forward-log sync :shutdown-log cb))

