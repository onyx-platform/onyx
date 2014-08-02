(ns ^:no-doc onyx.coordinator.repair
    (:require [taoensso.timbre :refer [warn]]
              [onyx.extensions :as extensions]))

(defn fast-forward-log [sync bucket cb]
  (prn (format "===== Phase Log fast forward [%s] ======" bucket))
  (try
    (loop [offset (extensions/next-offset sync bucket)]
      (prn "Next offset to try is: " offset)
      (when-let [entry (extensions/log-entry-at sync bucket offset)]
        (prn "Fast forwarding to " offset)
        (cb entry)
        (recur (extensions/speculate-offset sync offset))))
    (catch Exception e
      (warn e (str bucket " failed to fastforward"))))

  (prn (format "===== Done ====="))
  (println))

(defn fast-forward-triggers [sync bucket cb]
  (prn (format "===== Phase Trigger fast forward [%s] ======" bucket))
  (doseq [node (extensions/list-nodes sync bucket)]
    (prn (format "[%s] Trying to fast forward trigger %s" bucket node))
    (extensions/on-change sync node cb)

    (let [node-data (extensions/read-node sync node)]
      (when (:id node-data)
        (let [state-path (extensions/resolve-node sync :peer-state (:id node-data))
              peer-state (:content (extensions/dereference sync state-path))]
          (prn (format "Touched is %s" (extensions/touched? sync bucket node)))
          (when (extensions/touched? sync bucket node)
            (prn "Calling back to: " node)
            (cb node))))))
  (prn (format "===== Done ====="))
  (println))

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
  (fast-forward-triggers sync :ack cb))

(defn repair-exhaust-messages! [sync cb]
  (fast-forward-log sync :exhaust-log cb))

(defn repair-seal-messages! [sync seal-cb exhaust-cb]
  (fast-forward-triggers sync :exhaust seal-cb)
  (fast-forward-log sync :seal-log exhaust-cb))

(defn repair-completion-messages! [sync cb]
  (fast-forward-triggers sync :completion cb))

(defn repair-shutdown-messages! [sync cb]
  (fast-forward-log sync :shutdown-log cb))

