## Event Subscription

Onyx's log-based design provides open-ended access to react to all coordination events. This section describes how to tap into these notifications.

### Explanation

Onyx uses an internal log to totally order all coordination events across nodes in the cluster. This log is maintained as a directory of sequentially ordered znodes in ZooKeeper. It's often of interest to watch the events as they are written to the log. For instance, you may want to know when a particular task is completed, or when a peer joins or leaves the cluster. You can use the log subscriber to do just that.

### Subscribing to the Log

The following is a complete example to pretty print all events as they are written to the log. You need to provide the ZooKeeper address, Onyx ID, and shared job scheduler in the peer config.

```clojure
(def peer-config
  {:zookeeper/address "127.0.0.1:2181"
   :onyx/id onyx-id
   :onyx.peer/job-scheduler :onyx.job-scheduler/round-robin})

(def ch (chan 100))

(def subscription (onyx.api/subscribe-to-log peer-config ch))

(def log (:log (:env subscription)))

;; Loops forever
(loop [replica (:replica subscription)]
  (let [position (<!! ch)
        entry (onyx.extensions/read-log-entry log position)
        new-replica (onyx.extensions/apply-log-entry entry replica)]
    (clojure.pprint/pprint new-replica)
    (recur new-replica)))

(onyx.api/shutdown-env (:env subscription))
```

