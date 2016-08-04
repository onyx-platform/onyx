(ns onyx.messaging.messenger)

(defmulti assign-task-resources 
  (fn [replica peer-id task-id peer-site peer-sites]
    (:onyx.messaging/impl (:messaging replica))))


(defmulti get-peer-site 
  (fn [replica peer]
    (:onyx.messaging/impl (:messaging replica))))

(defprotocol MessengerGroup 
  (peer-site [messenger-group peer-id]))


;; TODO: DOCSTRINGS
(defprotocol Messenger
  (start [messenger])
  (stop [messenger])
  (add-subscription [messenger sub])
  (add-ack-subscription [messenger sub])
  (remove-subscription [messenger sub])
  (remove-ack-subscription [messenger sub])
  (add-publication [messenger pub])
  (remove-publication [messenger pub])
  (publications [messenger])
  (subscriptions [messenger])
  (ack-subscriptions [messenger])

  (poll [messenger])
  (poll-recover [messenger])

  (send-segments [messenger messages task-slots])
  (emit-barrier [messenger] [messenger barrier-opts])
  (emit-barrier-ack [messenger])
  (replica-version [messenger])
  (set-replica-version [messenger replica-version])
  (epoch [messenger])
  (next-epoch [messenger])
  (set-epoch [messenger epoch])
  (all-acks-seen? [messenger])
  (all-barriers-seen? [messenger])

  ;; Try to remove multi phase receive/flush. 
  ;; Required for immutable testing version
  (receive-acks [messenger])
  (flush-acks [messenger]))

