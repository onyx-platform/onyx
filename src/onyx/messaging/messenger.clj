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
  (add-subscription [messenger sub])
  (remove-subscription [messenger sub])
  (add-publication [messenger pub])
  (remove-publication [messenger pub])
  (receive-messages [messenger])
  (send-messages [messenger messages task-slots])
  (emit-barrier [messenger])
  (replica-version [messenger])
  (set-replica-version [messenger replica-version])
  (epoch [messenger])
  (next-epoch [messenger])
  (set-epoch [messenger epoch])
  (all-barriers-seen? [messenger])
  (receive-acks [messenger])
  (ack-barrier [messenger]))

