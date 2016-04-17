(ns onyx.messaging.messenger)

(defmulti assign-task-resources 
  (fn [replica peer-id task-id peer-site peer-sites]
    (:onyx.messaging/impl (:messaging replica))))


(defmulti get-peer-site 
  (fn [replica peer]
    (:onyx.messaging/impl (:messaging replica))))

(defprotocol Messenger
  (peer-site [messenger])
  (register-task-peer [messenger assigned buffers])
  (unregister-task-peer [messenger assigned])
  (shared-ticketing-counter [messenger job-id peer-id task-id])
  (new-partial-subscriber [messenger job-id peer-id task-id])
  (close-partial-subscriber [messenger partial-subscriber])
  (register-subscription [messenger sub])
  (unregister-subscription [messenger sub])
  (register-publication [messenger pub])
  (unregister-publication [messenger pub])
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

