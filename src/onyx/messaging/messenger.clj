(ns onyx.messaging.messenger)

(defmulti assign-task-resources 
  (fn [replica peer-id task-id peer-site peer-sites]
    (:onyx.messaging/impl (:messaging replica))))

(defmulti get-peer-site 
  (fn [peer-config]
    (:onyx.messaging/impl peer-config)))

(defmulti build-messenger-group 
  (fn [peer-config]
    (:onyx.messaging/impl peer-config)))

(defmulti build-messenger 
  (fn [peer-config messenger-group id]
    (:onyx.messaging/impl peer-config)))

(defprotocol MessengerGroup 
  (peer-site [messenger-group peer-id]))

;; TODO: DOCSTRINGS
(defprotocol Messenger
  (start [messenger])
  (stop [messenger])
  (add-subscription [messenger sub])
  (remove-subscription [messenger sub])
  (add-publication [messenger pub])
  (remove-publication [messenger pub])
  (publications [messenger])
  (subscriptions [messenger])
  (register-ticket [messenger sub-info])
  (get-ticket [messenger sub-info])
  (poll [messenger])
  (poll-recover [messenger])
  (offer-segments [messenger messages task-slots])
  (offer-barrier [messenger publication] [messenger publication barrier-opts])
  (unblock-subscriptions! [messenger])
  (replica-version [messenger])
  (set-replica-version! [messenger replica-version])
  (next-epoch! [messenger])
  (set-epoch! [messenger epoch])
  (epoch [messenger])
  (all-barriers-seen? [messenger])
  (all-barriers-completed? [messenger]))
