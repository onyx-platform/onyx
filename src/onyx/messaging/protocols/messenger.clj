(ns onyx.messaging.protocols.messenger)

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
  (id [messenger])
  (update-subscribers [messenger sub-infos])
  (update-publishers [messenger pub-infos])
  (ticket-counters [messenger])
  (publishers [messenger])
  (subscribers [messenger])
  (poll [messenger])
  (poll-recover [messenger])
  (poll-heartbeats [messenger])
  (offer-heartbeats [messenger])
  (offer-segments [messenger messages task-slots])
  (offer-barrier [messenger publication] [messenger publication barrier-opts])
  (unblock-subscribers! [messenger])
  (replica-version [messenger])
  (set-replica-version! [messenger replica-version])
  (next-epoch! [messenger])
  (set-epoch! [messenger epoch])
  (epoch [messenger])
  (barriers-aligned? [messenger])
  (all-barriers-completed? [messenger]))
