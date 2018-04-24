(ns onyx.messaging.protocols.messenger)

(defmulti assign-task-resources 
  (fn [replica peer-id job-id task-id peer-site peer-sites]
    (:onyx.messaging/impl (:messaging replica))))

(defmulti get-peer-site 
  (fn [peer-config]
    (:onyx.messaging/impl peer-config)))

(defmulti build-messenger-group 
  (fn [peer-config]
    (:onyx.messaging/impl peer-config)))

(defmulti build-messenger 
  (fn [peer-config messenger-group monitoring id task->grouping-fn]
    (:onyx.messaging/impl peer-config)))

(defprotocol MessengerGroup 
  (peer-site [messenger-group peer-id]))

(defprotocol Messenger
  (start [messenger])
  (stop [messenger])
  (id [messenger])
  (info [messenger])
  (update-subscriber [messenger sub-info])
  (update-publishers [messenger pub-infos])
  (publishers [messenger])
  (task->publishers [messenger])
  (subscriber [messenger])
  (poll [messenger])
  (poll-heartbeats [messenger])
  (offer-heartbeats [messenger])
  (offer-barrier [messenger publication] 
                 [messenger publication barrier-opts] 
                 [messenger publication barrier-opts endpoint-epoch])
  (unblock-subscriber! [messenger])
  (replica-version [messenger])
  (set-replica-version! [messenger replica-version])
  (next-epoch! [messenger])
  (set-epoch! [messenger epoch])
  (epoch [messenger]))
