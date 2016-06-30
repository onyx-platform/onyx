(ns ^:no-doc onyx.messaging.atom-messenger
  (:require [clojure.set :refer [subset?]]
            [com.stuartsierra.component :as component]
            [onyx.messaging.immutable-messenger :as im]
            [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.types :refer [->MonitorEventBytes map->Barrier ->Barrier ->BarrierAck ->Message]]
            [onyx.messaging.messenger :as m]))

(defrecord AtomMessagingPeerGroup [immutable-messenger]
  m/MessengerGroup
  (peer-site [messenger peer-id]
    {})

  component/Lifecycle
  (start [component]
    (assoc component :immutable-messenger (atom (im/immutable-messenger component))))

  (stop [component]
    component))

(defn atom-peer-group [opts]
  (map->AtomMessagingPeerGroup {:opts opts}))

(defmethod m/assign-task-resources :atom
  [replica peer-id task-id peer-site peer-sites]
  {})

(defmethod m/get-peer-site :atom
  [replica peer]
  {})

(defn switch-peer [messenger peer]
  (assoc messenger :peer-id peer))

(defn update-messenger-atom! [messenger f & args]
  ;; Lock because it's faster than constantly retrying
  (locking (:immutable-messenger messenger)
    (swap! (:immutable-messenger messenger) 
           (fn [m] 
             (apply f (switch-peer m (:peer-id messenger)) args))))) 

(defrecord AtomMessenger
  [peer-state peer-id messages immutable-messenger]
  component/Lifecycle

  (start [component]
    (assoc component 
           :peer-id (:id peer-state)
           :immutable-messenger (get-in peer-state [:messaging-group :immutable-messenger])))

  (stop [component]
    component)

  m/Messenger
  (add-subscription
    [messenger sub]
    (update-messenger-atom! messenger m/add-subscription sub)
    messenger
    )

  (remove-subscription
    [messenger sub]
    (update-messenger-atom! messenger m/remove-subscription sub)
    messenger
    )

  (add-publication
    [messenger pub]
    (update-messenger-atom! messenger m/add-publication pub)
    messenger
    )

  (remove-publication
    [messenger pub]
    (update-messenger-atom! messenger m/remove-publication pub)
    messenger
    )

  (set-replica-version
    [messenger replica-version]
    (update-messenger-atom! messenger m/set-replica-version replica-version)
    messenger
    )

  (replica-version
    [messenger]
    (m/replica-version (switch-peer @immutable-messenger peer-id)))

  (epoch
    [messenger]
    (m/epoch (switch-peer @immutable-messenger peer-id)))

  (set-epoch [messenger epoch]
    (update-messenger-atom! messenger m/set-epoch epoch)
    messenger
    )

  (next-epoch
    [messenger]
    (update-messenger-atom! messenger m/next-epoch)
    messenger)

  (receive-acks [messenger]
    (update-messenger-atom! messenger m/receive-acks)
    messenger)

  (receive-messages
    [messenger batch-size]
    (assoc messenger 
           :messages 
           (:messages (update-messenger-atom! messenger m/receive-messages batch-size))))

  (send-segments
    [messenger messages task-slots]
    (update-messenger-atom! messenger m/send-segments messages task-slots)
    messenger
    )

  (emit-barrier
    [messenger]
    (update-messenger-atom! messenger m/emit-barrier)
    messenger
    )

  (flush-acks [messenger]
    (update-messenger-atom! messenger m/flush-acks)
    messenger)

  (all-barriers-seen? [messenger]
    (m/all-barriers-seen? (switch-peer @immutable-messenger peer-id)))

  (all-acks-seen? [messenger]
    (m/all-acks-seen? (switch-peer @immutable-messenger peer-id)))

  (ack-barrier
    [messenger]
    (update-messenger-atom! messenger m/ack-barrier)
    messenger
    ))

(defn atom-messenger []
  (map->AtomMessenger {}))
