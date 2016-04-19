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
  (swap! (:immutable-messenger messenger) 
         (fn [m] 
           (apply f (switch-peer m (:peer-id messenger)) args)))) 

(defrecord AtomMessenger
  [peer peer-id immutable-messenger]
  component/Lifecycle

  (start [component]
    (assoc component 
           :peer-id (:id peer)
           :immutable-messenger (get-in peer [:peer-group :messaging-group :immutable-messenger])))

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

  (set-epoch [messenger epoch]
    (update-messenger-atom! messenger m/set-epoch epoch)
    messenger
    )

  (next-epoch
    [messenger]
    (update-messenger-atom! messenger m/next-epoch)
    messenger)

  (receive-acks [messenger]
    (:acks (update-messenger-atom! messenger m/receive-acks))
    )

  (receive-messages
    [messenger]
    (:messages (update-messenger-atom! messenger m/receive-messages)))

  (send-messages
    [messenger messages task-slots]
    (update-messenger-atom! messenger m/send-messages messages task-slots)
    messenger
    )

  (emit-barrier
    [messenger]
    (update-messenger-atom! messenger m/emit-barrier)
    messenger
    )

  (all-barriers-seen? [messenger]
    (m/all-barriers-seen? (switch-peer @immutable-messenger peer-id)))

  (ack-barrier
    [messenger]
    (update-messenger-atom! messenger m/ack-barrier)
    messenger
    ))

(defn atom-messenger []
  (map->AtomMessenger {}))
