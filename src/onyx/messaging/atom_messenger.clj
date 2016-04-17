(ns ^:no-doc onyx.messaging.atom-messenger
  (:require [clojure.set :refer [subset?]]
            [com.stuartsierra.component :as component]
            [onyx.messaging.immutable-messenger :as im]
            [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.types :refer [->MonitorEventBytes map->Barrier ->Barrier ->BarrierAck ->Message]]
            [onyx.messaging.messenger :as m]))

(defrecord AtomMessagingPeerGroup [immutable-messenger]
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
  [peer-group peer-id immutable-messenger]
  component/Lifecycle

  (start [component]
    (assoc component :immutable-messenger (:immutable-messenger peer-group)))

  (stop [component]
    component)

  m/Messenger
  (peer-site [messenger]
    (update-messenger-atom! messenger m/peer-site)
    messenger
    )

  (register-subscription
    [messenger sub]
    (update-messenger-atom! messenger m/register-subscription sub)
    messenger
    )

  (unregister-subscription
    [messenger sub]
    (update-messenger-atom! messenger m/unregister-subscription sub)
    messenger
    )

  (register-publication
    [messenger pub]
    (update-messenger-atom! messenger m/register-publication pub)
    messenger
    )

  (unregister-publication
    [messenger pub]
    (update-messenger-atom! messenger m/unregister-publication pub)
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
    (update-messenger-atom! messenger m/receive-acks)
    (:acks @immutable-messenger)
    )

  (receive-messages
    [messenger]
    (update-messenger-atom! messenger m/receive-messages)
    (:messages @immutable-messenger))

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

(defn atom-messenger [peer-group peer-id]
  (map->AtomMessenger {:peer-group peer-group :peer-id peer-id}))
