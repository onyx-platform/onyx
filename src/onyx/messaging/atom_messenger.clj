(ns ^:no-doc onyx.messaging.atom-messenger
  (:require [clojure.set :refer [subset?]]
            [com.stuartsierra.component :as component]
            [onyx.messaging.immutable-messenger :as im]
            [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.types :refer [->MonitorEventBytes map->Barrier ->Barrier ->Message]]
            [onyx.messaging.messenger :as m]))

(defrecord AtomMessagingPeerGroup [immutable-messenger]
  m/MessengerGroup
  (peer-site [messenger peer-id]
    {})

  component/Lifecycle
  (start [component]
    (assoc component :immutable-messenger (atom (im/immutable-messenger {}))))

  (stop [component]
    component))

(defmethod m/build-messenger-group :atom [peer-config]
  (map->AtomMessagingPeerGroup {:peer-config peer-config}))

(defmethod m/assign-task-resources :atom
  [replica peer-id task-id peer-site peer-sites]
  {})

(defmethod m/get-peer-site :atom
  [peer-config]
  {})

(defn switch-peer [messenger peer]
  (assoc messenger :id peer))

(defn update-messenger-atom! [messenger f & args]
  ;; Lock because it's faster than constantly retrying
  (locking (:immutable-messenger messenger)
    (swap! (:immutable-messenger messenger) 
           (fn [m] 
             (apply f (switch-peer m (:id messenger)) args))))) 

(defrecord AtomMessenger
  [messenger-group peer-config id immutable-messenger]
  component/Lifecycle

  (start [component]
    (let [messenger (:immutable-messenger messenger-group)] 
      ;; Reset this peer's subscriptions and publications to a clean state
      ;; As it may not have gone through the correct lifecycle
      (swap! messenger im/reset-messenger id)
      (assoc component :immutable-messenger messenger)))

  (stop [component]
    component)

  m/Messenger

  (publications [messenger]
    (m/publications 
     (switch-peer @immutable-messenger id)))

  (subscriptions [messenger]
    (m/subscriptions 
     (switch-peer @immutable-messenger id)))

  (add-subscription
    [messenger sub]
    (update-messenger-atom! messenger m/add-subscription sub)
    messenger)

  (remove-subscription
    [messenger sub]
    (update-messenger-atom! messenger m/remove-subscription sub)
    messenger)

  (add-publication
    [messenger pub]
    (update-messenger-atom! messenger m/add-publication pub)
    messenger)

  (remove-publication
    [messenger pub]
    (update-messenger-atom! messenger m/remove-publication pub)
    messenger)

  (set-replica-version!
    [messenger replica-version]
    (update-messenger-atom! messenger m/set-replica-version! replica-version)
    messenger)

  (replica-version
    [messenger]
    (m/replica-version (switch-peer @immutable-messenger id)))

  (epoch
    [messenger]
    (m/epoch (switch-peer @immutable-messenger id)))

  (set-epoch! [messenger epoch]
    (update-messenger-atom! messenger m/set-epoch! epoch)
    messenger)

  (next-epoch!
    [messenger]
    (update-messenger-atom! messenger m/next-epoch!)
    messenger)

  (poll [messenger]
    (if-let [message (:message (update-messenger-atom! messenger m/poll))]
      [message]
      []))

  (poll-recover [messenger]
    (:recover (update-messenger-atom! messenger m/poll-recover)))

  (offer-segments
    [messenger batch task-slot]
    (update-messenger-atom! messenger m/offer-segments batch task-slot)
    ;; Success!
    task-slot)

  (register-ticket [messenger sub-info] messenger)

  (offer-barrier [messenger publication]
    (onyx.messaging.messenger/offer-barrier messenger publication {}))

  (offer-barrier
    [messenger publication barrier-opts]
    (update-messenger-atom! messenger m/offer-barrier publication barrier-opts)
    1)
  
  (unblock-subscriptions! 
    [messenger]
    (update-messenger-atom! messenger m/unblock-subscriptions!)
    messenger)

  (all-barriers-seen? [messenger]
    (m/all-barriers-seen? (switch-peer @immutable-messenger id)))

  (all-barriers-completed? [messenger]
    (m/all-barriers-completed? (switch-peer @immutable-messenger id))))

(defmethod m/build-messenger :atom [peer-config messenger-group id]
  (map->AtomMessenger {:id id 
                       :peer-config peer-config 
                       :messenger-group messenger-group}))
