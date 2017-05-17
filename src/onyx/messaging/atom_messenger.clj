(ns ^:no-doc onyx.messaging.atom-messenger
  (:require [clojure.set :refer [subset?]]
            [com.stuartsierra.component :as component]
            [onyx.messaging.immutable-messenger :as im]
            [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.types :refer [->MonitorEventBytes]]
            [onyx.messaging.protocols.messenger :as m]))

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
  [replica peer-id job-id task-id peer-site peer-sites]
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
  [messenger-group peer-config id immutable-messenger task->grouping-fn]
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

  (publishers [messenger]
    (m/publishers 
     (switch-peer @immutable-messenger id)))

  (subscriber [messenger]
    (m/subscriber 
     (switch-peer @immutable-messenger id)))

  ; (add-subscription
  ;   [messenger sub]
  ;   (update-messenger-atom! messenger m/add-subscription sub)
  ;   messenger)

  ; (remove-subscription
  ;   [messenger sub]
  ;   (update-messenger-atom! messenger m/remove-subscription sub)
  ;   messenger)

  (update-publishers
    [messenger pubs]
    (update-messenger-atom! messenger m/update-publishers pubs)
    messenger)

  ; (remove-publication
  ;   [messenger pub]
  ;   (update-messenger-atom! messenger m/remove-publication pub)
  ;   messenger)

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

  (offer-barrier [messenger publication]
    (onyx.messaging.protocols.messenger/offer-barrier messenger publication {}))

  (offer-barrier
    [messenger publication barrier-opts]
    (update-messenger-atom! messenger m/offer-barrier publication barrier-opts)
    1))

(defmethod m/build-messenger :atom [peer-config messenger-group monitoring id task->grouping-fn]
  (map->AtomMessenger {:id id 
                       :peer-config peer-config 
                       :monitoring monitoring
                       :messenger-group messenger-group
                       :task->grouping-fn task->grouping-fn}))
