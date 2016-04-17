(ns ^:no-doc onyx.messaging.immutable-messenger
  (:require [clojure.set :refer [subset?]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.types :refer [->MonitorEventBytes map->Barrier ->Barrier ->BarrierAck ->Message]]
            [onyx.messaging.messenger :as m]))

(defrecord ImmutableMessagingPeerGroup []
  component/Lifecycle
  (start [component]
    component)

  (stop [component]
    component))

(defn immutable-peer-group [opts]
  (map->ImmutableMessagingPeerGroup {:opts opts}))

(defmethod m/assign-task-resources :immutable-messaging
  [replica peer-id task-id peer-site peer-sites]
  {})

(defmethod m/get-peer-site :immutable-messaging
  [replica peer]
  {})

(defn barrier? [v]
  (instance? onyx.types.Barrier v))

(defn message? [v]
  (instance? onyx.types.Message v))

(defn ack? [v]
  (instance? onyx.types.BarrierAck v))

(defn update-first-subscriber [messenger f]
  (update-in messenger [:subscriptions (:peer-id messenger) 0] f))

(defn set-ticket [messenger {:keys [src-peer-id dst-task-id] :as subscriber} ticket]
  (assoc-in messenger [:tickets src-peer-id dst-task-id] ticket))

(defn write [messenger {:keys [src-peer-id dst-task-id] :as task-slot} message]
  (update-in messenger 
             [:message-state src-peer-id dst-task-id] 
             (fn [messages] 
               (conj (vec messages) message))))

(defn rotate [xs]
  (if (seq xs)
    (conj (into [] (rest xs)) (first xs))
    xs))

(defn correct-barrier? [messenger {:keys [barrier] :as subscriber}]
  (assert (m/replica-version messenger))
  (and (= (m/replica-version messenger) (:replica-version barrier))
       (= (m/epoch messenger) (:epoch barrier))))

(defn less-than-barrier? [messenger {:keys [barrier] :as subscriber}]
  (or (nil? barrier)
      (< (:replica-version barrier) (m/replica-version messenger))
      (and (= (:replica-version barrier) (m/replica-version messenger))
           (< (:epoch barrier) (m/epoch messenger)))))

(defn next-barrier-seen? [messenger {:keys [barrier] :as subscriber}]
  (assert (m/replica-version messenger))
  (and (= (m/replica-version messenger) (:replica-version barrier))
       (= (inc (m/epoch messenger)) (:epoch barrier))))

(defn get-message [messenger {:keys [src-peer-id dst-task-id] :as subscriber} ticket]
  (get-in messenger [:message-state src-peer-id dst-task-id ticket]))

(defn read-subscriber [messenger subscriber ticket]
  (if (correct-barrier? messenger subscriber) 
    (let [message (get-message messenger subscriber ticket)] 
      (if (message? message)
        message))))

(defn messenger->subscriptions [messenger]
  (get-in messenger [:subscriptions (:peer-id messenger)]))

(defn rotate-subscriptions [messenger]
  (update-in messenger [:subscriptions (:peer-id messenger)] rotate))

(defn next-ticket [messenger {:keys [src-peer-id dst-task-id] :as subscriber}]
  (get-in messenger [:tickets src-peer-id dst-task-id]))

(defn next-barrier [messenger {:keys [src-peer-id dst-task-id position barrier] :as subscriber} max-index]
  (let [missed-indexes (range (inc position) (inc max-index))] 
    (->> missed-indexes
         (map (fn [idx] 
                [idx (get-in messenger [:message-state src-peer-id dst-task-id idx])]))
         (filter (comp barrier? second))
         first)))

(defn take-messages [messenger subscriber]
  (let [ticket (next-ticket messenger subscriber) 
        message (get-message messenger subscriber ticket)] 
    (cond (and message (less-than-barrier? messenger subscriber))
          ;; Skip up to next barrier so we're aligned again, but don't move past actual messages that haven't been read
          (let [[position next-barrier] (next-barrier messenger subscriber ticket)]
            {:ticket (max ticket (or position Integer/MIN_VALUE))
             :subscriber (if next-barrier 
                           (assoc subscriber :barrier next-barrier :position position)
                           subscriber)})
          ;; We're on the correct barrier so we can read messages
          (and message (correct-barrier? messenger subscriber))
          (cond (barrier? message)
                ;; If a barrier, then update your subscriber's barrier to next barrier
                {:ticket (inc ticket)
                 :subscriber (assoc subscriber :barrier message :position ticket)}
                (message? message)
                ;; If it's a message, update the subscriber position
                {:message (:message message)
                 :ticket (inc ticket)
                 :subscriber (assoc subscriber :position ticket)}
                :else 
                (throw (Exception.)))
          :else
          ;; Either nothing to read or we're past the current barrier, do nothing
          {:ticket ticket
           :subscriber subscriber})))

(defn take-acks [messenger {:keys [src-peer-id dst-task-id position barrier] :as subscriber}]
  (let [messages (get-in messenger [:message-state src-peer-id dst-task-id])]
    {:acks (filter ack? (drop (inc position) messages)) 
     :subscriber (assoc subscriber :position (count messages))}))

(defrecord ImmutableMessenger
  [peer-group peer-id replica-version epoch message-state publications subscriptions]
  component/Lifecycle

  (start [component]
    component)

  (stop [component]
    component)

  m/Messenger
  (peer-site [messenger]
    {})

  (register-subscription
    [messenger sub-info]
    (-> messenger 
        (update-in [:tickets (:src-peer-id sub-info) (:dst-task-id sub-info)] #(or % 0))
        (update-in [:subscriptions peer-id] 
                   (fn [sbs] 
                     (conj (or sbs []) 
                           (assoc sub-info :position -1))))))

  (unregister-subscription
    [messenger sub-info]
    (-> messenger 
        (update-in [:subscriptions peer-id] 
                   (fn [ss] 
                     (filterv (fn [s] 
                                (not= sub-info (select-keys s [:src-peer-id :dst-task-id])))
                              ss)))))

  (register-publication
    [messenger pub-info]
    (update-in messenger
               [:publications peer-id] 
               (fn [pbs] 
                 (conj (or pbs []) 
                       {:src-peer-id peer-id
                        :dst-task-id (:dst-task-id pub-info)}))))

  (unregister-publication
    [messenger pub-info]
    (update-in messenger
               [:publications peer-id] 
               (fn [pp] 
                 (filterv (fn [p] 
                            (= (:dst-task-id p) (:dst-task-id pub-info)))
                          pp))))

  (set-replica-version [messenger replica-version]
    (-> messenger 
        (assoc-in [:replica-version peer-id] replica-version)
        (m/set-epoch 0)))

  (replica-version [messenger]
    (get-in messenger [:replica-version peer-id]))

  (epoch [messenger]
    (get epoch peer-id))

  (set-epoch 
    [messenger epoch]
    (assoc-in messenger [:epoch peer-id] epoch))

  (next-epoch
    [messenger]
    (update-in messenger [:epoch peer-id] inc))

  (receive-acks [messenger]
    (reduce (fn [messenger _]
              (let [subscriber (first (messenger->subscriptions messenger))
                    result (take-acks messenger subscriber)]
                (-> messenger
                    (update :acks into (:acks result))
                    (update-first-subscriber (constantly (:subscriber result))))))
            (assoc messenger :acks [])
            (messenger->subscriptions messenger)))

  (receive-messages
    [messenger]
    (reduce (fn [messenger _]
              (let [subscriber (first (messenger->subscriptions messenger))
                    result (take-messages messenger subscriber)
                    mnew (-> messenger
                             (set-ticket subscriber (:ticket result))
                             (assoc :message (:message result))
                             (update-first-subscriber (constantly (:subscriber result)))
                             (rotate-subscriptions))] 
                (if (:message mnew)
                  (reduced mnew)
                  mnew)))
            messenger
            (messenger->subscriptions messenger)))

  (send-messages
    [messenger batch task-slots]
    (reduce (fn [m msg] 
              (reduce (fn [m2 task-slot] 
                        (write m2 task-slot (->Message peer-id (:dst-task-id task-slot) msg)))
                      m
                      task-slots)) 
            messenger
            batch))

  (emit-barrier
    [messenger]
    (reduce (fn [m p] 
              (write m p (->Barrier peer-id (:dst-task-id p) (m/replica-version messenger) (m/epoch messenger)))) 
            messenger 
            (get publications peer-id)))

  (all-barriers-seen? [messenger]
    (empty? (remove #(next-barrier-seen? messenger %) 
                    (messenger->subscriptions messenger))))

  (ack-barrier
    [messenger]
    (reduce (fn [m p] 
              (write m p (->BarrierAck peer-id (:dst-task-id p) (m/replica-version messenger) (m/epoch messenger)))) 
            messenger 
            (get publications peer-id))))

(defn immutable-messenger [peer-group]
  (map->ImmutableMessenger {:peer-group peer-group}))
