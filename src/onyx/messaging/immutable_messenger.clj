(ns ^:no-doc onyx.messaging.immutable-messenger
  (:require [clojure.set :refer [subset?]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.types :as t :refer [->MonitorEventBytes map->Barrier ->Barrier ->BarrierAck ->Message]]
            [onyx.messaging.messenger :as m]))

(defrecord ImmutableMessagingPeerGroup []
  m/MessengerGroup
  (peer-site [messenger peer-id]
    {})

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

(defn set-ticket [messenger src-peer-id dst-task-id ticket]
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

(defn find-next-barrier? [messenger {:keys [barrier] :as subscriber}]
  (or (nil? barrier)
      (< (:replica-version barrier) (m/replica-version messenger))
      (and (= (:replica-version barrier) (m/replica-version messenger))
           (< (:epoch barrier) (m/epoch messenger)))))

(defn is-next-barrier? [messenger barrier]
  (assert (m/replica-version messenger))
  (and (= (m/replica-version messenger) (:replica-version barrier))
       (= (inc (m/epoch messenger)) (:epoch barrier))))

(defn found-next-barrier? [messenger {:keys [barrier] :as subscriber}]
  (is-next-barrier? messenger barrier))

;; FIXME: is it really necessary to wait until all the previous barriers 
;; come in before starting processing? Yes? Need an ack setting for the 
;; subscribers that gets set when you've emitted the barrier.
(defn unblocked? [messenger {:keys [barrier] :as subscriber}]
  (and (= (m/replica-version messenger) (:replica-version barrier))
       (= (m/epoch messenger) (:epoch barrier))
       (:emitted? (:barrier subscriber))))

(defn get-message [messenger {:keys [src-peer-id dst-task-id] :as subscriber} ticket]
  (get-in messenger [:message-state src-peer-id dst-task-id ticket]))

(defn messenger->subscriptions [messenger]
  (get-in messenger [:subscriptions (:peer-id messenger)]))

(defn rotate-subscriptions [messenger]
  (update-in messenger [:subscriptions (:peer-id messenger)] rotate))

(defn curr-ticket [messenger {:keys [src-peer-id dst-task-id] :as subscriber}]
  (get-in messenger [:tickets src-peer-id dst-task-id]))

(defn next-barrier [messenger {:keys [src-peer-id dst-task-id position] :as subscriber} max-index]
  (let [missed-indexes (range (inc position) (inc max-index))] 
    (->> missed-indexes
         (map (fn [idx] 
                [idx (get-in messenger [:message-state src-peer-id dst-task-id idx])]))
         (filter (comp barrier? second))
         first)))

(defn take-messages [messenger subscriber]
  (let [ticket (curr-ticket messenger subscriber) 
        next-ticket (inc ticket)
        message (get-message messenger subscriber ticket)] 
    (println "ticket" ticket " message is " message "less than "  
             "is next? " (is-next-barrier? messenger message)
             ;(:barrier subscriber)
             ;(:message-state messenger)
             )
    (cond (and message (find-next-barrier? messenger subscriber))
          ;; Skip up to next barrier so we're aligned again, 
          ;; but don't move past actual messages that haven't been read
          (let [[position next-barrier] (next-barrier messenger subscriber ticket)]
            {:ticket (max next-ticket (or position Integer/MIN_VALUE))
             :subscriber (if next-barrier 
                           (assoc subscriber :barrier next-barrier :position next-ticket)
                           subscriber)})
          ;; We're on the correct barrier so we can read messages
          (and message 
               (barrier? message) 
               ;; Do not overread past future barriers
               ;; Should probably be checking whether it's the next-barrier?
               (is-next-barrier? messenger message))
          ;; If a barrier, then update your subscriber's barrier to next barrier
          (do (println "gonna set barrier " message)
              {:ticket next-ticket
               :subscriber (assoc subscriber :barrier message :position next-ticket)})
          (and message (unblocked? messenger subscriber) (message? message))
          ;; If it's a message, update the subscriber position
          {:message (:message message)
           :ticket next-ticket
           :subscriber (assoc subscriber :position next-ticket)}
            ;; TODO: add case for barrier ack in here?
          :else
          ;; Either nothing to read or we're past the current barrier, do nothing
          {:subscriber subscriber})))

(defn take-acks [messenger {:keys [src-peer-id dst-task-id position barrier] :as subscriber}]
  (let [messages (get-in messenger [:message-state src-peer-id dst-task-id])]
    {:acks (filter ack? (drop (inc position) messages)) 
     :subscriber (assoc subscriber :position (count messages))}))

(defn set-barrier-emitted [subscriber]
  (assoc-in subscriber [:barrier :emitted?] true) )

(defrecord ImmutableMessenger
  [peer-group peer-id replica-version epoch message-state publications subscriptions]
  component/Lifecycle

  (start [component]
    component)

  (stop [component]
    component)

  m/Messenger
  (add-subscription
    [messenger sub-info]
    (-> messenger 
        (update-in [:tickets (:src-peer-id sub-info) (:dst-task-id sub-info)] #(or % 0))
        (update-in [:subscriptions peer-id] 
                   (fn [sbs] 
                     (conj (or sbs []) 
                           (assoc sub-info :position -1))))))

  (remove-subscription
    [messenger sub-info]
    (-> messenger 
        (update-in [:subscriptions peer-id] 
                   (fn [ss] 
                     (filterv (fn [s] 
                                (not= sub-info (select-keys s [:src-peer-id :dst-task-id])))
                              ss)))))

  (add-publication
    [messenger pub-info]
    (update-in messenger
               [:publications peer-id] 
               (fn [pbs] 
                 (conj (or pbs []) 
                       {:src-peer-id peer-id
                        :dst-task-id (:dst-task-id pub-info)}))))

  (remove-publication
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
                    {:keys [message ticket] :as result} (take-messages messenger subscriber)] 
                (cond-> messenger
                  ticket (set-ticket (:src-peer-id subscriber) (:dst-task-id subscriber) ticket)
                  true (update-first-subscriber (constantly (:subscriber result)))
                  true (rotate-subscriptions)
                  message (update :messages conj (t/input message))
                  message reduced)))
            (assoc messenger :messages [])
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
    (as-> messenger mn
      (reduce (fn [m p] 
                (write m p (->Barrier peer-id (:dst-task-id p) (m/replica-version m) (m/epoch m)))) 
              mn
              (get publications peer-id))
      (m/next-epoch mn)
      (update-in mn
                 [:subscriptions peer-id] 
                 (fn [ss] 
                   (mapv set-barrier-emitted ss)))))

  (all-barriers-seen? 
    [messenger]
    (empty? (remove #(found-next-barrier? messenger %) 
                    (messenger->subscriptions messenger))))

  (ack-barrier
    [messenger]
    (as-> messenger mn 
      (reduce (fn [m p] 
                (write m p (->BarrierAck peer-id (:dst-task-id p) (m/replica-version m) (m/epoch m)))) 
              mn 
              (get publications peer-id))
      (m/next-epoch mn)
      (update-in mn
                 [:subscriptions peer-id] 
                 (fn [ss]
                   (mapv set-barrier-emitted ss))))))

(defn immutable-messenger [peer-group]
  (map->ImmutableMessenger {:peer-group peer-group}))
