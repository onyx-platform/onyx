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

(defn is-next-barrier? [messenger barrier]
  (assert (m/replica-version messenger))
  (and (= (m/replica-version messenger) (:replica-version barrier))
       (= (inc (m/epoch messenger)) (:epoch barrier))))

(defn found-next-barrier? [messenger {:keys [barrier] :as subscriber}]
  (info "barrier" (into {} barrier) "vs " (m/replica-version messenger))
  (and (is-next-barrier? messenger barrier) 
       (not (:emitted? barrier))))

(defn unblocked? [messenger {:keys [barrier] :as subscriber}]
  (println "Unblocked" 
        (= (m/replica-version messenger) (:replica-version barrier))
       (= (m/epoch messenger) (:epoch barrier))
       (:emitted? (:barrier subscriber))   
           )
  (and (= (m/replica-version messenger) (:replica-version barrier))
       (= (m/epoch messenger) (:epoch barrier))
       (:emitted? (:barrier subscriber))))

(defn get-message [messenger {:keys [src-peer-id dst-task-id] :as subscriber} ticket]
  ;(println "messages are" src-peer-id dst-task-id (get-in messenger [:message-state src-peer-id dst-task-id]))
  (get-in messenger [:message-state src-peer-id dst-task-id ticket]))

(defn get-messages [messenger {:keys [src-peer-id dst-task-id] :as subscriber}]
  (get-in messenger [:message-state src-peer-id dst-task-id]))

(defn messenger->subscriptions [messenger]
  (get-in messenger [:subscriptions (:peer-id messenger)]))

(defn messenger->ack-subscriptions [messenger]
  (get-in messenger [:ack-subscriptions (:peer-id messenger)]))

(defn rotate-subscriptions [messenger]
  (update-in messenger [:subscriptions (:peer-id messenger)] rotate))

(defn curr-ticket [messenger {:keys [src-peer-id dst-task-id] :as subscriber}]
  (get-in messenger [:tickets src-peer-id dst-task-id]))

(defn next-barrier [messenger {:keys [src-peer-id dst-task-id position] :as subscriber} max-index]
  (let [missed-indexes (range (inc position) (inc max-index))] 
    (->> missed-indexes
         (map (fn [idx] 
                [idx (get-in messenger [:message-state src-peer-id dst-task-id idx])]))
         (filter (fn [[idx m]]
                   (and (barrier? m)
                        (is-next-barrier? messenger m))))
         first)))

(defn barrier->str [barrier]
  (str "B: " [(:replica-version barrier) (:epoch barrier)]))

(defn subscriber->str [subscriber]
  (str "S: " [(:src-peer-id subscriber) (:dst-task-id subscriber)]
       " STATE: " (barrier->str (:barrier subscriber))))

(defn take-messages [messenger subscriber]
  (let [ticket (curr-ticket messenger subscriber) 
        next-ticket (inc ticket)
        message (get-message messenger subscriber ticket)
        skip-to-barrier? (or (nil? (:barrier subscriber))
                             (and (< (inc (:position subscriber)) ticket) 
                                  (unblocked? messenger subscriber)))] 
    (println "Message is " message "Unblocked? " (unblocked? messenger subscriber))
    (cond skip-to-barrier?
          ;; Skip up to next barrier so we're aligned again, 
          ;; but don't move past actual messages that haven't been read
          (let [max-index (if (nil? (:barrier subscriber)) 
                            (dec (count (get-messages messenger subscriber)))
                            ticket)
                [position next-barrier] (next-barrier messenger subscriber max-index)]
            (debug (:peer-id messenger) (subscriber->str subscriber) (barrier->str next-barrier))
            {:ticket (if position 
                       (max ticket (inc position))
                       ticket)
             :subscriber (if next-barrier 
                           (assoc subscriber :position position :barrier next-barrier)
                           subscriber)})

          ;; We're on the correct barrier so we can read messages
          (and message 
               (barrier? message) 
               (is-next-barrier? messenger message))
          ;; If a barrier, then update your subscriber's barrier to next barrier
          {:ticket next-ticket
           :subscriber (assoc subscriber :position ticket :barrier message)}

          ;; Skip over outdated barrier, and block subscriber until we hit the right barrier
          (and message 
               (barrier? message) 
               (> (m/replica-version messenger)
                  (:replica-version message)))
          {:ticket next-ticket
           :subscriber (assoc subscriber :position ticket :barrier nil)}

          (and message 
               (unblocked? messenger subscriber) 
               (message? message))
          ;; If it's a message, update the subscriber position
          {:message (:message message)
           :ticket next-ticket
           :subscriber (assoc subscriber :position ticket)}

          :else
          ;; Either nothing to read or we're past the current barrier, do nothing
          {:subscriber subscriber})))

(defn take-ack [messenger {:keys [src-peer-id dst-task-id position] :as subscriber}]
  (let [messages (get-in messenger [:message-state src-peer-id dst-task-id])
        _ (info "Trying to take ack from " 
                   src-peer-id dst-task-id
                   position
                   (vec messages))
        [idx ack] (->> messages
                       (map (fn [idx msg] [idx msg]) (range))
                       (drop (inc position))
                       (filter (fn [[idx ack]]
                                 (= (:replica-version ack) (m/replica-version messenger))))
                       (first))]
    (if ack 
      (assoc subscriber :barrier-ack ack :position idx)
      subscriber)))

(defn set-barrier-emitted [subscriber]
  (assoc-in subscriber [:barrier :emitted?] true))

(defn reset-messenger [messenger peer-id]
  (-> messenger 
      (assoc-in [:subscriptions peer-id] [])
      (assoc-in [:ack-subscriptions peer-id] [])
      (assoc-in [:publications peer-id] [])))

(defrecord ImmutableMessenger
  [peer-group peer-id replica-version epoch message-state publications subscriptions ack-subscriptions]
  component/Lifecycle
  (start [component]
    component)

  (stop [component]
    component)

  m/Messenger

  (publications [messenger]
    (get publications peer-id))

  (subscriptions [messenger]
    (get subscriptions peer-id))

  (ack-subscriptions [messenger]
    (get ack-subscriptions peer-id))

  (add-subscription
    [messenger sub-info]
    (-> messenger 
        (update-in [:tickets (:src-peer-id sub-info) (:dst-task-id sub-info)] #(or % 0))
        (update-in [:subscriptions peer-id] 
                   (fn [sbs] 
                     (conj (or sbs []) 
                           (assoc sub-info :position -1))))))

  (add-ack-subscription
    [messenger sub-info]
    (-> messenger 
        (update-in [:tickets (:src-peer-id sub-info) (:dst-task-id sub-info)] #(or % 0))
        (update-in [:ack-subscriptions peer-id] 
                   (fn [sbs] 
                     (conj (or sbs []) 
                           (assoc sub-info :position -1))))))

  (remove-subscription
    [messenger sub-info]
    (-> messenger 
        (update-in [:subscriptions peer-id] 
                   (fn [ss] 
                     (filterv (fn [s] 
                                (not= (select-keys sub-info [:src-peer-id :dst-task-id]) 
                                      (select-keys s [:src-peer-id :dst-task-id])))
                              ss)))))

  (remove-ack-subscription
    [messenger sub-info]
    (-> messenger 
        (update-in [:ack-subscriptions peer-id] 
                   (fn [ss] 
                     (filterv (fn [s] 
                                (not= (select-keys sub-info [:src-peer-id :dst-task-id]) 
                                      (select-keys s [:src-peer-id :dst-task-id])))
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
                            (not= (select-keys pub-info [:src-peer-id :dst-task-id]) 
                                  (select-keys p [:src-peer-id :dst-task-id])))
                          pp))))

  (set-replica-version [messenger replica-version]
    (-> messenger 
        (assoc-in [:replica-version peer-id] replica-version)
        (update-in [:subscriptions peer-id] 
                   (fn [ss] (mapv #(assoc % :barrier-ack nil :barrier nil) ss)))
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
    (println "Epoch " peer-id (get-in messenger [:epoch peer-id]))
    (update-in messenger [:epoch peer-id] inc))

  (receive-acks [messenger]
    (update-in messenger 
               [:ack-subscriptions peer-id]
               (fn [ss]
                 (mapv (fn [s] (take-ack messenger s)) ss))))

  (flush-acks [messenger]
    (update-in messenger 
               [:ack-subscriptions peer-id]
               (fn [ss]
                 (mapv #(assoc % :barrier-ack nil) ss))))

  (all-acks-seen? 
    [messenger]
    (if (empty? (remove :barrier-ack (messenger->ack-subscriptions messenger)))
      (select-keys (:barrier-ack (first (messenger->ack-subscriptions messenger))) 
                   [:replica-version :epoch])))

  (poll [messenger]
    (let [subscriber (first (messenger->subscriptions messenger))
          {:keys [message ticket] :as result} (take-messages messenger subscriber)] 
      (println (:peer-id messenger) "TRying to take from " subscriber "message is " message)
      (debug (:peer-id messenger) "MSG:" 
             (if message message "nil") 
             "New sub:" (subscriber->str (:subscriber result)))
      (cond-> (assoc messenger :message nil)
        ticket (set-ticket (:src-peer-id subscriber) (:dst-task-id subscriber) ticket)
        true (update-first-subscriber (constantly (:subscriber result)))
        true (rotate-subscriptions)
        message (assoc :message (t/input message)))))

  (send-segments
    [messenger batch task-slots]
    (reduce (fn [m msg] 
              (reduce (fn [m* task-slot] 
                        (write m* task-slot (->Message peer-id (:dst-task-id task-slot) msg)))
                      m
                      task-slots)) 
            messenger
            batch))

  (poll-recover [messenger]
    ;; Waits for the initial barriers when not 
    
    ;; read until got all barriers
    ;; Check they all have the same restore information
    ;; Return one restore information
    
    ;; Do loop of receive, all seen, emit, return {:epoch :replica-version}

    ;; FIXME hard coded "batch size"
    ; (println "Messages " 
    ;          (m/replica-version messenger)
    ;          (m/epoch messenger)
    ;          (m/all-barriers-seen? messenger)
    ;          (:message messenger))
      (if (m/all-barriers-seen? messenger)
        (let [recover (:recover (:barrier (first (messenger->subscriptions messenger))))] 
          (assert recover)
          (println "Subscription " (messenger->subscriptions messenger))
          (assoc messenger :recover recover))
        (assoc (m/poll messenger) :recover nil)))

  (emit-barrier [messenger]
    (onyx.messaging.messenger/emit-barrier messenger {}))

  (emit-barrier
    [messenger barrier-opts]
    (as-> messenger mn
      (m/next-epoch mn)
      (reduce (fn [m p] 
                (info "Emitting barrier " peer-id (:dst-task-id p) (m/replica-version mn) (m/epoch mn))
                (write m p (merge (->Barrier peer-id (:dst-task-id p) (m/replica-version mn) (m/epoch mn))
                                  barrier-opts))) 
              mn
              (get publications peer-id))
      (update-in mn
                 [:subscriptions peer-id] 
                 (fn [ss] 
                   (mapv set-barrier-emitted ss)))))

  (all-barriers-seen? 
    [messenger]
    (println "Barriers seen:" 
          (empty? (remove #(found-next-barrier? messenger %) 
                          (messenger->subscriptions messenger)))
          (vec (remove #(found-next-barrier? messenger %) 
                                        (messenger->subscriptions messenger))))
    (empty? (remove #(found-next-barrier? messenger %) 
                    (messenger->subscriptions messenger))))

  (emit-barrier-ack
    [messenger]
    (info "Calling ack barriers  " peer-id 
             (get publications peer-id)
             (get subscriptions peer-id))
    (as-> messenger mn 
      (reduce (fn [m p] 
                (info "Acking barrier to " peer-id (:dst-task-id p) (m/replica-version mn) (m/epoch mn))
                (write m p (->BarrierAck peer-id (:dst-task-id p) (m/replica-version mn) (m/epoch mn)))) 
              mn 
              (get publications peer-id))
      (m/next-epoch mn)
      (update-in mn
                 [:subscriptions peer-id] 
                 (fn [ss]
                   (mapv set-barrier-emitted ss))))))

(defn immutable-messenger [peer-group]
  (map->ImmutableMessenger {:peer-group peer-group}))
