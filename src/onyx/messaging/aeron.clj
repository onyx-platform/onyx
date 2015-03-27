(ns ^:no-doc onyx.messaging.aeron
    (:require [clojure.core.async :refer [chan >!! <!! alts!! timeout close!]]
              [com.stuartsierra.component :as component]
              [taoensso.timbre :refer [fatal] :as timbre]
              [onyx.messaging.protocol-aeron :as protocol]
              [onyx.messaging.acking-daemon :as acker]
              [onyx.messaging.common :refer [bind-addr external-addr allowable-ports]]
              [onyx.compression.nippy :refer [decompress compress]]
              [onyx.extensions :as extensions])
    (:import [uk.co.real_logic.aeron Aeron FragmentAssemblyAdapter]
             [uk.co.real_logic.aeron Aeron$Context]
             [uk.co.real_logic.agrona.concurrent UnsafeBuffer]
             [uk.co.real_logic.agrona CloseHelper]
             [uk.co.real_logic.aeron.driver MediaDriver]
             [uk.co.real_logic.aeron.common.concurrent.logbuffer DataHandler]
             [uk.co.real_logic.agrona.concurrent BackoffIdleStrategy]
             [java.util.function Consumer]
             [java.util.concurrent TimeUnit]))

(defrecord AeronPeerGroup [opts]
  component/Lifecycle
  (start [component]
    (taoensso.timbre/info "Starting Aeron peer group")
    (let [media-driver (MediaDriver/launch)]
      (assoc component :media-driver media-driver)))

  (stop [{:keys [media-driver] :as component}]
    (.close media-driver)
    (assoc component :media-driver nil)))

(defn aeron-peer-group [opts]
  (map->AeronPeerGroup {:opts opts}))

(defmethod extensions/assign-site-resources :aeron
  [config peer-site peer-sites]
  (let [used-ports (->> (vals peer-sites) 
                        (filter 
                          (fn [s]
                            (= (:aeron/external-addr peer-site) 
                               (:aeron/external-addr s))))
                        (map :aeron/port)
                        set)
        port (first (remove used-ports
                            (:aeron/ports peer-site)))]
    (assert port)
    {:aeron/port port}))

(defn handle-sent-message [inbound-ch ^UnsafeBuffer buffer offset length header]
  (let [messages (protocol/read-messages-buf buffer offset length)]
    (doseq [message messages]
      (>!! inbound-ch message))))

(defn handle-acker-message [daemon buffer offset length header]
  (let [thawed (protocol/read-acker-message buffer offset)]
    (acker/ack-message daemon
                       (:id thawed)
                       (:completion-id thawed)
                       (:ack-val thawed))))

(defn handle-completion-message [release-ch buffer offset length header]
  (let [completion-id (protocol/read-completion buffer offset)]
    (>!! release-ch completion-id)))

(defn data-handler [f]
  (FragmentAssemblyAdapter. 
    (proxy [DataHandler] []
      (onData [buffer offset length header]
        (f buffer offset length header)))))

(defn consumer [limit]
  (proxy [Consumer] []
    (accept [subscription]
      ;; TODO, evaluate different idle strategies.
      (let [strategy :high-latency-restart
            idle-strategy (case strategy 
                            :high-latency-restart (BackoffIdleStrategy.
                                                    100
                                                    10
                                                    (.toNanos TimeUnit/MICROSECONDS 10000)
                                                    (.toNanos TimeUnit/MICROSECONDS 100000))
                            :low-latency-restart (BackoffIdleStrategy.
                                                   100 
                                                   10
                                                   (.toNanos TimeUnit/MICROSECONDS 1)
                                                   (.toNanos TimeUnit/MICROSECONDS 100)))]
        (while (not (Thread/interrupted))
          (let [fragments-read (.poll ^uk.co.real_logic.aeron.Subscription subscription limit)]
            (.idle idle-strategy fragments-read)))))))

(def no-op-error-handler
  (proxy [Consumer] []
    (accept [_] (taoensso.timbre/warn "Conductor is down."))))

(defrecord AeronConnection [peer-group]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Aeron")
    (let [config (:config peer-group)
          release-ch (chan (clojure.core.async/dropping-buffer 100000))
          bind-addr (bind-addr config)
          external-addr (external-addr config)
          ports (allowable-ports config)]
      (assoc component 
             :bind-addr bind-addr 
             :external-addr external-addr
             :ports ports
             :resources (atom nil) 
             :release-ch release-ch)))

  (stop [{:keys [aeron resources release-ch] :as component}]
    (taoensso.timbre/info "Stopping Aeron")
    (try 
      (when-let [rs @resources]
        (let [{:keys [conn
                      send-subscriber 
                      acker-subscriber 
                      completion-subscriber 
                      accept-send-fut 
                      accept-acker-fut
                      accept-completion-fut]} rs] 
          (future-cancel accept-send-fut)
          (future-cancel accept-acker-fut)
          (future-cancel accept-completion-fut)
          (when send-subscriber (.close send-subscriber))
          (when acker-subscriber (.close acker-subscriber))
          (when completion-subscriber (.close completion-subscriber))
          (when conn (.close conn)))
        (reset! resources nil))
      (close! (:release-ch component))
      (catch Exception e (fatal e)))

    (assoc component :bind-addr nil :external-addr nil :site-resources nil :release-ch nil)))

(defn aeron [peer-group]
  (map->AeronConnection {:peer-group peer-group}))

(defmethod extensions/peer-site AeronConnection
  [messenger]
  {:aeron/ports (:ports messenger)
   :aeron/external-addr (:external-addr messenger)})

(def send-stream-id 1)
(def acker-stream-id 2)
(def completion-stream-id 3)

(defn aeron-channel [addr port]
  (format "udp://%s:%s" addr port))

(defmethod extensions/open-peer-site AeronConnection
  [messenger assigned]
  (timbre/info "Open peer site " messenger assigned)
  (let [inbound-ch (:inbound-ch (:messenger-buffer messenger))
        release-ch (:release-ch messenger)
        daemon (:acking-daemon messenger)

        ctx (.errorHandler (Aeron$Context.) no-op-error-handler)
        aeron (Aeron/connect ctx)

        channel (aeron-channel (:bind-addr messenger) (:aeron/port assigned))

        send-handler (data-handler (partial handle-sent-message inbound-ch))
        acker-handler (data-handler (partial handle-acker-message daemon))
        completion-handler (data-handler (partial handle-completion-message release-ch))

        send-subscriber (.addSubscription aeron channel send-stream-id send-handler)
        acker-subscriber (.addSubscription aeron channel acker-stream-id acker-handler)
        completion-subscriber (.addSubscription aeron channel completion-stream-id completion-handler)
        accept-send-fut (future (try (.accept ^Consumer (consumer 10) send-subscriber) 
                                     (catch Exception e (fatal e))))
        accept-acker-fut (future (try (.accept ^Consumer (consumer 10) acker-subscriber) 
                                      (catch Exception e (fatal e))))
        accept-completion-fut (future (try (.accept ^Consumer (consumer 10) completion-subscriber) 
                                           (catch Exception e (fatal e))))]
    (reset! (:resources messenger)
            {:conn aeron
             :accept-send-fut accept-send-fut
             :accept-acker-fut accept-acker-fut
             :accept-completion-fut accept-completion-fut
             :send-subscriber send-subscriber
             :acker-subscriber acker-subscriber
             :completion-subscriber completion-subscriber})))

(defmethod extensions/connect-to-peer AeronConnection
  [messenger event {:keys [aeron/external-addr aeron/port]}]
  (timbre/info "Connect to peer " external-addr port)
  (let [ctx (.errorHandler (Aeron$Context.) no-op-error-handler)
        aeron (Aeron/connect ctx)
        channel (aeron-channel external-addr port)
        send-pub (.addPublication aeron channel send-stream-id)
        acker-pub (.addPublication aeron channel acker-stream-id)
        completion-pub (.addPublication aeron channel completion-stream-id)]
    {:conn aeron :send-pub send-pub :acker-pub acker-pub :completion-pub completion-pub}))

(defmethod extensions/receive-messages AeronConnection
  [messenger {:keys [onyx.core/task-map] :as event}]
  (let [ms (or (:onyx/batch-timeout task-map) 1000)
        ch (:inbound-ch (:onyx.core/messenger-buffer event))
        timeout-ch (timeout ms)]
    (loop [segments [] i 0]
      (if (< i (:onyx/batch-size task-map))
        (if-let [v (first (alts!! [ch timeout-ch]))]
          (recur (conj segments v) (inc i))
          segments)
        segments))))

(defmethod extensions/send-messages AeronConnection
  [messenger event peer-link batch]
  (let [[len unsafe-buffer] (protocol/build-messages-msg-buf batch)
        pub ^uk.co.real_logic.aeron.Publication (:send-pub peer-link)
        offer-f (fn [] (.offer pub unsafe-buffer 0 len))]
    (while (not (offer-f)))))

(defmethod extensions/internal-ack-message AeronConnection
  [messenger event peer-link message-id completion-id ack-val]
  (let [unsafe-buffer (protocol/build-acker-message message-id completion-id ack-val)
        pub ^uk.co.real_logic.aeron.Publication (:acker-pub peer-link)
        offer-f (fn [] (.offer pub unsafe-buffer 0 protocol/ack-msg-length))]
    (while (not (offer-f)))))

(defmethod extensions/internal-complete-message AeronConnection
  [messenger event id peer-link]
  (let [unsafe-buffer (protocol/build-completion-msg-buf id)
        pub ^uk.co.real_logic.aeron.Publication (:completion-pub peer-link) 
        offer-f (fn [] (.offer pub unsafe-buffer 0 protocol/completion-msg-length))]
    (while (not (offer-f)))))

(defmethod extensions/close-peer-connection AeronConnection
  [messenger event peer-link]
  (.close (:send-pub peer-link))
  (.close (:acker-pub peer-link))
  (.close (:completion-pub peer-link))
  (.close (:conn peer-link)) 
  {})
