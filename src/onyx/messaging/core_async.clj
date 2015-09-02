(ns ^:no-doc onyx.messaging.core-async
    (:require [clojure.core.async :refer [chan >!! <!! alts!! sliding-buffer timeout close!]]
              [com.stuartsierra.component :as component]
              [taoensso.timbre :refer [fatal] :as timbre]
              [onyx.static.default-vals :refer [defaults]]
              [onyx.extensions :as extensions]))

(defrecord CoreAsyncPeerGroup []
  component/Lifecycle
  (start [component]
    (timbre/info "Starting core.async Peer Group")
    (assoc component :channels (atom {})))

  (stop [component]
    (timbre/info "Stopping core.async Peer Group")
    (doseq [ch (vals @(:channels component))]
      (close! ch))
    (assoc component :channels nil)))

(defn core-async-peer-group [opts]
  (map->CoreAsyncPeerGroup {}))

(defmethod extensions/assign-site-resources :core.async
  [replica peer-id peer-site peer-sites]
  peer-site)

(defmethod extensions/get-peer-site :core.async
  [replica peer]
  "localhost")

(defrecord CoreAsync [peer-group]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting core.async Messaging Channel")
    (let [release-ch (chan (sliding-buffer (:onyx.messaging/release-ch-buffer-size defaults)))
          retry-ch (chan (sliding-buffer (:onyx.messaging/retry-ch-buffer-size defaults)))]
      (assoc component :release-ch release-ch :retry-ch retry-ch)))

  (stop [component]
    (taoensso.timbre/info "Stopping core.async Messaging Channel")
    (close! (:release-ch component))
    (close! (:retry-ch component))
    (assoc component :release-ch nil :retry-ch nil)))

(defn core-async [peer-group]
  (map->CoreAsync {:peer-group peer-group}))

(defmethod extensions/peer-site CoreAsync
  [messenger]
  (let [chs (:channels (:messaging-group (:peer-group messenger)))
        id (java.util.UUID/randomUUID)
        acking-ch (:acking-ch (:acking-daemon messenger))
        inbound-ch (:inbound-ch (:messenger-buffer messenger))
        ch (chan (sliding-buffer 100000))]
    (future
      (try
        (loop []
          (when-let [x (<!! ch)]
            (cond (= (:type x) :send)
                  (doseq [m (:messages x)]
                    (>!! inbound-ch m))

                  (= (:type x) :ack)
                  (>!! acking-ch x)

                  (= (:type x) :complete)
                  (>!! (:release-ch messenger) (:id x))

                  (= (:type x) :retry)
                  (>!! (:retry-ch messenger) (:id x))

                  :else
                  (throw (ex-info "Don't recognize message type" {:msg x})))
            (recur)))
        (catch Throwable e
          (fatal e))))
    (swap! chs assoc id ch)
    {:site id}))

(defmethod extensions/open-peer-site CoreAsync
  [messenger assigned]
  ;; Pass, channel and future already running to process messages.
  )

(defmethod extensions/connect-to-peer CoreAsync
  [messenger peer-id event peer-site]
  (let [chs (:channels (:messaging-group (:peer-group messenger)))
        ch (get @chs (:site peer-site))]
    (assert ch)
    ch))

(defmethod extensions/receive-messages CoreAsync
  [messenger {:keys [onyx.core/task-map] :as event}]
  (let [ms (or (:onyx/batch-timeout task-map) (:onyx/batch-timeout defaults))
        ch (:inbound-ch (:onyx.core/messenger-buffer event))
        timeout-ch (timeout ms)]
    (loop [segments [] i 0]
      (if (< i (:onyx/batch-size task-map))
        (if-let [v (first (alts!! [ch timeout-ch]))]
          (recur (conj segments v) (inc i))
          segments)
        segments))))

(defn strip-message [segment]
  (select-keys segment [:id :acker-id :completion-id :ack-val :message]))

(defmethod extensions/send-messages CoreAsync
  [messenger event peer-link messages]
  (>!! peer-link {:type :send :messages (map strip-message messages)}))

(defmethod extensions/internal-ack-segments CoreAsync
  [messenger event peer-link acks]
  (doseq [{:keys [id completion-id ack-val]} acks]
    (>!! peer-link {:type :ack :id id :completion-id completion-id :ack-val ack-val})))

(defmethod extensions/internal-complete-message CoreAsync
  [messenger event id peer-link]
  (>!! peer-link {:type :complete :id id}))

(defmethod extensions/internal-retry-segment CoreAsync
  [messenger event id peer-link]
  (>!! peer-link {:type :retry :id id}))

(defmethod extensions/close-peer-connection CoreAsync
  [messenger event peer-link]
  ;; Nothing to do here, closing the channel would close
  ;; it permanently - not desired.
  )
