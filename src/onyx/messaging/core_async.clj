(ns ^:no-doc onyx.messaging.core-async
    (:require [clojure.core.async :refer [chan >!! <!! alts!! dropping-buffer timeout close!]]
              [com.stuartsierra.component :as component]
              [taoensso.timbre :as timbre]
              [onyx.messaging.acking-daemon :as acker]
              [onyx.extensions :as extensions]))

(defrecord CoreAsyncPeerGroup []
  component/Lifecycle
  (start [component]
    (timbre/info "Starting core.async Peer Group")
    (assoc component :channels (atom {})))

  (stop [component]
    (timbre/info "Stopping core.async Peer Group")
    (assoc component :channels nil)))

(defn core-async-peer-group [opts]
  (map->CoreAsyncPeerGroup {}))

(defmethod extensions/assign-site-resources :core.async
  [config peer-site peer-sites]
  {:site (inc (or (last (sort (map :site (vals peer-sites)))) 0))})

(defrecord CoreAsync [peer-group]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting core.async Messaging Channel")
    (let [release-ch (chan (dropping-buffer 10000))
          retry-ch (chan (dropping-buffer 10000))]
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
        inbound-ch (:inbound-ch (:messenger-buffer messenger))]
    (swap! chs assoc id (chan (dropping-buffer 10000)))
    {:site id}))

(defmethod extensions/open-peer-site CoreAsync
  [messenger assigned]
  ;; Pass, channel and future already running to process messages.
  )

(defmethod extensions/connect-to-peer CoreAsync
  [messenger event peer-site]
  (prn peer-site)
  (clojure.pprint/pprint @(:channels (:messaging-group (:peer-group messenger))))
  (let [chs (:channels (:messaging-group (:peer-group messenger)))
        ch (get @chs (:site peer-site))]
    (assert ch)
    ch))

(defmethod extensions/receive-messages CoreAsync
  [messenger {:keys [onyx.core/task-map] :as event}]
  (let [ms (or (:onyx/batch-timeout task-map) 50)
        ch (:inbound-ch (:onyx.core/messenger-buffer event))
        timeout-ch (timeout ms)]
    (loop [segments [] i 0]
      (if (< i (:onyx/batch-size task-map))
        (if-let [v (first (alts!! [ch timeout-ch]))]
          (recur (conj segments v) (inc i))
          segments)
        segments))))

(defmethod extensions/send-messages CoreAsync
  [messenger event peer-link messages]
  (>!! peer-link {:msgs messages}))

(defmethod extensions/internal-ack-message CoreAsync
  [messenger event peer-link message-id completion-id ack-val]
  (>!! peer-link {:ack {:message-id message-id :completion-id completion-id :ack-val ack-val}}))

(defmethod extensions/internal-complete-message CoreAsync
  [messenger event id peer-link]
  (>!! peer-link {:complete id}))

(defmethod extensions/internal-retry-message CoreAsync
  [messenger event id peer-link]
  (>!! peer-link {:retry id}))

(defmethod extensions/close-peer-connection CoreAsync
  [messenger event peer-link]
  ;; Nothing to do here, closing the channel would close
  ;; it permanently - not desired.
  )
