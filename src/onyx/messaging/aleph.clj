(ns ^:no-doc onyx.messaging.aleph
    (:require [clojure.core.async :refer [chan >!! >! <!! alts!! timeout close! go-loop]]
              [com.stuartsierra.component :as component]
              [org.httpkit.server :as server]
              [taoensso.timbre :as timbre]
              [onyx.messaging.acking-daemon :as acker]
              [onyx.compression.nippy :refer [compress decompress]]
              [onyx.extensions :as extensions]
              [taoensso.nippy :as nippy]
              [manifold.deferred :as d]
              [manifold.stream :as s]
              [gloss.io :as io]
              [gloss.core :as gloss]
              [clojure.edn :as edn]
              [aleph.tcp :as tcp])
    (:import [java.nio ByteBuffer]))

(def protocol
  (gloss/compile-frame
   (gloss/finite-frame :int32 (gloss/repeated :byte :prefix :none))
   compress
   (comp decompress
         ; would prefer having to convert from a vec to a byte array here
         (partial into-array Byte/TYPE))))

(defn wrap-duplex-stream
  [protocol s]
  (let [out (s/stream)]
    (s/connect
     (s/map #(io/encode protocol %) out)
     s)
    (s/splice
     out
     (io/decode-stream s protocol))))

(defn ch-handler [ch]
  (fn [s info]
    (s/connect s ch)))

(defn create-client
  [host port]
  (d/chain (tcp/client {:host host, :port port})
               #(wrap-duplex-stream protocol %)))

(defn start-server
  [handler port]
  (tcp/start-server
   (fn [s info]
     (handler (wrap-duplex-stream protocol s) info))
   {:port port}))

(defn app [daemon net-ch inbound-ch release-ch]
  ; TODO: Need a way to stop this loop
  (go-loop []
           (let [[msg-type payload] (<!! net-ch)]
             (try 
               (case msg-type
                 :send (doseq [message payload]
                              (>!! inbound-ch message))

                 :ack (acker/ack-message daemon
                                                (:id payload)
                                                (:completion-id payload)
                                                (:ack-val payload))
                 :completion (>!! release-ch (:id payload)))
               (catch Exception e
                 (taoensso.timbre/error e)
                 (throw e))))
           (recur)))

(defrecord AlephTcpSockets [opts]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Aleph TCP Sockets")

    (let [net-ch (chan (clojure.core.async/dropping-buffer 10000))
          inbound-ch (:inbound-ch (:messenger-buffer component))
          release-ch (chan (clojure.core.async/dropping-buffer 10000))
          daemon (:acking-daemon component)
          ip "0.0.0.0"
          port (+ 5000 (rand-int 45000))
          server (start-server (ch-handler net-ch) port)
          _ (app daemon net-ch inbound-ch release-ch)]
      (assoc component :server server :ip ip :port port :release-ch release-ch)))

  (stop [component]
    (taoensso.timbre/info "Stopping Aleph TCP Sockets")
    ; NEED TO STOP APP GO LOOP HERE
    (close! (:release-ch component))
    (assoc component :release-ch nil)))

(defn aleph-tcp-sockets [opts]
  (map->AlephTcpSockets {:opts opts}))

; not much need for these in aleph
(defmethod extensions/send-peer-site AlephTcpSockets
  [messenger]
  [(:ip messenger) (:port messenger)])

(defmethod extensions/acker-peer-site AlephTcpSockets
  [messenger]
  [(:ip messenger) (:port messenger)])

(defmethod extensions/completion-peer-site AlephTcpSockets
  [messenger]
  [(:ip messenger) (:port messenger)])

(defmethod extensions/connect-to-peer AlephTcpSockets
  [messenger event [host port]]
  @(create-client host port))

; Should be able to reuse as is from httpkit
(defmethod extensions/receive-messages AlephTcpSockets
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

(defmethod extensions/send-messages AlephTcpSockets
  [messenger event peer-link]
  (let [messages (:onyx.core/compressed event)]
    (s/put! peer-link [:send messages])))

(defmethod extensions/internal-ack-message AlephTcpSockets
  [messenger event peer-link message-id completion-id ack-val]
  (s/put! peer-link [:ack {:id message-id :completion-id completion-id :ack-val ack-val}]))

(defmethod extensions/internal-complete-message AlephTcpSockets
  [messenger event id peer-link]
  (s/put! peer-link [:completion {:id id}]))
