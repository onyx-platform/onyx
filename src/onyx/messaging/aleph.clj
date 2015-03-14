(ns ^:no-doc onyx.messaging.aleph
    (:require [clojure.core.async :refer [chan >!! >! <!! alts!! timeout close! go-loop]]
              [com.stuartsierra.component :as component]
              [org.httpkit.server :as server]
              [taoensso.timbre :as timbre]
              [onyx.messaging.acking-daemon :as acker]
              [onyx.messaging.protocol :as protocol]
              [onyx.compression.nippy :refer [compress decompress]]
              [onyx.extensions :as extensions]
              [manifold.deferred :as d]
              [manifold.stream :as s]
              [gloss.io :as io]
              [clojure.edn :as edn]
              [aleph.netty :as aleph-netty]
              [aleph.tcp :as tcp])
    (:import [java.nio ByteBuffer]))

(def protocol protocol/codec-protocol)

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
  (d/chain (tcp/client {:host host :port port})
               #(wrap-duplex-stream protocol %)))

(defn start-server
  [handler port]
  (tcp/start-server
   (fn [s info]
     (handler (wrap-duplex-stream protocol s) info))
   {:port port}))

(defn app [daemon net-ch inbound-ch release-ch]
  (go-loop []
           (try (let [{:keys [type] :as msg} (<!! net-ch)]
                  (cond (= type protocol/send-id) 
                        (doseq [message (:messages msg)]
                          (>!! inbound-ch message))

                        (= type protocol/ack-id)
                        (acker/ack-message daemon
                                           (:id msg)
                                           (:completion-id msg)
                                           (:ack-val msg))

                        (= type protocol/completion-id)
                        (>!! release-ch (:id msg))))
             (catch Exception e
               (taoensso.timbre/error e)
               (throw e)))
           (recur)))

(defrecord AlephTcpSockets [opts]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Aleph TCP Sockets")

    (let [net-ch (chan (clojure.core.async/dropping-buffer 1000000))
          inbound-ch (:inbound-ch (:messenger-buffer component))
          release-ch (chan (clojure.core.async/dropping-buffer 1000000))
          daemon (:acking-daemon component)
          ip "127.0.0.1"
          server (start-server (ch-handler net-ch) 0)
          app-loop (app daemon net-ch inbound-ch release-ch)]
      (assoc component :app-loop app-loop :server server :ip ip :port (aleph-netty/port server) :release-ch release-ch)))

  (stop [component]
    (taoensso.timbre/info "Stopping Aleph TCP Sockets")
    (close! (:app-loop component))
    (close! (:release-ch component))
    (assoc component :release-ch nil)))

(defn aleph-tcp-sockets [opts]
  (map->AlephTcpSockets {:opts opts}))

; Not much need for these in aleph
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

; Reused as is from HttpKitWebSockets. Probably something to extract.
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
  [messenger event peer-link messages]
  (s/put! peer-link (protocol/send-messages->frame messages)))

(defmethod extensions/internal-ack-message AlephTcpSockets
  [messenger event peer-link message-id completion-id ack-val]
  (s/put! peer-link (protocol/ack-msg->frame {:id message-id :completion-id completion-id :ack-val ack-val})))

(defmethod extensions/internal-complete-message AlephTcpSockets
  [messenger event id peer-link]
  (s/put! peer-link (protocol/completion-msg->frame {:id id})))
