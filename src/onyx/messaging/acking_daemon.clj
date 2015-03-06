(ns ^:no-doc onyx.messaging.acking-daemon
    (:require [clojure.core.async :refer [chan >!! close!]]
              [com.stuartsierra.component :as component]
              [taoensso.timbre :as timbre]))

(defrecord AckingDaemon []
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Acking Daemon")
    (assoc component :ack-state (atom {}) :completions-ch (chan 1000)))

  (stop [component]
    (taoensso.timbre/info "Stopping Acking Daemon")
    (close! (:completions-ch component))
    (assoc component :ack-state nil :completions-ch nil)))

(defn acking-daemon [config]
  (map->AckingDaemon {}))

(defn ack-message [daemon message-id completion-id ack-val]
  (let [rets
        (swap!
         (:ack-state daemon)
         (fn [state]
           (if-not (get-in state [message-id])
             (assoc state message-id [completion-id ack-val])
             (let [current-val (second (get-in state [message-id]))]
               (assoc state message-id [completion-id (bit-xor current-val ack-val)])))))]
    (when-let [x (get rets message-id)]
      (when (zero? (second x))
        (swap! (:ack-state daemon) dissoc message-id)
        (>!! (:completions-ch daemon) {:id message-id :peer-id completion-id})))))

(defn gen-message-id
  "Generates a unique ID for a message - acts as the root id."
  []
  (java.util.UUID/randomUUID))

(defn gen-ack-value
  "Generate a 64-bit value to bit-xor against the current ack-value."
  []
  (.nextLong (java.security.SecureRandom.)))

(defn prefuse-vals
  "Prefuse values on a peer before sending them to the acking
   daemon to decrease packet size."
  [& vals]
  (let [vals (filter identity vals)]
    (cond (zero? (count vals)) nil
          (= 1 (count vals)) (first vals)
          :else (apply bit-xor vals))))

