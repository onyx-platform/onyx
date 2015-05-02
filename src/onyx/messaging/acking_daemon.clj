(ns ^:no-doc onyx.messaging.acking-daemon
    (:require [clojure.core.async :refer [chan >!! close! sliding-buffer]]
              [com.stuartsierra.component :as component]
              [onyx.static.default-vals :refer [defaults]]
              [onyx.types :refer [->Ack]]
              [taoensso.timbre :as timbre]))

(defrecord AckingDaemon [opts]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Acking Daemon")
    (let [buffer-size (or (:onyx.messaging/completion-buffer-size opts) 
                          (:onyx.messaging/completion-buffer-size defaults))]
      (assoc component :ack-state (atom {}) :completions-ch (chan (sliding-buffer buffer-size)))))

  (stop [component]
    (taoensso.timbre/info "Stopping Acking Daemon")
    (close! (:completions-ch component))
    (assoc component :ack-state nil :completions-ch nil)))

(defn acking-daemon [config]
  (map->AckingDaemon {:opts config}))

;; TODO, performance
;; Should ack multiple messages at a time so only a single swap! 
;; is required
(defn ack-message [daemon message-id completion-id ack-val]
  (let [rets
        (swap!
          (:ack-state daemon)
          (fn [state]
            (let [updated-ack-val (if-let [current-ack-val (get-in state [message-id :ack-val])] 
                                    (bit-xor current-ack-val ack-val))]
              (cond (nil? updated-ack-val)
                    (assoc state message-id (->Ack nil completion-id ack-val))
                    (zero? updated-ack-val)
                    (dissoc state message-id) 
                    :else 
                    (assoc-in state [message-id :ack-val] updated-ack-val)))))]
    (when-not (get rets message-id)
      (>!! (:completions-ch daemon) {:id message-id :peer-id completion-id}))))

(defn gen-message-id
  "Generates a unique ID for a message - acts as the root id."
  []
  (java.util.UUID/randomUUID))

(defn gen-ack-value
  "Generate a 64-bit value to bit-xor against the current ack-value."
  []
  (.nextLong (java.util.concurrent.ThreadLocalRandom/current)))

; (defn prefuse-vals
;   "Prefuse values on a peer before sending them to the acking
;   daemon to decrease packet size."
;   [vals]
;   (reduce (fn [running v]
;             (if v 
;               (bit-xor running v)
;               running))
;           (or (first vals) 0)
;           (rest vals)))
