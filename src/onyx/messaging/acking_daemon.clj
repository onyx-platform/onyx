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

(defn ack-message [daemon message-id completion-id ack-val]
  (let [rets
        (swap!
          (:ack-state daemon)
          (fn [state]
            (let [ack (get state message-id)
                  updated-ack-val (if-let [current-ack-val (:ack-val ack)] 
                                    (bit-xor current-ack-val ack-val))]
              (cond (nil? updated-ack-val)
                    (assoc state message-id (->Ack nil completion-id ack-val))
                    (zero? updated-ack-val)
                    (dissoc state message-id) 
                    :else 
                    (assoc state message-id (assoc ack :ack-val updated-ack-val))))))]
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

(defn generate-acks 
  "Batch generates acks."
  [cnt] 
  (let [rng (java.util.concurrent.ThreadLocalRandom/current)] 
    (loop [n 0 coll (transient [])]
      (if (= n cnt)
        (persistent! coll)
        (recur (inc n) 
               (conj! coll (.nextLong rng)))))))
