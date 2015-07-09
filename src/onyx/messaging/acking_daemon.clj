(ns ^:no-doc onyx.messaging.acking-daemon
    (:require [clojure.core.async :refer [chan >!! close! sliding-buffer]]
              [com.stuartsierra.component :as component]
              [onyx.static.default-vals :refer [arg-or-default]]
              [onyx.types :refer [->Ack]]
              [taoensso.timbre :as timbre]))

(defn now []
  (System/currentTimeMillis))

(defn clear-messages-loop [state opts]
  (let [timeout (arg-or-default :onyx.messaging/ack-daemon-timeout opts)
        interval (arg-or-default :onyx.messaging/ack-daemon-clear-interval opts)]
    (loop []
      (try
        (Thread/sleep interval)
        (let [t (now)
              snapshot @state
              dead (map first (filter (fn [[k v]] (>= (- t (:timestamp v)) timeout)) snapshot))]
          (doseq [k dead]
            (swap! state dissoc k)))
        (catch InterruptedException e
          (throw e))
        (catch Throwable e
          (timbre/fatal e)))
      (recur))))

(defrecord AckingDaemon [opts]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Acking Daemon")
    (let [buffer-size (arg-or-default :onyx.messaging/completion-buffer-size opts)
          state (atom {})
          timeout-fut (future (clear-messages-loop state opts))]
      (assoc component
        :ack-state state
        :completions-ch (chan (sliding-buffer buffer-size))
        :timeout-fut timeout-fut)))

  (stop [component]
    (taoensso.timbre/info "Stopping Acking Daemon")
    (close! (:completions-ch component))
    (future-cancel (:timeout-fut component))
    (assoc component :ack-state nil :completions-ch nil :timeout-fut nil)))

(defn acking-daemon [config]
  (map->AckingDaemon {:opts config}))

(defn ack-message [daemon message-id completion-id ack-val]
  (let [rets
        (swap!
          (:ack-state daemon)
          (fn [state]
            (if-let [ack (get state message-id)]
              (let [updated-ack-val (bit-xor ^long (:ack-val ack) ^long ack-val)]
                (if (zero? updated-ack-val)
                  (dissoc state message-id) 
                  (assoc state message-id (assoc ack :ack-val updated-ack-val))))
              (if (zero? ^long ack-val) 
                state
                (assoc state message-id (->Ack nil completion-id ack-val (now)))))))]
    (when-not (get rets message-id)
      (>!! (:completions-ch daemon)
           {:id message-id :peer-id completion-id}))))

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
