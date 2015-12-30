(ns ^:no-doc onyx.messaging.acking-daemon
    (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
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
              snapshot (:state @state)
              dead (map first (filter (fn [[k v]] (>= (- t (:timestamp v)) timeout)) snapshot))]
          (doseq [k dead]
            (swap! state update :state dissoc k)))
        (catch InterruptedException e
          (throw e))
        (catch Throwable e
          (timbre/fatal e)))
      (recur))))

(defrecord AckState [state completed?])

(defn update-found-ack [state found-ack message-id ack-val]
  (let [updated-ack-val (bit-xor ^long (:ack-val found-ack) ack-val)]
    (if (zero? updated-ack-val)
      (->AckState (dissoc state message-id) true)
      (->AckState (assoc state message-id (assoc found-ack :ack-val updated-ack-val)) false))))

(defn fully-acked [state]
  (->AckState state true))

(defn add-ack [state message-id ack-val completion-id]
  (->AckState (assoc state message-id (->Ack nil completion-id ack-val nil (now))) 
              false))

(defn update-ack-state [ack-state ack]
  (let [ack-val ^long (:ack-val ack)
        state (:state ack-state)
        message-id (:id ack)
        found-ack (get state message-id)]
    (cond found-ack
          (update-found-ack state found-ack message-id ack-val)

          (zero? ^long ack-val)
          (fully-acked state)

          :else
          (add-ack state message-id ack-val (:completion-id ack)))))

(defn ack-segment [ack-state completion-ch ack]
  (let [rets (swap! ack-state update-ack-state ack)]
    (when (:completed? rets)
      (>!! completion-ch
           {:id (:id ack)
            :peer-id (:completion-id ack)}))))

(defn ack-segments-loop [ack-state acking-ch completion-ch]
  (loop []
    (when-let [ack (<!! acking-ch)]
      (ack-segment ack-state completion-ch ack)
      (recur)))
  (timbre/info "Stopped Ack Messages Loop"))

(defn init-state []
  (atom (->AckState {} false)))

(defrecord AckingDaemon [opts ack-state acking-ch completion-ch timeout-ch]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Acking Daemon")
    (let [completion-buffer-size (arg-or-default :onyx.messaging/completion-buffer-size opts)
          completion-ch (chan completion-buffer-size)
          acking-buffer-size (arg-or-default :onyx.messaging/completion-buffer-size opts)
          acking-ch (chan acking-buffer-size)
          state (init-state)
          ack-segments-fut (future (ack-segments-loop state acking-ch completion-ch))
          timeout-fut (future (clear-messages-loop state opts))]
      (assoc component
             :ack-state state
             :completion-ch completion-ch
             :acking-ch acking-ch
             :ack-segments-fut ack-segments-fut
             :timeout-fut timeout-fut)))

  (stop [component]
    (taoensso.timbre/info "Stopping Acking Daemon")
    (close! (:completion-ch component))
    (close! (:acking-ch component))
    (future-cancel (:timeout-fut component))
    (assoc component :ack-state nil :completion-ch nil :timeout-fut nil :ack-segments-fut nil)))

(defn acking-daemon [config]
  (map->AckingDaemon {:opts config}))

(defn gen-ack-value
  "Generate a 64-bit value to bit-xor against the current ack-value."
  []
  (.nextLong (java.util.concurrent.ThreadLocalRandom/current)))
