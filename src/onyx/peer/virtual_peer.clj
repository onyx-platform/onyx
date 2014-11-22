(ns ^:no-doc onyx.peer.virtual-peer
  (:require [clojure.core.async :refer [chan >!! <!! thread alts!! close!]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :as timbre]
            [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]))

(defn send-to-outbox [{:keys [outbox-ch] :as state} reactions]
  (if (:stall-output? state)
    (do
      (doseq [reaction (filter :immediate? reactions)]
        (clojure.core.async/>!! outbox-ch reaction))
      (update-in state [:buffered-outbox] concat (remove :immediate? reactions)))
    (do
      (doseq [reaction reactions]
        (clojure.core.async/>!! outbox-ch reaction))
      state)))

(defn processing-loop [id log inbox-ch outbox-ch kill-ch]
  (try
    (loop [replica {}
           state {:id id :log log :outbox-ch outbox-ch :stall-output? true}]
      (let [position (first (alts!! [kill-ch inbox-ch] :priority? true))]
        (when position
          (let [entry (extensions/read-log-entry log position)
                new-replica (extensions/apply-log-entry entry replica)
                diff (extensions/replica-diff entry replica new-replica)
                reactions (extensions/reactions entry replica new-replica diff state)
                new-state (extensions/fire-side-effects! entry replica new-replica diff state)]
            (recur new-replica (send-to-outbox new-state reactions))))))
    (catch Exception e
      (taoensso.timbre/fatal "Fell out of processing loop")
      (taoensso.timbre/fatal e))))

(defn outbox-loop [id log outbox-ch]
  (try
    (loop []
      (when-let [entry (<!! outbox-ch)]
        (extensions/write-log-entry log entry)
        (recur)))
    (catch Exception e
      (taoensso.timbre/fatal "Fell out of outbox loop")
      (taoensso.timbre/fatal e))))

(defrecord VirtualPeer [opts]
  component/Lifecycle

  (start [{:keys [log] :as component}]
    (let [id (java.util.UUID/randomUUID)]
      (taoensso.timbre/info (format "Starting Virtual Peer %s" id))

      (let [inbox-ch (chan (:inbox-capacity opts))
            outbox-ch (chan (:outbox-capacity opts))
            kill-ch (chan 1)
            entry (create-log-entry :prepare-join-cluster {:joiner id})]
        (extensions/subscribe-to-log log 0 inbox-ch)
        (extensions/register-pulse log id)

        (>!! outbox-ch entry)

        (thread (outbox-loop id log outbox-ch))
        (thread (processing-loop id log inbox-ch outbox-ch kill-ch))
        (assoc component :id id :inbox-ch inbox-ch :outbox-ch outbox-ch :kill-ch kill-ch))))

  (stop [component]
    (taoensso.timbre/info (format "Stopping Virtual Peer %s" (:id component)))

    (close! (:inbox-ch component))
    (close! (:outbox-ch component))
    (close! (:kill-ch component))

    component))

(defn virtual-peer [opts]
  (map->VirtualPeer {:opts opts}))

