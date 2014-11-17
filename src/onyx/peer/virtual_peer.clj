(ns ^:no-doc onyx.peer.virtual-peer
  (:require [clojure.core.async :refer [chan >!! <!! thread alts!! close!]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :as timbre]
            [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]))

(defn processing-loop [id log inbox-ch outbox-ch kill-ch]
  (try
    (loop [replica {}
           state {:id id :log log}]
;;      (timbre/info (format "[%s] Replica is: %s" id replica))
;;      (timbre/info (format "[%s] Reading from inbox.." id))
      (let [position (first (alts!! [kill-ch inbox-ch] :priority? true))]
;;        (timbre/info (format "[%s] Read position: %s" id position))
        (when position
          (let [entry (extensions/read-log-entry log position)
;;                _ (timbre/info (format "[%s] Entry is: %s" id entry))
                new-replica (extensions/apply-log-entry entry replica)
;;                _ (timbre/info (format "[%s] New replica is: %s" id new-replica))
                diff (extensions/replica-diff entry replica new-replica)
;;                _ (timbre/info (format "[%s] Diff is: %s" id diff))
                reactions (extensions/reactions entry replica new-replica diff state)
;;                _ (timbre/info (format "[%s] Reactions are: %s" id reactions))
                new-state (extensions/fire-side-effects! entry replica new-replica diff state)]
;;            (timbre/info (format "[%s] New state is: %s" id new-state))
            (doseq [reaction reactions]
              (clojure.core.async/>!! outbox-ch reaction))
            (recur new-replica (update-in state [:buffered-outbox] conj reactions))))))
    (catch Exception e
      (taoensso.timbre/fatal "Fell out of processing loop")
      (taoensso.timbre/fatal e))))

(defn outbox-loop [id log outbox-ch]
  (try
    (loop []
;;      (timbre/info (format "[%s] Tip of outbox loop..." id))
      (when-let [entry (<!! outbox-ch)]
;;        (timbre/info (format "[%s] Read in outbox loop: %s" id entry))
        (extensions/write-log-entry log entry)
;;        (timbre/info (format "[%s] Wrote entry" id))
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
;;        (timbre/info (format "[%s] Subscribing to the log..." id))
        (extensions/subscribe-to-log log 0 inbox-ch)
;;        (timbre/info (format "[%s] Registering pulse..." id))
        (extensions/register-pulse log id)
;;        (timbre/info (format "[%s] Registering writing to outbox..." id))

        (>!! outbox-ch entry)

;;        (timbre/info (format "[%s] Starting processing loop..." id))
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

