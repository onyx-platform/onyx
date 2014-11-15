(ns ^:no-doc onyx.peer.virtual-peer
  (:require [clojure.core.async :refer [chan thread alts!! close!]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :as timbre]
            [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]))

#_(defn processing-loop [id log inbox-ch outbox-ch p-ch kill-ch]
  (loop [local {:replica {} :local-state {:id id :log log}}]
    (let [old-replica (:replica local)
          old-state (:local-state local)
          position (first (alts!! [kill-ch inbox-ch] :priority? true))]
      (when position
        (let [entry (extensions/read-log-entry log position)
              bundled (bundle-entry entry position)
              new-replica (extensions/apply-log-entry bundled old-replica)
              diff (extensions/replica-diff bundled old-replica new-replica)
              reactions (extensions/reactions entry old-replica new-replica diff)
              new-state (extensions/fire-side-effects! bundled old-replica new-replica diff old-state)]
          (doseq [reaction reactions]
            (extensions/hold-in-outbox outbox-ch reaction))
          (recur (assoc local :replica new-replica :local-state new-state)))))))

(defrecord VirtualPeer [opts]
  component/Lifecycle

  (start [{:keys [log inbox outbox] :as component}]
    (let [id (java.util.UUID/randomUUID)]
      (taoensso.timbre/info (format "Starting Virtual Peer %s" id))

      (let [inbox-ch (chan (:inbox-capacity opts))
            outbox-ch (chan (:outbox-capacity opts))
            kill-ch (chan 1)
            entry (create-log-entry :prepare-join-cluster {:joiner id})]
        (extensions/register-pulse log id)
        (extensions/write-to-outbox entry)

        (assoc component :id id :inbox-ch inbox :outbox-ch outbox :kill-ch kill-ch))))

  (stop [component]
    (taoensso.timbre/info (format "Stopping Virtual Peer %s" (:id component)))

    (close! (:inbox-ch component))
    (close! (:outbox-ch component))
    (close! (:kill-ch component))

    component))

(defn virtual-peer [opts]
  (map->VirtualPeer {:opts opts}))

