(ns ^:no-doc onyx.peer.virtual-peer
  (:require [clojure.core.async :refer [chan thread alts!! close!]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :as timbre]
            [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]))

(defn bundle-entry [entry position]
  {:message-id position
   :fn (:fn entry)
   :args (:args entry)})

(defn warm-up-processing-loop [log inbox-ch outbox-ch p-ch kill-ch]
  (loop [local {:replica {} :local-state {}}]
    (let [position (first (alts!! [kill-ch inbox-ch] :priority? true))]
      (when position
        (let [entry (extensions/read-log-entry log position)
              bundled (bundle-entry entry position)
              new-replica (extensions/apply-log-entry bundled (:replica local))
              diff (extensions/replica-diff bundled (:replica local) new-replica)
              reactions (extensions/reactions entry (:replica local) new-replica diff)]
          (doseq [reaction reactions]
            (extensions/hold-in-outbox outbox-ch reaction))
          (recur (assoc local :replica new-replica)))))))

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

        (thread (warm-up-processing-loop log inbox-ch outbox-ch nil kill-ch))
        (thread (processing-loop))
        (assoc component :id id :inbox-ch inbox :outbox-ch outbox :kill-ch kill-ch))))

  (stop [component]
    (taoensso.timbre/info (format "Stopping Virtual Peer %s" (:id component)))

    (close! (:inbox-ch component))
    (close! (:outbox-ch component))
    (close! (:kill-ch component))

    component))

(defn virtual-peer [opts]
  (map->VirtualPeer {:opts opts}))

