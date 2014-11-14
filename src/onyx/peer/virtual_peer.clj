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

(defn warm-up-processing-loop [inbox p-ch kill-ch]
  (loop [local {:replica {} :local-state {}}]
    (let [position (first (alts!! [kill-ch (:ch inbox)] :priority? true))]
      (when position
        (let [entry (extensions/read-log-entry (:log inbox) position)
              bundled (bundle-entry entry position)
              next-replica (extensions/apply-log-entry bundled (:replica local))
              diff (extensions/replica-diff bundled )
              reactions]
          (doseq [reaction reactions]
            (extensions/hold-in-outbox outbox reaction))))
      (recur (assoc local :replica next-replica)))))

(defrecord VirtualPeer [opts]
  component/Lifecycle

  (start [{:keys [log inbox outbox] :as component}]
    (let [id (java.util.UUID/randomUUID)]
      (taoensso.timbre/info (format "Starting Virtual Peer %s" id))

      (let [entry (create-log-entry :prepare-join-cluster {:joiner id})]
        (extensions/register-pulse log id)
        (extensions/write-to-outbox entry)

        (let [w-thread (thread (warm-up-processing-loop))
              p-thread (thread (processing-loop))]
          (assoc component :id id :w-thread w-thread :p-thread p-thread)))))

  (stop [component]
    (taoensso.timbre/info (format "Stopping Virtual Peer %s" (:id component)))
    component))

(defn virtual-peer [opts]
  (map->VirtualPeer {:opts opts}))

