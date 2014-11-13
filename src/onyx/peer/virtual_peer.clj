(ns ^:no-doc onyx.peer.virtual-peer
  (:require [clojure.core.async :refer [chan thread close!]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :as timbre]
            [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]))

(defn processing-loop []
  (let [{:keys [fn args]} (extensions/read-next-entry)]
    (extensions/apply-log-entry fn args)))

(defrecord VirtualPeer [opts]
  component/Lifecycle

  (start [{:keys [log inbox outbox] :as component}]
    (let [id (java.util.UUID/randomUUID)]
      (taoensso.timbre/info (format "Starting Virtual Peer %s" id))

      (let [entry (create-log-entry :prepare-join-cluster {:joiner id})]
        (extensions/register-pulse log id)
        (extensions/send-to-outbox outbox entry)

        (let [p-thread (thread (processing-loop))])
        (assoc component :id id :processing-thread p-thread))))

  (stop [component]
    (taoensso.timbre/info (format "Stopping Virtual Peer %s" (:uuid (:peer component))))
    component))

(defn virtual-peer [opts]
  (map->VirtualPeer {:opts opts}))

