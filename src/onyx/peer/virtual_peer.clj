(ns ^:no-doc onyx.peer.virtual-peer
  (:require [clojure.core.async :refer [chan mult tap alts!! >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [warn] :as timbre]
            [onyx.extensions :as extensions]))

(defrecord VirtualPeer [opts]
  component/Lifecycle

  (start [{:keys [log] :as component}]
    (let [id (java.util.UUID/randomUUID)]
      (extensions/register-pulse log id)

      (taoensso.timbre/info (format "Starting Virtual Peer %s" id))
      (assoc component :id id)))

  (stop [component]
    (taoensso.timbre/info (format "Stopping Virtual Peer %s" (:uuid (:peer component))))
    component))

(defn virtual-peer [opts]
  (map->VirtualPeer {:opts opts}))

