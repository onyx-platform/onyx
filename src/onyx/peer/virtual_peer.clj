(ns ^:no-doc onyx.peer.virtual-peer
  (:require [clojure.core.async :refer [chan >!! <!! thread alts!! close! dropping-buffer]]
            [com.stuartsierra.component :as component]
            [onyx.extensions :as extensions]
            [taoensso.timbre :as timbre]
            [onyx.peer.operation :as operation]
            [onyx.messaging.aeron :as am]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.static.default-vals :refer [defaults arg-or-default]]
            [onyx.static.onyx-component :refer [map->ComponentSystem]]))

(defrecord VirtualPeer [peer-config peer-group task-component-fn]
  component/Lifecycle

  (start [{:keys [acking-daemon messenger] :as component}]
    (let [id (java.util.UUID/randomUUID)]
      (taoensso.timbre/info (format "Starting Virtual Peer %s" id))
      (let [peer-group-snapshot @(:component-state peer-group)
            state (merge {:id id
                          :task-component-fn task-component-fn
                          :replica (:replica (:replica-subscription peer-group-snapshot))
                          :peer-replica-view (atom {})
                          :log (:log peer-group-snapshot)
                          :messenger messenger
                          :monitoring (:monitoring peer-group-snapshot)
                          :opts peer-config
                          :outbox-ch (:outbox-ch (:replica-controller peer-group-snapshot))}
                         (:onyx.peer/state peer-config))
            peer-site (extensions/peer-site messenger)]
        (>!! (:outbox-ch (:replica-controller peer-group-snapshot))
             (create-log-entry :add-virtual-peer
                               {:id id :peer-site peer-site
                                :tags (or (:onyx.peer/tags peer-config) [])}))
        (assoc component :id id :state state))))

  (stop [component]
    (taoensso.timbre/info (format "Stopping Virtual Peer %s" (:id component)))
    component))

(defmethod clojure.core/print-method VirtualPeer
  [system ^java.io.Writer writer]
  (.write writer "#<Virtual Peer>"))

(defn virtual-peer
  [peer-config peer-group task-component-fn]
  (map->VirtualPeer {:peer-config peer-config
                     :peer-group peer-group
                     :task-component-fn task-component-fn}))

(defrecord VirtualPeers [peer-config]
  component/Lifecycle

  (start [{:keys [peer-config monitoring messaging-group] :as component}]
    (let [group-id (java.util.UUID/randomUUID)]
      (assoc component :group-id group-id :vpeer-systems (atom {}))))

  (stop [component]
    (doseq [vps (vals @(:vpeer-systems component))]
      (component/stop vps))
    component))

(defn virtual-peers [peer-config]
  (->VirtualPeers peer-config))
