(ns ^:no-doc onyx.peer.virtual-peer
  (:require [clojure.core.async :refer [chan >!! <!! thread alts!! close! dropping-buffer]]
            [com.stuartsierra.component :as component]
            [onyx.extensions :as extensions]
            [taoensso.timbre :as timbre]
            [onyx.peer.operation :as operation]
            [onyx.messaging.aeron :as am]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.static.default-vals :refer [defaults arg-or-default]]))

(defrecord VirtualPeer [peer-config task-component-fn]
  component/Lifecycle

  (start [{:keys [group-id logging-config monitoring log acking-daemon messenger
                  virtual-peers replica-subscription replica-chamber]
           :as component}]
    (let [id (java.util.UUID/randomUUID)]
      (taoensso.timbre/info (format "Starting Virtual Peer %s" id))
      (let [state
            (atom (merge {:id id
                          :group-id group-id
                          :task-component-fn task-component-fn
                          :replica (:replica replica-subscription)
                          :peer-replica-view (atom {})
                          :log log
                          :messenger messenger
                          :monitoring monitoring
                          :opts peer-config
                          :outbox-ch (:outbox-ch replica-chamber)
                          :completion-ch (:completion-ch acking-daemon)
                          :logging-config logging-config}
                         (:onyx.peer/state peer-config)))
            peer-site (extensions/peer-site messenger)]
        (assoc component
               :id id
               :group-id group-id
               :peer-config peer-config
               :peer-site peer-site
               :state state))))

  (stop [component]
    (taoensso.timbre/info (format "Stopping Virtual Peer %s" (:id component)))
    (let [vps (:vpeer-systems (:virtual-peers component))]
      (swap! vps dissoc (:id component))
      (swap! (:state component)
             (fn [state-snapshot]
               (when-let [f (:lifecycle-stop-fn state-snapshot)]
                 (f :peer-left))
               nil))
      (assoc component :state nil))))

(defmethod clojure.core/print-method VirtualPeer
  [system ^java.io.Writer writer]
  (.write writer "#<Virtual Peer>"))

(defn virtual-peer
  [peer-config task-component-fn]
  (map->VirtualPeer {:peer-config peer-config
                     :task-component-fn task-component-fn}))

(defrecord VirtualPeers [peer-config]
  component/Lifecycle

  (start [{:keys [peer-config monitoring messaging-group] :as component}]
    (let [group-id (java.util.UUID/randomUUID)]
      (assoc component :group-id group-id :vpeer-systems (atom {}))))

  (stop [component]
    component))

(defn virtual-peers [peer-config]
  (->VirtualPeers peer-config))
