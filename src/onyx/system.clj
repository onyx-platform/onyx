(ns onyx.system
  (:require [com.stuartsierra.component :as component]
            [onyx.coordinator.async :refer [coordinator]]
            [onyx.coordinator.log.datomic :refer [datomic log-schema]]
            [onyx.coordinator.sync.zookeeper :refer [zookeeper zk-addr]]
            [onyx.queue.hornetq :refer [hornetq]]))

(def components [:log :sync :queue :coordinator])

(defrecord OnyxSystem [log sync queue]
  component/Lifecycle
  (start [this]
    (component/start-system this components))
  (stop [this]
    (component/stop-system this components)))

(defn in-memory-coordinator [{:keys [revoke-delay]}]
  (let [onyx-id (str (java.util.UUID/randomUUID))
        hornetq-addr "localhost:5445"
        datomic-uri (str "datomic:mem://" (java.util.UUID/randomUUID))]
    (map->OnyxSystem
     {:log (datomic datomic-uri (log-schema))
      :sync (zookeeper (zk-addr) onyx-id)
      :queue (hornetq hornetq-addr)
      :coordinator (component/using (coordinator revoke-delay)
                                    [:log :sync :queue])})))

