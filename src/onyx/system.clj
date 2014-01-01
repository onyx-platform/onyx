(ns onyx.system
  (:require [com.stuartsierra.component :as component]
            [onyx.coordinator.async :refer [coordinator]]
            [onyx.coordinator.log.datomic :refer [datomic log-schema]]
            [onyx.coordinator.sync.zookeeper :refer [zookeeper zk-addr]]))

(def components [:log :sync :coordinator])

(defrecord OnyxSystem [log sync queue]
  component/Lifecycle
  (start [this]
    (component/start-system this components))
  (stop [this]
    (component/stop-system this components)))

(defn onyx-system [{:keys [sync queue]}]
  (let [datomic-uri (str "datomic:mem://" (java.util.UUID/randomUUID))
        log (datomic datomic-uri (log-schema))
        sync (zookeeper (zk-addr))
        coordinator (coordinator log sync queue)]
    (map->OnyxSystem
     {:coordinator coordinator
      :log log
      :sync sync
      :queue queue})))

