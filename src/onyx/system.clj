(ns onyx.system
  (:require [com.stuartsierra.component :as component]
            [onyx.coordinator.async :refer [coordinator]]
            [onyx.coordinator.log.datomic :refer [datomic log-schema]]
            [onyx.coordinator.sync.zookeeper :refer [zookeeper zk-addr]]
            [onyx.coordinator.queue.hornetq]))

(def components [:log :sync :coordinator])

(defrecord OnyxSystem [log sync queue]
  component/Lifecycle
  (start [this]
    (component/start-system this components))
  (stop [this]
    (component/stop-system this components)))

(defn onyx-system [{:keys [queue eviction-delay]}]
  (let [datomic-uri (str "datomic:mem://" (java.util.UUID/randomUUID))]
    (map->OnyxSystem
     {:log (datomic datomic-uri (log-schema))
      :sync (zookeeper (zk-addr))
      :queue queue
      :coordinator (component/using (coordinator eviction-delay)
                                    [:log :sync :queue])})))

