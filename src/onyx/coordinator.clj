(ns onyx.coordinator
  (:require [clojure.string :refer [split]]
            [clojure.core.async :refer [>!!]]
            [com.stuartsierra.component :as component]
            [onyx.coordinator.async :refer [coordinator]]
            [onyx.coordinator.log.datomic :refer [datomic log-schema]]
            [onyx.coordinator.sync.zookeeper :refer [zookeeper zk-addr]]
            [onyx.queue.hornetq :refer [hornetq]]))

(def components [:log :sync :queue :coordinator])

(defrecord OnyxCoordinator [log sync queue]
  component/Lifecycle
  (start [this]
    (component/start-system this components))
  (stop [this]
    (component/stop-system this components)))

(defn onyx-coordinator [{:keys [onyx-id revoke-delay]}]
  (let [hornetq-addr "localhost:5445"
        datomic-uri (str "datomic:mem://" (java.util.UUID/randomUUID))]
    (map->OnyxCoordinator
     {:log (datomic datomic-uri (log-schema))
      :sync (zookeeper (zk-addr) onyx-id)
      :queue (hornetq hornetq-addr)
      :coordinator (component/using (coordinator revoke-delay onyx-id)
                                    [:log :sync :queue])})))

(defprotocol Submittable
  (submit-job [this job]))

(defprotocol Registerable
  (register-peer [this peer-node]))

(deftype InMemoryCoordinator [onyx-coord]
  Submittable
  (submit-job [this job]
    (>!! (:planning-ch-head (:coordinator onyx-coord)) job))

  Registerable
  (register-peer [this peer-node]
    (>!! (:born-peer-ch-head (:coordinator onyx-coord)) peer-node)))

(deftype NettyCoordinator [uri]
  Submittable
  (submit-job [this job])

  Registerable
  (register-peer [this peer-node]))

(defmulti connect
  (fn [uri opts] (keyword (first (split (second (split uri #":")) #"//")))))

(defmethod connect :mem
  [uri opts]
  (def c (onyx-coordinator opts))
  (alter-var-root #'c component/start)
  (InMemoryCoordinator. c))

(defmethod connect :netty
  [uri opts] (NettyCoordinator. nil))

