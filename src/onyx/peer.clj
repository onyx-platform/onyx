(ns onyx.peer
  (:require [com.stuartsierra.component :as component]
            [onyx.coordinator :as coordinator]
            [onyx.peer.virtual-peer :refer [virtual-peer] :as v]
            [onyx.coordinator.sync.zookeeper :refer [zookeeper zk-addr]]
            [onyx.queue.hornetq :refer [hornetq]])
  (:import [onyx.coordinator InMemoryCoordinator]
           [onyx.coordinator NettyCoordinator]))

(def components [:sync :queue :peer])

(defrecord OnyxPeer [sync queue]
  component/Lifecycle
  (start [this]
    (component/start-system this components))
  (stop [this]
    (component/stop-system this components)))

(defn onyx-peer [onyx-id]
  (let [hornetq-addr "localhost:5445"]
    (map->OnyxPeer
     {:sync (zookeeper (zk-addr) onyx-id)
      :queue (hornetq hornetq-addr)
      :peer (component/using (virtual-peer)
                             [:sync :queue])})))

(defmulti connect
  (fn [coordinator n config] (type coordinator)))

(defmethod connect InMemoryCoordinator
  [coord n config]
  (doall
   (map
    (fn [_]
      (def v-peer (onyx-peer (:onyx-id config)))
      (alter-var-root #'v-peer component/start)
      (let [rets {:runner (future @(:payload-thread (:peer v-peer))
                                  (alter-var-root #'v-peer component/stop))}]
        (coordinator/register-peer coord (:peer-node (:peer v-peer)))
        rets))
    (range n))))

(defmethod connect NettyCoordinator
  [coordinator n config])



;; (def opts {:revoke-delay 3000 :onyx-id (str (java.util.UUID/randomUUID))})

;; (def coordinator-conn
;;   (onyx.coordinator/connect
;;    (str "onyx:mem//localhost/" (:onyx-id opts))
;;    opts))

;; (def vpeers (onyx.peer/connect coordinator-conn 2 {:onyx-id (:onyx-id opts)}))

;; (map deref (map :runner vpeers))

