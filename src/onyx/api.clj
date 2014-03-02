(ns onyx.api
  (:require [clojure.string :refer [split]]
            [clojure.core.async :refer [>!!]]
            [com.stuartsierra.component :as component]
            [onyx.system :as system]))

(defprotocol ISubmit
  (submit-job [this job]))

(defprotocol IRegister
  (register-peer [this peer-node]))

(defprotocol IShutdown
  (shutdown [this]))

(deftype InMemoryCoordinator [onyx-coord]
  ISubmit
  (submit-job [this job]
    (>!! (:planning-ch-head (:coordinator onyx-coord)) job))

  IRegister
  (register-peer [this peer-node]
    (>!! (:born-peer-ch-head (:coordinator onyx-coord)) peer-node))

  IShutdown
  (shutdown [this] (component/stop onyx-coord)))

(deftype NettyCoordinator [uri]
  ISubmit
  (submit-job [this job])

  IRegister
  (register-peer [this peer-node]))

(defmulti connect
  (fn [uri opts] (keyword (first (split (second (split uri #":")) #"//")))))

(defmethod connect :memory
  [uri opts]
  (let [c (system/onyx-coordinator opts)]
    (InMemoryCoordinator. (component/start c))))

(defmethod connect :distributed
  [uri opts] (NettyCoordinator. nil))

(defmulti start-peers
  (fn [coordinator n config] (type coordinator)))

(defmethod start-peers InMemoryCoordinator
  [coord n config]
  (doall
   (map
    (fn [_]
      (let [v-peer (component/start (system/onyx-peer config))]
        (let [rets {:runner (future (try @(:payload-thread (:peer v-peer)) (catch Exception e (.printStackTrace e))))
                    :shutdown-fn (fn [] (component/stop v-peer))}]
          (register-peer coord (:peer-node (:peer v-peer)))
          rets)))
    (range n))))

(defmethod start-peers NettyCoordinator
  [coordinator n config])

