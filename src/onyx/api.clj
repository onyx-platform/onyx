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
    (>!! (:planning-ch-head (:coordinator (var-get onyx-coord))) job))

  IRegister
  (register-peer [this peer-node]
    (>!! (:born-peer-ch-head (:coordinator (var-get onyx-coord))) peer-node))

  IShutdown
  (shutdown [this] (alter-var-root onyx-coord component/stop)))

(deftype NettyCoordinator [uri]
  ISubmit
  (submit-job [this job])

  IRegister
  (register-peer [this peer-node]))

(defmulti connect
  (fn [uri opts] (keyword (first (split (second (split uri #":")) #"//")))))

(defmethod connect :mem
  [uri opts]
  (def c (system/onyx-coordinator opts))
  (alter-var-root #'c component/start)
  (InMemoryCoordinator. #'c))

(defmethod connect :netty
  [uri opts] (NettyCoordinator. nil))

(defmulti start-peers
  (fn [coordinator n config] (type coordinator)))

(defmethod start-peers InMemoryCoordinator
  [coord n config]
  (doall
   (map
    (fn [_]
      (def v-peer (system/onyx-peer config))
      (alter-var-root #'v-peer component/start)
      (let [rets {:runner (future @(:payload-thread (:peer v-peer)))
                  :shutdown-fn (fn [] (alter-var-root #'v-peer component/stop))}]
        (register-peer coord (:peer-node (:peer v-peer)))
        rets))
    (range n))))

(defmethod start-peers NettyCoordinator
  [coordinator n config])

