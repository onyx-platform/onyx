(ns onyx.api
  (:require [clojure.string :refer [split]]
            [clojure.core.async :refer [>!!]]
            [onyx.system :as system]))

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
  (def c (system/onyx-coordinator opts))
  (alter-var-root #'c component/start)
  (InMemoryCoordinator. c))

(defmethod connect :netty
  [uri opts] (NettyCoordinator. nil))

(defmulti start-peers
  (fn [coordinator n config] (type coordinator)))

(defmethod start-peers InMemoryCoordinator
  [coord n config]
  (doall
   (map
    (fn [_]
      (def v-peer (system/onyx-peer (:onyx-id config)))
      (alter-var-root #'v-peer component/start)
      (let [rets {:runner (future @(:payload-thread (:peer v-peer))
                                  (alter-var-root #'v-peer component/stop))}]
        (coordinator/register-peer coord (:peer-node (:peer v-peer)))
        rets))
    (range n))))

(defmethod start-peers NettyCoordinator
  [coordinator n config])

