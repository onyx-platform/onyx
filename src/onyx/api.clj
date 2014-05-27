(ns onyx.api
  (:require [clojure.string :refer [split]]
            [clojure.core.async :refer [>!!]]
            [com.stuartsierra.component :as component]
            [clj-http.client :refer [post]]
            [taoensso.timbre :refer [info]]
            [onyx.system :as system]))

(defprotocol ISubmit
  "Protocol for sending a job to the coordinator for execution."
  (submit-job [this job]))

(defprotocol IRegister
  "Protocol for registering a virtual peer with the coordinator.
   Registering allows the virtual peer to accept tasks."
  (register-peer [this peer-node]))

(defprotocol IShutdown
  "Protocol for stopping a virtual peer's task and no longer allowing
   it to accept new tasks. Releases all resources that were previously
   acquired."
  (shutdown [this]))

;; A coordinator that runs strictly in memory. Peers communicate with
;; the coordinator by directly accessing its channels.
(deftype InMemoryCoordinator [onyx-coord]
  ISubmit
  (submit-job [this job]
    (>!! (:planning-ch-head (:coordinator onyx-coord)) job))

  IRegister
  (register-peer [this peer-node]
    (>!! (:born-peer-ch-head (:coordinator onyx-coord)) peer-node))

  IShutdown
  (shutdown [this] (component/stop onyx-coord)))

;; A coordinator that can run remotely. Peers commuicate with the
;; coordinator by submitting HTTP requests and parsing the responses.
(deftype HttpCoordinator [uri]
  ISubmit
  (submit-job [this job]
    (let [response (post (str "http://" uri "/submit-job") {:body (pr-str job)})]
      (read-string (:body response))))

  IRegister
  (register-peer [this peer-node]
    (let [response (post (str "http://" uri "/register-peer") {:body (pr-str peer-node)})]
      (read-string (:body response))))

  IShutdown
  (shutdown [this]))

(defmulti connect
  "A polymorphic function to connect with the coordinator."
  (fn [uri opts] (keyword (first (split (second (split uri #":")) #"//")))))

(defmethod connect :memory
  [uri opts]
  (let [c (system/onyx-coordinator opts)]
    (InMemoryCoordinator. (component/start c))))

(defmethod connect :distributed
  [uri opts]
  (HttpCoordinator. (first (split (second (split uri #"//")) #"/"))))

(defn start-peers
  "Launches a number of virtual peers. Starts n
   (the number of peers to launch) threads, one for each virtual peer.
   Takes a coordinator type to connect to and a virtual peer configuration."
  [coord n config]
  (doall
   (map
    (fn [_]
      (let [v-peer (component/start (system/onyx-peer config))]
        (let [rets {:runner (future (try @(:payload-thread (:peer v-peer))
                                         (catch Exception e
                                           (info e))))
                    :shutdown-fn (fn [] (component/stop v-peer))}]
          (register-peer coord (:peer-node (:peer v-peer)))
          rets)))
    (range n))))

