(ns onyx.api
  (:require [clojure.string :refer [split]]
            [clojure.core.async :refer [chan alts!! >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [clj-http.client :refer [post]]
            [taoensso.timbre :refer [warn]]
            [onyx.system :as system]
            [onyx.extensions :as extensions]
            [onyx.validation :as validator]))

(defprotocol ISubmit
  "Protocol for sending a job to the coordinator for execution."
  (submit-job [this job]))

(defprotocol IRegister
  "Protocol for registering a virtual peer with the coordinator.
   Registering allows the virtual peer to accept tasks."
  (register-peer [this peer-node]))

(defprotocol IAwait
  "Protocol for waiting for completion of Onyx internals"
  (await-job-completion [this job-id]))

(defprotocol IShutdown
  "Protocol for stopping a virtual peer's task and no longer allowing
   it to accept new tasks. Releases all resources that were previously
   acquired."
  (shutdown [this]))

(defn await-job-completion* [sync job-id]
  ;; TODO: re-implement me
  )

(defn unpack-workflow
  ([workflow] (vec (unpack-workflow workflow [])))
  ([workflow result]
     (let [roots (keys workflow)]
       (if roots
         (concat result
                 (mapcat
                  (fn [k]
                    (let [child (get workflow k)]
                      (if (map? child)
                        (concat (map (fn [x] [k x]) (keys child))
                                (unpack-workflow child result))
                        [[k child]])))
                  roots))
         result))))

(defn start-peers!
  "Launches n virtual peers. Each peer may be stopped
   by invoking the fn returned by :shutdown-fn."
  [onyx-id n config opts]
  (doall
   (map
    (fn [_]
      (let [stop-ch (chan (clojure.core.async/sliding-buffer 1))
            v-peer (system/onyx-peer onyx-id config opts)]
        {:runner (future
                   (let [live (component/start v-peer)]
                     (let [ack-ch (<!! stop-ch)]
                       (component/stop live)
                       (>!! ack-ch true)
                       (close! ack-ch))))
         :shutdown-fn (fn []
                        (let [ack-ch (chan)]
                          (>!! stop-ch ack-ch)
                          (<!! ack-ch)))}))
    (range n))))

