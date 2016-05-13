(ns ^:no-doc onyx.peer.virtual-peer
  (:require [clojure.core.async :refer [chan >!! <!! thread alts!! close! dropping-buffer]]
            [com.stuartsierra.component :as component]
            [onyx.extensions :as extensions]
            [taoensso.timbre :as timbre]
            [onyx.peer.operation :as operation]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.static.default-vals :refer [defaults arg-or-default]]))

(defn processing-loop [id restart-ch kill-ch state]
  (try
    (loop []
      (let [entry (<!! kill-ch)]
        (cond 
          (instance? java.lang.Throwable entry) 
          (close! restart-ch)

          (nil? entry) 
          (when (:lifecycle state)
            (component/stop @(:lifecycle state)))

          :else
          (throw (ex-info "Unexpected message in Peer processing loop" {:id id :entry entry})))))
    (catch Throwable e
      (taoensso.timbre/error e (format "Peer %s: error in processing loop. Restarting." id))
      (close! restart-ch))
    (finally
      (taoensso.timbre/info (format "Peer %s: finished of processing loop" id)))))

(defrecord VirtualPeer [peer-config task-component-fn peer-group]
  component/Lifecycle

  (start [{:keys [acking-daemon messenger logging-config] :as component}]
    (let [id (java.util.UUID/randomUUID)
          log (:log peer-group)
          monitoring (:monitoring peer-group)
          replica-services (:replica peer-group)]
      (taoensso.timbre/info (format "Starting Virtual Peer %s" id))
      (try
        (let [completion-ch (:completion-ch acking-daemon)
              kill-ch (chan (dropping-buffer 1))
              restart-ch (chan 1)
              state (merge {:id id
                            :task-component-fn task-component-fn
                            :replica (:replica replica-services)
                            :peer-replica-view (atom {})
                            :log log
                            :buffered-outbox []
                            :messenger messenger
                            :monitoring monitoring
                            :completion-ch completion-ch
                            :opts peer-config
                            :kill-ch kill-ch
                            :restart-ch restart-ch
                            :outbox-ch (:outbox-ch log)
                            :logging-config logging-config}
                           (:onyx.peer/state peer-config))]
          (swap! (:peer-states replica-services) assoc id state)
          (let [processing-loop-ch (thread (processing-loop id restart-ch kill-ch state))]
            (assoc component
                   :id id
                   :log log
                   :processing-loop-ch processing-loop-ch
                   :kill-ch kill-ch
                   :restart-ch restart-ch)))
        (catch Throwable e
          (taoensso.timbre/fatal e (format "Error starting Virtual Peer %s" id))
          (throw e)))))

  (stop [component]
    (taoensso.timbre/info (format "Stopping Virtual Peer %s" (:id component)))

    (close! (:kill-ch component))
    (close! (:restart-ch component))
    (<!! (:processing-loop-ch component))

    (assoc component :kill-ch nil :restart-ch nil :processing-loop-ch nil)))

(defmethod clojure.core/print-method VirtualPeer
  [system ^java.io.Writer writer]
  (.write writer "#<Virtual Peer>"))

(defn virtual-peer [peer-config task-component-fn peer-group]
  (map->VirtualPeer {:peer-config peer-config
                     :task-component-fn task-component-fn
                     :peer-group peer-group}))
