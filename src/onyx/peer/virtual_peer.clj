(ns ^:no-doc onyx.peer.virtual-peer
  (:require [clojure.core.async :refer [chan >!! <!! thread alts!! close! dropping-buffer]]
            [com.stuartsierra.component :as component]
            [onyx.extensions :as extensions]
            [taoensso.timbre :as timbre]
            [onyx.peer.operation :as operation]
            [onyx.messaging.aeron :as am]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.static.default-vals :refer [defaults arg-or-default]]))

(defn join-entry [state]
  {:fn :add-virtual-peer 
   :args {:id (:id state)
          :group-id (:group-id state)
          :peer-site (:peer-site state)
          :tags (:onyx.peer/tags (:peer-config state))}})

(defrecord VirtualPeer [command-ch outbox-ch peer-config task-component-fn id]
  component/Lifecycle

  (start [{:keys [group-id logging-config monitoring log acking-daemon messenger]
           :as component}]
    (taoensso.timbre/info (format "Starting Virtual Peer %s" id))
    (let [peer-site (extensions/peer-site messenger)
          state {:id id
                 :type :peer
                 :group-id group-id
                 :task-component-fn task-component-fn
                 :peer-replica-view (atom {})
                 :replica (atom {})
                 :log log
                 :messenger messenger
                 :monitoring monitoring
                 :opts peer-config
                 :outbox-ch outbox-ch
                 :command-ch command-ch
                 :completion-ch (:completion-ch acking-daemon)
                 :logging-config logging-config
                 :peer-site peer-site}]
      (assoc component
             :id id
             :group-id group-id
             :peer-config peer-config
             :peer-site peer-site
             :command-ch command-ch
             :outbox-ch outbox-ch
             :state state)))

  (stop [component]
    (taoensso.timbre/info (format "Stopping Virtual Peer %s" (:id component)))
    (assoc component 
           :state nil :command-ch nil :outbox-ch nil :id nil 
           :group-id nil :peer-config nil :peer-site nil)))

(defmethod clojure.core/print-method VirtualPeer
  [system ^java.io.Writer writer]
  (.write writer "#<Virtual Peer>"))

(defn virtual-peer
  [command-ch outbox-ch log peer-config task-component-fn id]
  (map->VirtualPeer {:id id
                     :log log
                     :outbox-ch outbox-ch
                     :command-ch command-ch
                     :peer-config peer-config
                     :task-component-fn task-component-fn}))
