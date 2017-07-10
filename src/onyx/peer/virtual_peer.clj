(ns ^:no-doc onyx.peer.virtual-peer
  (:require [clojure.core.async :refer [chan >!! <!! thread alts!! close! dropping-buffer promise-chan]]
            [com.stuartsierra.component :as component]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.extensions :as extensions]
            [taoensso.timbre :as timbre :refer [info]]
            [onyx.peer.operation :as operation]
            [onyx.log.commands.common :as common]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.static.default-vals :refer [arg-or-default]]))

(defrecord VirtualPeer [group-ch outbox-ch peer-config task-component-fn id]
  component/Lifecycle

  (start [{:keys [group-id logging-config monitoring messenger-group state-store-group log]
           :as component}]
    (taoensso.timbre/info (format "Starting Virtual Peer %s" id))
    (let [peer-site (m/get-peer-site peer-config)
          state {:id id
                 :type :peer
                 :group-id group-id
                 :task-component-fn task-component-fn
                 :replica (atom {})
                 :log log
                 :messenger-group messenger-group
                 :monitoring monitoring
                 :opts peer-config
                 :outbox-ch outbox-ch
                 :group-ch group-ch
                 :state-store-ch (:ch state-store-group)
                 :logging-config logging-config
                 :peer-site peer-site}]
      (>!! outbox-ch 
           {:fn :add-virtual-peer
            :peer-parent id
            :args {:id id
                   :group-id group-id 
                   :peer-site peer-site 
                   :tags (:onyx.peer/tags peer-config)}})
      (assoc component
             :id id
             :group-id group-id
             :peer-config peer-config
             :peer-site peer-site
             :group-ch group-ch
             :outbox-ch outbox-ch
             :state state)))

  (stop [{:keys [outbox-ch group-id id state] :as component}]
    (taoensso.timbre/info (format "Stopping Virtual Peer %s" (:id component)))
    (common/stop-lifecycle-peer-group! state :peer-left)
    (>!! outbox-ch
         {:fn :leave-cluster
          :peer-parent id
          :args {:id id
                 :group-id group-id}})

    (assoc component 
           :state nil :group-ch nil :outbox-ch nil :id nil 
           :group-id nil :peer-config nil :peer-site nil)))

(defmethod clojure.core/print-method VirtualPeer
  [system ^java.io.Writer writer]
  (.write writer "#<Virtual Peer>"))

(defn virtual-peer
  [group-ch outbox-ch log peer-config task-component-fn id]
  (map->VirtualPeer {:id id
                     :log log
                     :outbox-ch outbox-ch
                     :group-ch group-ch
                     :peer-config peer-config
                     :task-component-fn task-component-fn}))
