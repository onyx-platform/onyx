(ns ^:no-doc onyx.peer.virtual-peer
  (:require [clojure.core.async :refer [chan >!! <!! thread alts!! close! dropping-buffer promise-chan]]
            [com.stuartsierra.component :as component]
            [onyx.extensions :as extensions]
            [taoensso.timbre :as timbre :refer [info]]
            [onyx.peer.operation :as operation]
            [onyx.messaging.aeron :as am]
            [onyx.log.commands.common :as common]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.static.default-vals :refer [defaults arg-or-default]]))

(defrecord VirtualPeer [group-ch outbox-ch peer-config task-component-fn id]
  component/Lifecycle

  (start [{:keys [group-id logging-config monitoring log acking-daemon messenger]
           :as component}]
    (taoensso.timbre/info (format "Starting Virtual Peer %s" id))
    (let [peer-site (extensions/peer-site messenger)
          kill-ch (promise-chan)
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
                 :kill-ch kill-ch
                 :outbox-ch outbox-ch
                 :group-ch group-ch
                 :completion-ch (:completion-ch acking-daemon)
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
             :kill-ch kill-ch
             :group-ch group-ch
             :outbox-ch outbox-ch
             :state state)))

  (stop [{:keys [outbox-ch kill-ch group-id id state] :as component}]
    (taoensso.timbre/info (format "Stopping Virtual Peer %s" (:id component)))

    (close! kill-ch)

    (when-let [f (:lifecycle-stop-fn state)]
      (common/stop-lifecycle-safe! f :peer-left state))

    (>!! outbox-ch
         {:fn :leave-cluster
          :peer-parent id
          :args {:id id
                 :group-id group-id}})

    (assoc component 
           :state nil :group-ch nil :outbox-ch nil :kill-ch nil :id nil 
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
