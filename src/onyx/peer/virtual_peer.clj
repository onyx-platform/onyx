(ns ^:no-doc onyx.peer.virtual-peer
  (:require [clojure.core.async :refer [chan >!! <!! thread alts!! close!]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :as timbre]
            [onyx.extensions :as extensions]
            [onyx.peer.task-lifecycle :refer [task-lifecycle]]
            [onyx.log.entry :refer [create-log-entry]]))

(defn send-to-outbox [{:keys [outbox-ch] :as state} reactions]
  (if (:stall-output? state)
    (do
      (doseq [reaction (filter :immediate? reactions)]
        (clojure.core.async/>!! outbox-ch reaction))
      (update-in state [:buffered-outbox] concat (remove :immediate? reactions)))
    (do
      (doseq [reaction reactions]
        (clojure.core.async/>!! outbox-ch reaction))
      state)))

(defn forward-completion-calls [messenger ch replica]
  (try
    (loop []
      (when-let [{:keys [id peer-id]} (<!! ch)]
        (extensions/internal-complete-message messenger id peer-id replica)
        (recur)))
    (catch Exception e
      (timbre/fatal e))))

(defn processing-loop [id log buffer messenger origin inbox-ch outbox-ch restart-ch kill-ch completion-ch opts]
  (try
    (let [replica-atom (atom {})]
      (reset! replica-atom origin)
      (thread (forward-completion-calls messenger completion-ch replica-atom))
      (loop [state (merge {:id id
                           :replica replica-atom
                           :log log
                           :messenger-buffer buffer
                           :messenger messenger
                           :outbox-ch outbox-ch
                           :opts opts
                           :restart-ch restart-ch
                           :stall-output? true
                           :task-lifecycle-fn task-lifecycle}
                          (:onyx.peer/state opts))]
        (let [replica @replica-atom
              position (first (alts!! [kill-ch inbox-ch] :priority? true))]
          (if position
            (let [entry (extensions/read-log-entry log position)
                  new-replica (extensions/apply-log-entry entry replica)
                  diff (extensions/replica-diff entry replica new-replica)
                  reactions (extensions/reactions entry replica new-replica diff state)
                  new-state (extensions/fire-side-effects! entry replica new-replica diff state)]
              (reset! replica-atom new-replica)
              (recur (send-to-outbox new-state reactions)))
            (when (:lifecycle state)
              (component/stop @(:lifecycle state)))))))
    (catch org.apache.zookeeper.KeeperException$ConnectionLossException e
        ;; ZooKeeper connection dropped, close out cleanly.
        )
    (catch Exception e
      (taoensso.timbre/fatal e))
    (finally
     (taoensso.timbre/fatal "Fell out of processing loop"))))

(defn outbox-loop [id log outbox-ch]
  (try
    (loop []
      (when-let [entry (<!! outbox-ch)]
        (extensions/write-log-entry log entry)
        (recur)))
    (catch Exception e
      (taoensso.timbre/fatal e))
    (finally
     (taoensso.timbre/fatal "Fell out of outbox loop"))))

(defrecord VirtualPeer [opts]
  component/Lifecycle

  (start [{:keys [log acking-daemon messenger-buffer messenger] :as component}]
    (let [id (java.util.UUID/randomUUID)]
      (taoensso.timbre/info (format "Starting Virtual Peer %s" id))

      (let [inbox-ch (chan (or (:onyx.peer/inbox-capacity opts) 1000))
            outbox-ch (chan (or (:onyx.peer/outbox-capacity opts) 1000))
            kill-ch (chan 1)
            restart-ch (chan 1)
            completion-ch (:completions-ch acking-daemon)
            site (onyx.extensions/peer-site messenger)
            entry (create-log-entry :prepare-join-cluster {:joiner id :peer-site site})
            origin (extensions/subscribe-to-log log inbox-ch)]
        (extensions/register-pulse log id)

        (>!! outbox-ch entry)

        (thread (outbox-loop id log outbox-ch))
        (thread (processing-loop id log messenger-buffer messenger origin inbox-ch outbox-ch restart-ch kill-ch completion-ch opts))
        (assoc component :id id :inbox-ch inbox-ch
               :outbox-ch outbox-ch :kill-ch kill-ch
               :restart-ch restart-ch))))

  (stop [component]
    (taoensso.timbre/info (format "Stopping Virtual Peer %s" (:id component)))

    (close! (:inbox-ch component))
    (close! (:outbox-ch component))
    (close! (:kill-ch component))
    (close! (:restart-ch component))

    component))

(defn virtual-peer [opts]
  (map->VirtualPeer {:opts opts}))

