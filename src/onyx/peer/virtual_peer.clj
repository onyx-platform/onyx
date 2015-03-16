(ns ^:no-doc onyx.peer.virtual-peer
    (:require [clojure.core.async :refer [chan >!! <!! thread alts!! close! dropping-buffer]]
              [com.stuartsierra.component :as component]
              [taoensso.timbre :as timbre]
              [onyx.extensions :as extensions]
              [onyx.extensions :as operation]
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

(defn processing-loop [id log buffer messenger origin inbox-ch outbox-ch restart-ch kill-ch completion-ch opts]
  (try
    (let [replica-atom (atom {})]
      (reset! replica-atom origin)
      (loop [state (merge {:id id
                           :replica replica-atom
                           :log log
                           :messenger-buffer buffer
                           :messenger messenger
                           :outbox-ch outbox-ch
                           :completion-ch completion-ch
                           :opts opts
                           :kill-ch kill-ch
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
    (catch clojure.lang.ExceptionInfo e
      ;; ZooKeeper connection dropped, close out cleanly.
      )
    (catch Exception e
      (taoensso.timbre/info e))
    (finally
     (taoensso.timbre/info "Fell out of processing loop"))))

(defn outbox-loop [id log outbox-ch]
  (try
    (loop []
      (when-let [entry (<!! outbox-ch)]
        (extensions/write-log-entry log entry)
        (recur)))
    (catch Exception e
      (taoensso.timbre/info e))
    (finally
     (taoensso.timbre/info "Fell out of outbox loop"))))

(defrecord VirtualPeer [opts]
  component/Lifecycle

  (start [{:keys [log acking-daemon messenger-buffer messenger] :as component}]
    (let [id (java.util.UUID/randomUUID)]
      (taoensso.timbre/info (format "Starting Virtual Peer %s" id))

      ;; Race to write the job scheduler to durable storage so that
      ;; non-peers subscribers can discover which job scheduler to use.
      ;; Only one peer will succeed, and only one needs to.
      (extensions/write-chunk log :job-scheduler {:job-scheduler (:onyx.peer/job-scheduler opts)} nil)

      (let [inbox-ch (chan (or (:onyx.peer/inbox-capacity opts) 1000))
            outbox-ch (chan (or (:onyx.peer/outbox-capacity opts) 1000))
            kill-ch (chan (dropping-buffer 1))
            restart-ch (chan 1)
            completion-ch (:completions-ch acking-daemon)
            send-peer-site (extensions/send-peer-site messenger)
            acker-peer-site (extensions/acker-peer-site messenger)
            completion-peer-site (extensions/completion-peer-site messenger)
            entry (create-log-entry :prepare-join-cluster
                                    {:joiner id
                                     :send-site send-peer-site
                                     :acker-site acker-peer-site
                                     :completion-site completion-peer-site})
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

