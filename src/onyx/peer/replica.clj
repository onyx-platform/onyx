(ns ^:no-doc onyx.peer.replica
  (:require [clojure.core.async :refer [>!! <!! alts!! promise-chan close! chan thread]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info error warn fatal]]
            [onyx.extensions :as extensions]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.log.entry :refer [create-log-entry]]))

(defn send-to-outbox [outbox-ch reactions]
  (doseq [reaction reactions]
    (>!! outbox-ch reaction)))

(defn annotate-reaction [{:keys [message-id]} id entry]
  (let [peer-annotated (assoc entry :peer-parent id)]
    ;; Not all messages are derived from other messages.
    ;; For instance, :prepare-join-cluster is a "root"
    ;; message.
    (if message-id
      (assoc peer-annotated :entry-parent message-id)
      peer-annotated)))

(defn transition-peers [log entry old-replica new-replica diff peer-states peer-config]
  (reduce
   (fn [result [id {:keys [peer-replica-view] :as peer-state}]]
     (let [rs (extensions/reactions entry old-replica new-replica diff peer-state)
           annotated-rs (map (partial annotate-reaction entry (:id peer-state)) rs)
           new-peer-view (extensions/peer-replica-view log entry old-replica new-replica peer-replica-view diff peer-state peer-config)
           new-state (extensions/fire-side-effects! entry old-replica new-replica diff peer-state)
           new-state (assoc new-state :peer-replica-view new-peer-view)]
       (-> result
           (update-in [:reactions] into annotated-rs)
           (assoc-in [:states id] new-state))))
   {:reactions []
    :states {}}
   peer-states))

(defn processing-loop
  [log replica-atom inbox-ch outbox-ch component-kill-ch restart-ch opts peer-states peer-config]
  (try
    (loop []
      (let [replica @replica-atom
            [entry ch] (alts!! [component-kill-ch inbox-ch] :priority true)]
        (when (and (= ch inbox-ch) entry)
          (let [new-replica (extensions/apply-log-entry entry replica)
                diff (extensions/replica-diff entry replica new-replica)
                {:keys [reactions states]} (transition-peers log entry replica new-replica diff @peer-states peer-config)]
            (doseq [[peer-id new-state] states]
              (swap! peer-states assoc peer-id new-state))
            (reset! replica-atom new-replica)
            (send-to-outbox outbox-ch reactions)
            (recur)))))
    (catch Throwable e
      (error e "Error in Replica Services processing loop.")
      (close! restart-ch))
    (finally
      (info "Replica Services finished processing loop."))))

(defn outbox-loop [log outbox-ch restart-ch]
  (loop []
    (when-let [entry (<!! outbox-ch)]
      (try
        (extensions/write-log-entry log entry)
        (catch Throwable e
          (warn e "Replica services couldn't write to ZooKeeper.")
          (close! restart-ch)))
      (recur))))

(defrecord Replica [peer-config restart-ch]
  component/Lifecycle

  (start [{:keys [log monitoring] :as component}]
    (taoensso.timbre/info "Starting Replica Services")
    (try
      ;; Race to write the job scheduler and messaging to durable storage so that
      ;; non-peers subscribers can discover which properties to use.
      ;; Only one writer will succeed, and only one needs to.
      (extensions/write-chunk log :job-scheduler {:job-scheduler (:onyx.peer/job-scheduler peer-config)} nil)
      (extensions/write-chunk log :messaging {:messaging (select-keys peer-config [:onyx.messaging/impl])} nil)

      (let [group-id (java.util.UUID/randomUUID)
            inbox-ch (chan (arg-or-default :onyx.peer/inbox-capacity peer-config))
            outbox-ch (chan (arg-or-default :onyx.peer/outbox-capacity peer-config))
            component-kill-ch (promise-chan)
            peer-states (atom {})
            origin (extensions/subscribe-to-log log inbox-ch)
            replica-atom (atom origin)
            entry (create-log-entry :prepare-join-cluster
                                    {:joiner group-id
                                     :tags (or (:onyx.peer/tags peer-config) [])})
            outbox-loop-ch (thread (outbox-loop log outbox-ch restart-ch))
            processing-loop-ch
            (thread
              (processing-loop log replica-atom inbox-ch outbox-ch
                               component-kill-ch restart-ch peer-config
                               peer-states peer-config))]
        (extensions/register-pulse log group-id)
        (>!! outbox-ch entry)
        (assoc component
               :log log
               :replica replica-atom
               :inbox-ch inbox-ch
               :outbox-ch outbox-ch
               :outbox-loop-ch outbox-loop-ch
               :processing-loop-ch processing-loop-ch
               :component-kill-ch component-kill-ch
               :group-id group-id
               :peer-states peer-states))
      (catch Throwable e
        (fatal e "Error starting Replica Services")
        (throw e))))

  (stop [component]
    (taoensso.timbre/info "Stopping Replica Services")

    (close! (:inbox-ch component))
    (close! (:outbox-ch component))
    (close! (:component-kill-ch component))
    (<!! (:outbox-loop-ch component))
    (<!! (:processing-loop-ch component))

    (assoc component
           :inbox-ch nil
           :outbox-ch nil
           :outbox-loop-ch nil 
           :component-kill-ch nil
           :processing-loop-ch nil)))

(defn replica [peer-config restart-ch]
  (->Replica peer-config restart-ch))
