(ns ^:no-doc onyx.peer.virtual-peer
    (:require [clojure.core.async :refer [chan >!! <!! thread alts!! close! dropping-buffer]]
              [com.stuartsierra.component :as component]
              [onyx.extensions :as extensions]
              [taoensso.timbre :as timbre]
              [onyx.peer.operation :as operation]
              [onyx.peer.task-lifecycle :refer [task-lifecycle]]
              [onyx.log.entry :refer [create-log-entry]]
              [onyx.static.default-vals :refer [defaults arg-or-default]]))

(defn send-to-outbox [{:keys [outbox-ch] :as state} reactions]
  (doseq [reaction reactions]
    (clojure.core.async/>!! outbox-ch reaction))
  state)

(defn processing-loop [id log buffer messenger origin inbox-ch outbox-ch restart-ch kill-ch completion-ch opts monitoring]
  (try
    (let [replica-atom (atom nil)
          peer-view-atom (atom {})]
      (reset! replica-atom origin)
      (loop [state (merge {:id id
                           :replica replica-atom
                           :peer-replica-view peer-view-atom
                           :log log
                           :messenger-buffer buffer
                           :messenger messenger
                           :monitoring monitoring
                           :outbox-ch outbox-ch
                           :completion-ch completion-ch
                           :opts opts
                           :kill-ch kill-ch
                           :restart-ch restart-ch
                           :task-lifecycle-fn task-lifecycle}
                          (:onyx.peer/state opts))]
        (let [replica @replica-atom
              peer-view @peer-view-atom
              entry (first (alts!! [kill-ch inbox-ch] :priority true))]
          (if entry
            (let [new-replica (extensions/apply-log-entry entry replica)
                  diff (extensions/replica-diff entry replica new-replica)
                  reactions (extensions/reactions entry replica new-replica diff state)
                  new-peer-view (extensions/peer-replica-view log entry replica new-replica peer-view diff id opts)
                  new-state (extensions/fire-side-effects! entry replica new-replica diff state)]
              (reset! replica-atom new-replica)
              (reset! peer-view-atom new-peer-view)
              (recur (send-to-outbox new-state reactions)))
            (when (:lifecycle state)
              (component/stop @(:lifecycle state)))))))
    (catch Throwable e
      (taoensso.timbre/error e (str e) "Error in processing loop"))
    (finally
     (taoensso.timbre/info "Fell out of processing loop"))))

(defn outbox-loop [id log outbox-ch]
  (try
    (loop []
      (when-let [entry (<!! outbox-ch)]
        (extensions/write-log-entry log entry)
        (recur)))
    (catch Throwable e
      (taoensso.timbre/info e))
    (finally
     (taoensso.timbre/info "Fell out of outbox loop"))))

(defn track-backpressure [id messenger-buffer outbox-ch low-water-pct high-water-pct check-interval]
  (let [on? (atom false)
        buf (.buf (:inbound-ch messenger-buffer))
        low-water-ratio (/ low-water-pct 100)
        high-water-ratio (/ high-water-pct 100)]
    (while (not (Thread/interrupted))
      (let [ratio (/ (count buf) (.n buf))
            on-val @on?]
        (cond (and (not on-val) (> ratio high-water-ratio))
              (do (reset! on-val true)
                  (>!! outbox-ch (create-log-entry :backpressure-on {:peer id})))
              (and on-val (< ratio low-water-ratio))
              (do (reset! on? false)
                  (>!! outbox-ch (create-log-entry :backpressure-off {:peer id})))))
      (Thread/sleep check-interval))))

(defrecord VirtualPeer [opts]
  component/Lifecycle

  (start [{:keys [log acking-daemon messenger-buffer messenger monitoring] :as component}]
    (let [id (java.util.UUID/randomUUID)]
      (taoensso.timbre/info (format "Starting Virtual Peer %s" id))
      (try
        ;; Race to write the job scheduler and messaging to durable storage so that
        ;; non-peers subscribers can discover which messaging to use.
        ;; Only one peer will succeed, and only one needs to.
        (extensions/write-chunk log :job-scheduler {:job-scheduler (:onyx.peer/job-scheduler opts)} nil)
        (extensions/write-chunk log :messaging {:messaging (select-keys opts [:onyx.messaging/impl])} nil)

        (let [inbox-ch (chan (arg-or-default :onyx.peer/inbox-capacity opts))
              outbox-ch (chan (arg-or-default :onyx.peer/outbox-capacity opts))
              kill-ch (chan (dropping-buffer 1))
              restart-ch (chan 1)
              completion-ch (:completion-ch acking-daemon)
              peer-site (extensions/peer-site messenger)
              entry (create-log-entry :prepare-join-cluster {:joiner id :peer-site peer-site})
              origin (extensions/subscribe-to-log log inbox-ch)]
          (extensions/register-pulse log id)
          (>!! outbox-ch entry)

          (let [outbox-loop-ch (thread (outbox-loop id log outbox-ch))
                processing-loop-ch (thread (processing-loop id log messenger-buffer messenger origin inbox-ch outbox-ch restart-ch kill-ch completion-ch opts monitoring))
                track-backpressure-fut (future (track-backpressure id messenger-buffer outbox-ch
                                                                   (arg-or-default :onyx.peer/backpressure-low-water-pct opts)
                                                                   (arg-or-default :onyx.peer/backpressure-high-water-pct opts)
                                                                   (arg-or-default :onyx.peer/backpressure-check-interval opts)))]
            (assoc component
                   :outbox-loop-ch outbox-loop-ch
                   :processing-loop-ch processing-loop-ch
                   :track-backpressure-fut track-backpressure-fut
                   :id id :inbox-ch inbox-ch
                   :outbox-ch outbox-ch :kill-ch kill-ch
                   :restart-ch restart-ch)))
        (catch Throwable e
          (taoensso.timbre/fatal e (format "Error starting Virtual Peer %s" id))
          (throw e)))))

  (stop [component]
    (taoensso.timbre/info (format "Stopping Virtual Peer %s" (:id component)))

    (future-cancel (:track-backpressure-fut component))
    (close! (:inbox-ch component))
    (close! (:outbox-ch component))
    (close! (:kill-ch component))
    (close! (:restart-ch component))
    (<!! (:outbox-loop-ch component))
    (<!! (:processing-loop-ch component))

    (assoc component :track-backpressure-fut nil :inbox-ch nil
           :outbox-loop-ch nil :kill-ch nil :restart-ch nil
           :outbox-loop-ch nil :processing-loop-ch nil)))

(defn virtual-peer [opts]
  (map->VirtualPeer {:opts opts}))
