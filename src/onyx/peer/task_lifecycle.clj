(ns ^:no-doc onyx.peer.task-lifecycle
    (:require [clojure.core.async :refer [alts!! <!! >!! chan close! thread go sliding-buffer]]
              [com.stuartsierra.component :as component]
              [dire.core :as dire]
              [taoensso.timbre :refer [info warn] :as timbre]
              [onyx.log.commands.common :as common]
              [onyx.log.entry :as entry]
              [onyx.planning :refer [find-task]]
              [onyx.messaging.acking-daemon :refer [gen-ack-value gen-message-id]]
              [onyx.peer.task-lifecycle-extensions :as l-ext]
              [onyx.peer.pipeline-extensions :as p-ext]
              [onyx.peer.function :as function]
              [onyx.peer.aggregate :as aggregate]
              [onyx.peer.operation :as operation]
              [onyx.extensions :as extensions]
              [onyx.plugin.hornetq]))

(def restartable-exceptions
  [org.hornetq.api.core.HornetQNotConnectedException
   org.hornetq.api.core.HornetQInternalErrorException])

(defn resolve-calling-params [catalog-entry opts]
  (concat (get (:onyx.peer/fn-params opts) (:onyx/name catalog-entry))
          (map (fn [param] (get catalog-entry param)) (:onyx/params catalog-entry))))

(defn munge-start-lifecycle [event]
  (l-ext/start-lifecycle?* event))

(defn munge-inject-temporal [event]
  (let [cycle-params {:onyx.core/lifecycle-id (java.util.UUID/randomUUID)}]
    (merge event cycle-params (l-ext/inject-temporal-resources* event))))

(defn add-message-id [m]
  (assoc m :id (gen-message-id)))

(defn add-acker-id [event m]
  (let [peers (:peers @(:onyx.core/replica event))
        n (mod (.hashCode (:message m)) (count peers))]
    (assoc m :acker-id (nth peers n))))

(defn add-completion-id [event m]
  (assoc m :completion-id (:onyx.core/id event)))

(defn tag-each-message [event]
  (if (= (:onyx/type (:onyx.core/task-map event)) :input)
    (-> event
        (update-in [:onyx.core/batch] (partial map add-message-id))
        (update-in [:onyx.core/batch] (partial map (partial add-acker-id event)))
        (update-in [:onyx.core/batch] (partial map (partial add-completion-id event))))
    event))

(defn munge-read-batch [event]
  (merge event (tag-each-message (p-ext/read-batch event))))

(defn munge-decompress-batch [event]
  (merge event (p-ext/decompress-batch event)))

(defn ack-message [event]
  (doseq []))

(defn munge-apply-fn [{:keys [onyx.core/decompressed] :as event}]
  (if (seq decompressed)
    (let [result (merge event (p-ext/apply-fn event))]
      result)
    (merge event {:onyx.core/results []})))

(defn munge-compress-batch [event]
  (merge event (p-ext/compress-batch event)))

(defn munge-write-batch [event]
  (merge event (p-ext/write-batch event)))

(defn munge-close-temporal-resources [event]
  (merge event (l-ext/close-temporal-resources* event)))

(defn munge-close-resources
  [event]
  event)

(defn munge-seal-resource
  [{:keys [onyx.core/pipeline-state onyx.core/outbox-ch
           onyx.core/seal-response-ch] :as event}]
  (when (:onyx.core/tail-batch? event)
    (let [state @pipeline-state]
      (if (:tried-to-seal? state)
        (merge event {:onyx.core/sealed? false})
        (let [args {:id (:onyx.core/id event)
                    :job (:onyx.core/job-id event)
                    :task (:onyx.core/task-id event)}
              entry (entry/create-log-entry :seal-task args)
              _ (>!! outbox-ch entry)
              seal? (<!! seal-response-ch)]
          (swap! pipeline-state assoc :tried-to-seal? true)
          (when seal?
            (p-ext/seal-resource event)
            (let [args {:id (:onyx.core/id event)
                        :job (:onyx.core/job-id event)
                        :task (:onyx.core/task-id event)}
                  entry (entry/create-log-entry :complete-task args)]
              (>!! outbox-ch entry)))))))
  event)

(defn with-clean-up [f ch dead-ch release-f exception-f]
  (try
    (f)
    (catch Exception e
      (exception-f e)
      (close! ch)
      ;; Unblock any blocked puts
      (<!! ch))
    (finally
     (>!! dead-ch true)
     (release-f))))

(defn inject-temporal-loop [read-ch kill-ch pipeline-data dead-ch release-f exception-f]
  (with-clean-up
    #(loop []
       (when (first (alts!! [kill-ch] :default true))
         (let [state @(:onyx.core/pipeline-state pipeline-data)]
           (when (operation/drained-all-inputs? pipeline-data state)
             (Thread/sleep (:onyx.core/drained-back-off pipeline-data)))
           (>!! read-ch (munge-inject-temporal pipeline-data)))
         (recur)))
    kill-ch dead-ch release-f exception-f))

(defn read-batch-loop [read-ch decompress-ch dead-ch release-f exception-f]
  (with-clean-up
    #(loop []
       (when-let [event (<!! read-ch)]
         (>!! decompress-ch (munge-read-batch event))
         (recur)))
    read-ch dead-ch release-f exception-f))

(defn decompress-batch-loop [decompress-ch apply-ch dead-ch release-f exception-f]
  (with-clean-up
    #(loop []
       (when-let [event (<!! decompress-ch)]
         (>!! apply-ch (munge-decompress-batch event))
         (recur)))
    decompress-ch dead-ch release-f exception-f))

(defn apply-fn-loop [apply-fn-ch compress-ch dead-ch release-f exception-f]
  (with-clean-up
    #(loop []
       (when-let [event (<!! apply-fn-ch)]
         (>!! compress-ch (munge-apply-fn event))
         (recur)))
    apply-fn-ch dead-ch release-f exception-f))

(defn compress-batch-loop [compress-ch write-batch-ch dead-ch release-f exception-f]
  (with-clean-up
    #(loop []
       (when-let [event (<!! compress-ch)]
         (>!! write-batch-ch (munge-compress-batch event))
         (recur)))
    compress-ch dead-ch release-f exception-f))

(defn write-batch-loop [write-ch close-ch dead-ch release-f exception-f]
  (with-clean-up
    #(loop []
       (when-let [event (<!! write-ch)]
         (>!! close-ch (munge-write-batch event))
         (recur)))
    write-ch dead-ch release-f exception-f))

(defn close-resources-loop [close-ch close-temporal-ch dead-ch release-f exception-f]
  (with-clean-up
    #(loop []
       (when-let [event (<!! close-ch)]
         (>!! close-temporal-ch (munge-close-resources event))
         (recur)))
    close-ch dead-ch release-f exception-f))

(defn close-temporal-loop [close-temporal-ch seal-ch dead-ch release-f exception-f]
  (with-clean-up
    #(loop []
       (when-let [event (<!! close-temporal-ch)]
         (>!! seal-ch (munge-close-temporal-resources event))
         (recur)))
    close-temporal-ch dead-ch release-f exception-f))

(defn seal-resource-loop [seal-ch dead-ch release-f exception-f]
  (with-clean-up
    #(loop []
       (when-let [event (<!! seal-ch)]
         (munge-seal-resource event)
         (recur)))
    seal-ch dead-ch release-f exception-f))

(defn handle-exception [e restart-ch outbox-ch job-id]
  (warn e)
  (if (some #{(type e)} restartable-exceptions)
    (>!! restart-ch true)
    (let [entry (entry/create-log-entry :kill-job {:job job-id})]
      (>!! outbox-ch entry))))

(defrecord TaskLifeCycle [id log messenger-buffer messenger job-id task-id replica restart-ch outbox-ch seal-resp-ch opts]
  component/Lifecycle

  (start [component]
    (try
      (let [open-session-kill-ch (chan 0)

            read-batch-ch (chan 0)
            decompress-batch-ch (chan 0)
            apply-fn-ch (chan 0)
            compress-batch-ch (chan 0)
            write-batch-ch (chan 0)
            close-resources-ch (chan 0)
            close-temporal-ch (chan 0)
            seal-ch (chan 0)

            open-session-dead-ch (chan (sliding-buffer 1))
            read-batch-dead-ch (chan (sliding-buffer 1))
            decompress-batch-dead-ch (chan (sliding-buffer 1))
            apply-fn-dead-ch (chan (sliding-buffer 1))
            compress-batch-dead-ch (chan (sliding-buffer 1))
            write-batch-dead-ch (chan (sliding-buffer 1))
            close-resources-dead-ch (chan (sliding-buffer 1))
            close-temporal-dead-ch (chan (sliding-buffer 1))
            seal-dead-ch (chan (sliding-buffer 1))

            release-fn! (fn []
                          (close! open-session-kill-ch)
                          (<!! open-session-dead-ch)

                          (close! read-batch-ch)
                          (<!! read-batch-dead-ch)

                          (close! decompress-batch-ch)
                          (<!! decompress-batch-dead-ch)

                          (close! apply-fn-ch)
                          (<!! apply-fn-dead-ch)

                          (close! compress-batch-ch)
                          (<!! compress-batch-dead-ch)

                          (close! write-batch-ch)
                          (<!! write-batch-dead-ch)

                          (close! close-resources-ch)
                          (<!! close-resources-dead-ch)
    
                          (close! close-temporal-ch)
                          (<!! close-temporal-dead-ch)

                          (close! seal-ch)
                          (<!! seal-dead-ch)

                          (close! open-session-dead-ch)
                          (close! read-batch-dead-ch)
                          (close! decompress-batch-dead-ch)
                          (close! apply-fn-dead-ch)
                          (close! compress-batch-dead-ch)
                          (close! write-batch-dead-ch)
                          (close! close-resources-dead-ch)
                          (close! close-temporal-dead-ch)
                          (close! seal-dead-ch))

            catalog (extensions/read-chunk log :catalog job-id)
            task (extensions/read-chunk log :task task-id)
            catalog-entry (find-task catalog (:name task))

            _ (taoensso.timbre/info (format "[%s] Starting Task LifeCycle for %s" id (:name task)))

            pipeline-data {:onyx.core/id id
                           :onyx.core/job-id job-id
                           :onyx.core/task-id task-id
                           :onyx.core/task (:name task)
                           :onyx.core/catalog catalog
                           :onyx.core/workflow (extensions/read-chunk log :workflow job-id)
                           :onyx.core/task-map catalog-entry
                           :onyx.core/serialized-task task
                           :onyx.core/params (resolve-calling-params catalog-entry  opts)
                           :onyx.core/drained-back-off (or (:onyx.peer/drained-back-off opts) 400)
                           :onyx.core/log log
                           :onyx.core/messenger-buffer messenger-buffer
                           :onyx.core/messenger messenger
                           :onyx.core/outbox-ch outbox-ch
                           :onyx.core/seal-response-ch seal-resp-ch
                           :onyx.core/peer-opts opts
                           :onyx.core/pipeline-state (atom {})
                           :onyx.core/replica replica}

            ex-f (fn [e] (handle-exception e restart-ch outbox-ch job-id))
            pipeline-data (merge pipeline-data (l-ext/inject-lifecycle-resources* pipeline-data))]

        (while (not (:onyx.core/start-lifecycle? (munge-start-lifecycle pipeline-data)))
          (Thread/sleep (or (:onyx.peer/sequential-back-off opts) 2000)))

        (>!! outbox-ch (entry/create-log-entry :signal-ready {:id id}))
        (while (not (common/job-covered? @replica job-id))
          (prn "Job not covered yet. Backing off and trying again")
          (Thread/sleep 2000))

        (assoc component
          :open-session-kill-ch open-session-kill-ch
          :read-batch-ch read-batch-ch
          :decompress-batch-ch decompress-batch-ch
          :apply-fn-ch apply-fn-ch
          :compress-batch-ch compress-batch-ch
          :write-batch-ch write-batch-ch
          :close-resources-ch close-resources-ch
          :close-temporal-ch close-temporal-ch
          :seal-ch seal-ch

          :open-session-dead-ch open-session-dead-ch
          :read-batch-dead-ch read-batch-dead-ch
          :decompress-batch-dead-ch decompress-batch-dead-ch
          :apply-fn-dead-ch apply-fn-dead-ch
          :compress-batch-dead-ch compress-batch-dead-ch
          :write-batch-dead-ch write-batch-dead-ch
          :close-resources-dead-ch close-resources-dead-ch
          :close-temporal-dead-ch close-temporal-dead-ch
          :seal-dead-ch seal-dead-ch

          :inject-temporal-loop (thread (inject-temporal-loop read-batch-ch open-session-kill-ch pipeline-data open-session-dead-ch release-fn! ex-f))
          :read-batch-loop (thread (read-batch-loop read-batch-ch decompress-batch-ch read-batch-dead-ch release-fn! ex-f))
          :decompress-batch-loop (thread (decompress-batch-loop decompress-batch-ch apply-fn-ch decompress-batch-dead-ch release-fn! ex-f))
          :apply-fn-loop (thread (apply-fn-loop apply-fn-ch compress-batch-ch apply-fn-dead-ch release-fn! ex-f))
          :compress-batch-loop (thread (compress-batch-loop compress-batch-ch write-batch-ch compress-batch-dead-ch release-fn! ex-f))
          :write-batch-loop (thread (write-batch-loop write-batch-ch close-resources-ch write-batch-dead-ch release-fn! ex-f))
          :close-resources-loop (thread (close-resources-loop close-resources-ch close-temporal-ch close-resources-dead-ch release-fn! ex-f))
          :close-temporal-loop (thread (close-temporal-loop close-temporal-ch seal-ch close-temporal-dead-ch release-fn! ex-f))
          :seal-resource-loop (thread (seal-resource-loop seal-ch seal-dead-ch release-fn! ex-f))

          :release-fn! release-fn!
          :pipeline-data pipeline-data))
      (catch Exception e
        (handle-exception e restart-ch outbox-ch job-id)
        component)))

  (stop [component]
    (taoensso.timbre/info (format "[%s] Stopping Task LifeCycle for %s" id (:onyx.core/task (:pipeline-data component))))

    (when-let [f (:release-fn! component)]
      (f))
    
    (l-ext/close-lifecycle-resources* (:pipeline-data component))

    component))

(defn task-lifecycle [args {:keys [id log messenger-buffer messenger job task replica
                                   restart-ch outbox-ch seal-ch opts]}]
  (map->TaskLifeCycle {:id id :log log :messenger-buffer messenger-buffer
                       :messenger messenger :job-id job
                       :task-id task :restart-ch restart-ch :outbox-ch outbox-ch
                       :replica replica :seal-resp-ch seal-ch :opts opts}))

(dire/with-post-hook! #'munge-start-lifecycle
  (fn [{:keys [onyx.core/id onyx.core/lifecycle-id onyx.core/start-lifecycle?] :as event}]
    (when-not start-lifecycle?
      (timbre/info (format "[%s / %s] Sequential task currently has queue consumers. Backing off and retrying..." id lifecycle-id)))))

(dire/with-post-hook! #'munge-inject-temporal
  (fn [{:keys [onyx.core/id onyx.core/lifecycle-id]}]
    (taoensso.timbre/info (format "[%s / %s] Created new tx session" id lifecycle-id))))

(dire/with-post-hook! #'munge-read-batch
  (fn [{:keys [onyx.core/id onyx.core/batch onyx.core/lifecycle-id]}]
    (taoensso.timbre/info (format "[%s / %s] Read %s segments" id lifecycle-id (count batch)))))

(dire/with-post-hook! #'munge-decompress-batch
  (fn [{:keys [onyx.core/id onyx.core/decompressed onyx.core/batch onyx.core/lifecycle-id]}]
    (taoensso.timbre/info (format "[%s / %s] Decompressed %s segments" id lifecycle-id (count decompressed)))))

(dire/with-post-hook! #'munge-apply-fn
  (fn [{:keys [onyx.core/id onyx.core/results onyx.core/lifecycle-id]}]
    (taoensso.timbre/info (format "[%s / %s] Applied fn to %s segments" id lifecycle-id (count results)))))

(dire/with-post-hook! #'munge-compress-batch
  (fn [{:keys [onyx.core/id onyx.core/compressed onyx.core/lifecycle-id]}]
    (taoensso.timbre/info (format "[%s / %s] Compressed %s segments" id lifecycle-id (count compressed)))))

(dire/with-post-hook! #'munge-write-batch
  (fn [{:keys [onyx.core/id onyx.core/lifecycle-id onyx.core/compressed]}]
    (taoensso.timbre/info (format "[%s / %s] Wrote %s segments" id lifecycle-id (count compressed)))))

(dire/with-post-hook! #'munge-close-resources
  (fn [{:keys [onyx.core/id onyx.core/lifecycle-id]}]
    (taoensso.timbre/info (format "[%s / %s] Closed resources" id lifecycle-id))))

(dire/with-post-hook! #'munge-close-temporal-resources
  (fn [{:keys [onyx.core/id onyx.core/lifecycle-id]}]
    (taoensso.timbre/info (format "[%s / %s] Closed temporal plugin resources" id lifecycle-id))))

(dire/with-post-hook! #'munge-seal-resource
  (fn [{:keys [onyx.core/id onyx.core/task onyx.core/sealed? onyx.core/lifecycle-id]}]
    (taoensso.timbre/info (format "[%s / %s] Sealing resource for %s? %s" id lifecycle-id task sealed?))))

