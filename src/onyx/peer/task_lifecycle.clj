(ns ^:no-doc onyx.peer.task-lifecycle
    (:require [clojure.core.async :refer [alts!! <!! >!! chan close! thread go sliding-buffer]]
              [com.stuartsierra.component :as component]
              [dire.core :as dire]
              [taoensso.timbre :refer [info warn] :as timbre]
              [onyx.log.commands.common :as common]
              [onyx.log.entry :as entry]
              [onyx.queue.hornetq :refer [hornetq]]
              [onyx.planning :refer [find-task build-pred-fn]]
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
  (let [cycle-params {:onyx.core/lifecycle-id (java.util.UUID/randomUUID)}
        rets (l-ext/inject-temporal-resources* event)]
    (if-not (:onyx.core/session rets)
      (let [session (extensions/create-tx-session (:onyx.core/queue event))]
        (merge event rets cycle-params {:onyx.core/session session}))
      (merge event cycle-params rets))))

(defn munge-read-batch [event]
  (merge event {:onyx.core/commit? true} (p-ext/read-batch event)))

(defn munge-decompress-batch [event]
  (merge event (p-ext/decompress-batch event)))

(defn munge-strip-sentinel [event]
  (merge event (p-ext/strip-sentinel event)))

(defn munge-requeue-sentinel [{:keys [onyx.core/requeue?] :as event}]
  (if requeue?
    (merge event (p-ext/requeue-sentinel event))
    event))

(defn join-output-paths [all to-add downstream]
  (cond (= to-add :all) (into #{} downstream)
        (= to-add :none) #{}
        :else (clojure.set/union all to-add)))

(defn choose-output-paths [flow-conditions event new downstream]
  (if (seq flow-conditions)
    (reduce
     (fn [all entry]
       (if ((:flow/predicate entry) [event new])
         (if (:flow/short-circuit? entry)
           (reduced (join-output-paths all (:flow/to entry) downstream))
           (join-output-paths all (:flow/to entry) downstream))
         all))
     #{}
     flow-conditions)
    downstream))

(defn output-paths
  [{:keys [onyx.core/serialized-task onyx.core/compiled-flow-conditions] :as event}]
  (merge
   event
   {:onyx.core/result-paths
    (map
     (fn [segment]
       (choose-output-paths compiled-flow-conditions event segment
                            (keys (:egress-queues serialized-task))))
     (:onyx.core/results event))}))

(defn munge-apply-fn [{:keys [onyx.core/decompressed] :as event}]
  (if (seq decompressed)
    (merge event (output-paths (merge event (p-ext/apply-fn event))))
    (merge event {:onyx.core/results []})))

(defn munge-compress-batch [event]
  (merge event (p-ext/compress-batch event)))

(defn munge-write-batch [event]
  (merge event (p-ext/write-batch event)))

(defn munge-commit-tx
  [{:keys [onyx.core/queue onyx.core/session onyx.core/commit?] :as event}]
  (extensions/commit-tx queue session)
  (merge event {:onyx.core/committed? commit?}))

(defn munge-close-temporal-resources [event]
  (merge event (l-ext/close-temporal-resources* event)))

(defn munge-close-resources
  [{:keys [onyx.core/queue onyx.core/session onyx.core/producers
           onyx.core/consumers onyx.core/reserve?] :as event}]
  (doseq [producer producers] (extensions/close-resource queue producer))
  (when-not reserve?
    (extensions/close-resource queue session))
  (assoc event :onyx.core/closed? true))

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

(defn decompress-batch-loop [decompress-ch strip-ch dead-ch release-f exception-f]
  (with-clean-up
    #(loop []
       (when-let [event (<!! decompress-ch)]
         (>!! strip-ch (munge-decompress-batch event))
         (recur)))
    decompress-ch dead-ch release-f exception-f))

(defn strip-sentinel-loop [strip-ch requeue-ch dead-ch release-f exception-f]
  (with-clean-up
    #(loop []
       (when-let [event (<!! strip-ch)]
         (>!! requeue-ch (munge-strip-sentinel event))
         (recur)))
    strip-ch dead-ch release-f exception-f))

(defn requeue-sentinel-loop [requeue-ch apply-fn-ch dead-ch release-f exception-f]
  (with-clean-up
    #(loop []
       (when-let [event (<!! requeue-ch)]
         (>!! apply-fn-ch (munge-requeue-sentinel event))
         (recur)))
    requeue-ch dead-ch release-f exception-f))

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

(defn write-batch-loop [write-ch commit-ch dead-ch release-f exception-f]
  (with-clean-up
    #(loop []
       (when-let [event (<!! write-ch)]
         (>!! commit-ch (munge-write-batch event))
         (recur)))
    write-ch dead-ch release-f exception-f))

(defn commit-tx-loop [commit-ch close-resources-ch dead-ch release-f exception-f]
  (with-clean-up
    #(loop []
       (when-let [event (<!! commit-ch)]
         (>!! close-resources-ch (munge-commit-tx event))
         (recur)))
    commit-ch dead-ch release-f exception-f))

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

(defn only-relevant-branches [flow task]
  (filter #(= (:flow/from %) task) flow))

(defn compile-flow-conditions [flow-conditions task-name]
  (let [conditions (only-relevant-branches flow-conditions task-name)]
    (map
     (fn [condition]
       (update-in condition [:flow/predicate] build-pred-fn))
     conditions)))

(defrecord TaskLifeCycle [id log queue job-id task-id restart-ch outbox-ch seal-resp-ch opts]
  component/Lifecycle

  (start [component]
    (try
      (let [open-session-kill-ch (chan 0)

            read-batch-ch (chan 0)
            decompress-batch-ch (chan 0)
            strip-sentinel-ch (chan 0)
            requeue-sentinel-ch (chan 0)
            apply-fn-ch (chan 0)
            compress-batch-ch (chan 0)
            write-batch-ch (chan 0)
            commit-tx-ch (chan 0)
            close-resources-ch (chan 0)
            close-temporal-ch (chan 0)
            seal-ch (chan 0)

            open-session-dead-ch (chan (sliding-buffer 1))
            read-batch-dead-ch (chan (sliding-buffer 1))
            decompress-batch-dead-ch (chan (sliding-buffer 1))
            strip-sentinel-dead-ch (chan (sliding-buffer 1))
            requeue-sentinel-dead-ch (chan (sliding-buffer 1))
            apply-fn-dead-ch (chan (sliding-buffer 1))
            compress-batch-dead-ch (chan (sliding-buffer 1))
            write-batch-dead-ch (chan (sliding-buffer 1))
            commit-tx-dead-ch (chan (sliding-buffer 1))
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

                          (close! strip-sentinel-ch)
                          (<!! strip-sentinel-dead-ch)

                          (close! requeue-sentinel-ch)
                          (<!! requeue-sentinel-dead-ch)

                          (close! apply-fn-ch)
                          (<!! apply-fn-dead-ch)

                          (close! compress-batch-ch)
                          (<!! compress-batch-dead-ch)

                          (close! write-batch-ch)
                          (<!! write-batch-dead-ch)

                          (close! commit-tx-ch)
                          (<!! commit-tx-dead-ch)

                          (close! close-resources-ch)
                          (<!! close-resources-dead-ch)
    
                          (close! close-temporal-ch)
                          (<!! close-temporal-dead-ch)

                          (close! seal-ch)
                          (<!! seal-dead-ch)

                          (close! open-session-dead-ch)
                          (close! read-batch-dead-ch)
                          (close! decompress-batch-dead-ch)
                          (close! strip-sentinel-dead-ch)
                          (close! requeue-sentinel-dead-ch)
                          (close! apply-fn-dead-ch)
                          (close! compress-batch-dead-ch)
                          (close! write-batch-dead-ch)
                          (close! commit-tx-dead-ch)
                          (close! close-resources-dead-ch)
                          (close! close-temporal-dead-ch)
                          (close! seal-dead-ch))

            catalog (extensions/read-chunk log :catalog job-id)
            task (extensions/read-chunk log :task task-id)
            flow-conditions (extensions/read-chunk log :flow-conditions job-id)
            catalog-entry (find-task catalog (:name task))

            _ (taoensso.timbre/info (format "[%s] Starting Task LifeCycle for %s" id (:name task)))

            pipeline-data {:onyx.core/id id
                           :onyx.core/job-id job-id
                           :onyx.core/task-id task-id
                           :onyx.core/task (:name task)
                           :onyx.core/catalog catalog
                           :onyx.core/workflow (extensions/read-chunk log :workflow job-id)
                           :onyx.core/flow-conditions flow-conditions
                           :onyx.core/compiled-flow-conditions (compile-flow-conditions flow-conditions (:name task))
                           :onyx.core/task-map catalog-entry
                           :onyx.core/serialized-task task
                           :onyx.core/ingress-queues (:ingress-queues task)
                           :onyx.core/egress-queues (:egress-queues task)
                           :onyx.core/params (resolve-calling-params catalog-entry  opts)
                           :onyx.core/drained-back-off (or (:onyx.peer/drained-back-off opts) 400)
                           :onyx.core/queue queue
                           :onyx.core/log log
                           :onyx.core/outbox-ch outbox-ch
                           :onyx.core/seal-response-ch seal-resp-ch
                           :onyx.core/peer-opts opts
                           :onyx.core/pipeline-state (atom {})}

            ex-f (fn [e] (handle-exception e restart-ch outbox-ch job-id))
            pipeline-data (assoc pipeline-data :onyx.core/queue (extensions/optimize-concurrently queue pipeline-data))
            pipeline-data (merge pipeline-data (l-ext/inject-lifecycle-resources* pipeline-data))]

        (while (not (:onyx.core/start-lifecycle? (munge-start-lifecycle pipeline-data)))
          (Thread/sleep (or (:onyx.peer/sequential-back-off opts) 2000)))

        (assoc component
          :open-session-kill-ch open-session-kill-ch
          :read-batch-ch read-batch-ch
          :decompress-batch-ch decompress-batch-ch
          :strip-sentinel-ch strip-sentinel-ch
          :requeue-sentinel-ch requeue-sentinel-ch
          :apply-fn-ch apply-fn-ch
          :compress-batch-ch compress-batch-ch
          :write-batch-ch write-batch-ch
          :commit-tx-ch commit-tx-ch
          :close-resources-ch close-resources-ch
          :close-temporal-ch close-temporal-ch
          :seal-ch seal-ch

          :open-session-dead-ch open-session-dead-ch
          :read-batch-dead-ch read-batch-dead-ch
          :decompress-batch-dead-ch decompress-batch-dead-ch
          :strip-sentinel-dead-ch strip-sentinel-dead-ch
          :requeue-sentinel-dead-ch requeue-sentinel-dead-ch
          :apply-fn-dead-ch apply-fn-dead-ch
          :compress-batch-dead-ch compress-batch-dead-ch
          :write-batch-dead-ch write-batch-dead-ch
          :commit-tx-dead-ch commit-tx-dead-ch
          :close-resources-dead-ch close-resources-dead-ch
          :close-temporal-dead-ch close-temporal-dead-ch
          :seal-dead-ch seal-dead-ch

          :inject-temporal-loop (thread (inject-temporal-loop read-batch-ch open-session-kill-ch pipeline-data open-session-dead-ch release-fn! ex-f))
          :read-batch-loop (thread (read-batch-loop read-batch-ch decompress-batch-ch read-batch-dead-ch release-fn! ex-f))
          :decompress-batch-loop (thread (decompress-batch-loop decompress-batch-ch strip-sentinel-ch decompress-batch-dead-ch release-fn! ex-f))
          :strip-sentinel-loop (thread (strip-sentinel-loop strip-sentinel-ch requeue-sentinel-ch strip-sentinel-dead-ch release-fn! ex-f))
          :requeue-sentinel-loop (thread (requeue-sentinel-loop requeue-sentinel-ch apply-fn-ch requeue-sentinel-dead-ch release-fn! ex-f))
          :apply-fn-loop (thread (apply-fn-loop apply-fn-ch compress-batch-ch apply-fn-dead-ch release-fn! ex-f))
          :compress-batch-loop (thread (compress-batch-loop compress-batch-ch write-batch-ch compress-batch-dead-ch release-fn! ex-f))
          :write-batch-loop (thread (write-batch-loop write-batch-ch commit-tx-ch write-batch-dead-ch release-fn! ex-f))
          :commit-tx-loop (thread (commit-tx-loop commit-tx-ch close-resources-ch commit-tx-dead-ch release-fn! ex-f))
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

(defn task-lifecycle [args {:keys [id log queue job task restart-ch outbox-ch seal-ch opts]}]
  (map->TaskLifeCycle {:id id :log log :queue queue :job-id job
                       :task-id task :restart-ch restart-ch :outbox-ch outbox-ch
                       :seal-resp-ch seal-ch :opts opts}))

(dire/with-post-hook! #'munge-start-lifecycle
  (fn [{:keys [onyx.core/id onyx.core/lifecycle-id onyx.core/start-lifecycle?] :as event}]
    (when-not start-lifecycle?
      (timbre/info (format "[%s / %s] Sequential task currently has queue consumers. Backing off and retrying..." id lifecycle-id)))))

(dire/with-post-hook! #'munge-inject-temporal
  (fn [{:keys [onyx.core/id onyx.core/lifecycle-id]}]
    (taoensso.timbre/trace (format "[%s / %s] Created new tx session" id lifecycle-id))))

(dire/with-post-hook! #'munge-read-batch
  (fn [{:keys [onyx.core/id onyx.core/batch onyx.core/lifecycle-id]}]
    (taoensso.timbre/info (format "[%s / %s] Read %s segments" id lifecycle-id (count batch)))))

(dire/with-post-hook! #'munge-strip-sentinel
  (fn [{:keys [onyx.core/id onyx.core/decompressed onyx.core/lifecycle-id]}]
    (taoensso.timbre/trace (format "[%s / %s] Attempted to strip sentinel. %s segments left" id lifecycle-id (count decompressed)))))

(dire/with-post-hook! #'munge-requeue-sentinel
  (fn [{:keys [onyx.core/id onyx.core/tail-batch? onyx.core/lifecycle-id]}]
    (taoensso.timbre/trace (format "[%s / %s] Requeued sentinel value" id lifecycle-id))))

(dire/with-post-hook! #'munge-decompress-batch
  (fn [{:keys [onyx.core/id onyx.core/decompressed onyx.core/batch onyx.core/lifecycle-id]}]
    (taoensso.timbre/trace (format "[%s / %s] Decompressed %s segments" id lifecycle-id (count decompressed)))))

(dire/with-post-hook! #'munge-apply-fn
  (fn [{:keys [onyx.core/id onyx.core/results onyx.core/lifecycle-id]}]
    (taoensso.timbre/trace (format "[%s / %s] Applied fn to %s segments" id lifecycle-id (count results)))))

(dire/with-post-hook! #'munge-compress-batch
  (fn [{:keys [onyx.core/id onyx.core/compressed onyx.core/lifecycle-id]}]
    (taoensso.timbre/trace (format "[%s / %s] Compressed %s segments" id lifecycle-id (count compressed)))))

(dire/with-post-hook! #'munge-write-batch
  (fn [{:keys [onyx.core/id onyx.core/lifecycle-id onyx.core/compressed]}]
    (taoensso.timbre/info (format "[%s / %s] Wrote %s segments" id lifecycle-id (count compressed)))))

(dire/with-post-hook! #'munge-commit-tx
  (fn [{:keys [onyx.core/id onyx.core/commit? onyx.core/lifecycle-id]}]
    (taoensso.timbre/trace (format "[%s / %s] Committed transaction? %s" id lifecycle-id commit?))))

(dire/with-post-hook! #'munge-close-resources
  (fn [{:keys [onyx.core/id onyx.core/lifecycle-id]}]
    (taoensso.timbre/trace (format "[%s / %s] Closed resources" id lifecycle-id))))

(dire/with-post-hook! #'munge-close-temporal-resources
  (fn [{:keys [onyx.core/id onyx.core/lifecycle-id]}]
    (taoensso.timbre/trace (format "[%s / %s] Closed temporal plugin resources" id lifecycle-id))))

(dire/with-post-hook! #'munge-seal-resource
  (fn [{:keys [onyx.core/id onyx.core/task onyx.core/sealed? onyx.core/lifecycle-id]}]
    (taoensso.timbre/trace (format "[%s / %s] Sealing resource for %s? %s" id lifecycle-id task sealed?))))

