(ns ^:no-doc onyx.peer.task-lifecycle
    (:require [clojure.core.async :refer [alts!! <!! >!! <! timeout chan close! thread go dropping-buffer]]
              [com.stuartsierra.component :as component]
              [dire.core :as dire]
              [taoensso.timbre :refer [info warn trace] :as timbre]
              [onyx.log.commands.common :as common]
              [onyx.log.entry :as entry]
              [onyx.planning :refer [find-task]]
              [onyx.messaging.acking-daemon :as acker]
              [onyx.peer.task-lifecycle-extensions :as l-ext]
              [onyx.peer.pipeline-extensions :as p-ext]
              [onyx.peer.function :as function]
              [onyx.peer.aggregate :as aggregate]
              [onyx.peer.operation :as operation]
              [onyx.extensions :as extensions]
              [onyx.plugin.hornetq]))

(def restartable-exceptions [])

(defn resolve-calling-params [catalog-entry opts]
  (concat (get (:onyx.peer/fn-params opts) (:onyx/name catalog-entry))
          (map (fn [param] (get catalog-entry param)) (:onyx/params catalog-entry))))

(defn munge-start-lifecycle [event]
  (l-ext/start-lifecycle?* event))

(defn add-ack-value [m]
  (assoc m :ack-val (acker/gen-ack-value)))

(defn add-acker-id [event m]
  (let [peers (:peers @(:onyx.core/replica event))
        n (mod (.hashCode (:message m)) (count peers))]
    (assoc m :acker-id (nth peers n))))

(defn add-completion-id [event m]
  (assoc m :completion-id (:onyx.core/id event)))

(defn tag-each-message [event]
  (if (= (:onyx/type (:onyx.core/task-map event)) :input)
    (let [event
          (-> event
              (update-in [:onyx.core/batch] (partial map add-ack-value))
              (update-in [:onyx.core/batch] (partial map (partial add-acker-id event)))
              (update-in [:onyx.core/batch] (partial map (partial add-completion-id event))))]
      (doseq [raw-segment (:onyx.core/batch event)]
        (extensions/internal-ack-message
         (:onyx.core/messenger event)
         event
         (:id raw-segment)
         (:acker-id raw-segment)
         (:completion-id raw-segment)
         (:ack-val raw-segment)))
      event)
    event))

(defn sentinel-found? [event]
  (seq (filter (partial = :done) (map :message (:onyx.core/decompressed event)))))

(defn complete-job [event]
  (prn "Job is done!"))

(defn sentinel-id [event]
  (:id (first (filter #(= :done (:message %)) (:onyx.core/decompressed event)))))

(defn drop-nth [n coll]
  (keep-indexed #(if (not= %1 n) %2) coll))

(defn strip-sentinel [{:keys [onyx.core/batch onyx.core/decompressed] :as event}]
  (if-let [k (.indexOf (map :message decompressed) :done)]
    (merge event {:onyx.core/batch (drop-nth k batch)
                  :onyx.core/decompressed (drop-nth k decompressed)})))

(defn fuse-ack-vals [task parent-ack child-ack]
  (if (= (:onyx/type task) :output)
    parent-ack
    (acker/prefuse-vals parent-ack child-ack)))

(defn ack-messages [{:keys [onyx.core/acking-daemon onyx.core/children] :as event}]
  (if children
    (doseq [raw-segment (keys children)]
      (when (:ack-val raw-segment)
        (extensions/internal-ack-message
         (:onyx.core/messenger event)
         event
         (:id raw-segment)
         (:acker-id raw-segment)
         (:completion-id raw-segment)
         (fuse-ack-vals (:onyx.core/task-map event) (:ack-val raw-segment) (get children raw-segment)))))))

(defn segments->ack-values [event segments]
  (if (= :output (:onyx/type (:onyx.core/task-map event)))
    (repeat (count segments) nil)
    (map (fn [x] (acker/gen-ack-value)) segments)))

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

(defn inject-temporal-resources [event]
  (let [cycle-params {:onyx.core/lifecycle-id (java.util.UUID/randomUUID)}]
    (merge event cycle-params (l-ext/inject-temporal-resources* event))))

(defn read-batch [event]
  (let [rets (tag-each-message (merge event (p-ext/read-batch event)))]
    (when (= (:onyx/type (:onyx.core/task-map event)) :input)
      (doseq [m (:onyx.core/batch rets)]
        (go (try (<! (timeout 5000))
                 (when (p-ext/pending? rets (:id m))
                   (p-ext/replay-message event (:id m)))
                 (catch Exception e
                   (taoensso.timbre/warn e))))))
    rets))

(defn decompress-batch [event]
  (let [rets (merge event (p-ext/decompress-batch event))]
    (if (sentinel-found? rets)
      (do (if (p-ext/drained? rets)
            (complete-job rets)
            (p-ext/replay-message rets (sentinel-id rets)))
          (strip-sentinel rets))
      rets)))

(defn apply-fn [{:keys [onyx.core/batch onyx.core/decompressed] :as event}]
  (if (seq decompressed)
    (let [rets
          (merge event
                 (reduce
                  (fn [rets [raw thawed]]
                    (let [new-segments (p-ext/apply-fn event (:message thawed))
                          new-segments (if coll? new-segments) new-segments (vector new-segments)
                          new-ack-vals (map (fn [x] (acker/gen-ack-value)) new-segments)
                          tagged (apply acker/prefuse-vals new-ack-vals)
                          results (map (fn [segment ack-val]
                                         {:id (:id raw)
                                          :acker-id (:acker-id raw)
                                          :completion-id (:completion-id raw)
                                          :message segment
                                          :ack-val ack-val})
                                       new-segments new-ack-vals)]
                      (-> rets
                          (update-in [:onyx.core/results] concat results)
                          (assoc-in [:onyx.core/children raw] tagged))))
                  {:onyx.core/results [] :onyx.core/children {}}
                  (map vector batch decompressed)))]
      (ack-messages rets)
      rets)
    (merge event {:onyx.core/results []})))

(defn compress-batch [event]
  (merge event (p-ext/compress-batch event)))

(defn write-batch [event]
  (merge event (p-ext/write-batch event)))

(defn close-temporal-resources [event]
  (merge event (l-ext/close-temporal-resources* event)))

(defn seal-output-targets
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
              (>!! outbox-ch entry))))))))

(defn release-messages! [messenger event]
  (loop []
    (when-let [id (<!! (:release-ch messenger))]
      (p-ext/ack-message event id)
      (recur))))

(defn handle-exception [e restart-ch outbox-ch job-id]
  (warn e)
  (if (some #{(type e)} restartable-exceptions)
    (>!! restart-ch true)
    (let [entry (entry/create-log-entry :kill-job {:job job-id})]
      (>!! outbox-ch entry))))

(defn run-task-lifecycle [init-event kill-ch]
  (loop [event init-event]
    (when (first (alts!! [kill-ch] :default true))
      (-> event
          (inject-temporal-resources)
          (read-batch)
          (decompress-batch)
          (apply-fn)
          (compress-batch)
          (write-batch)
          (close-temporal-resources)
          (seal-output-targets))
      (recur init-event))))

(defrecord TaskLifeCycle [id log messenger-buffer messenger job-id task-id replica restart-ch outbox-ch seal-resp-ch opts]
  component/Lifecycle

  (start [component]
    (try
      (let [catalog (extensions/read-chunk log :catalog job-id)
            task (extensions/read-chunk log :task task-id)
            catalog-entry (find-task catalog (:name task))

            kill-ch (chan (dropping-buffer 1))

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

        (thread (release-messages! messenger pipeline-data))
        (thread (run-task-lifecycle pipeline-data kill-ch))

        (assoc component :pipeline-data pipeline-data :kill-ch kill-ch))
      (catch Exception e
        (handle-exception e restart-ch outbox-ch job-id)
        component)))

  (stop [component]
    (taoensso.timbre/info (format "[%s] Stopping Task LifeCycle for %s" id (:onyx.core/task (:pipeline-data component))))
    (l-ext/close-lifecycle-resources* (:pipeline-data component))

    (close! (:kill-ch component))

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

(dire/with-post-hook! #'inject-temporal-resources
  (fn [{:keys [onyx.core/id onyx.core/lifecycle-id]}]
    (taoensso.timbre/trace (format "[%s / %s] Created new tx session" id lifecycle-id))))

(dire/with-post-hook! #'read-batch
  (fn [{:keys [onyx.core/id onyx.core/batch onyx.core/lifecycle-id]}]
    (taoensso.timbre/info (format "[%s / %s] Read %s segments" id lifecycle-id (count batch)))))

(dire/with-post-hook! #'decompress-batch
  (fn [{:keys [onyx.core/id onyx.core/decompressed onyx.core/batch onyx.core/lifecycle-id]}]
    (taoensso.timbre/trace (format "[%s / %s] Decompressed %s segments" id lifecycle-id (count decompressed)))))

(dire/with-post-hook! #'apply-fn
  (fn [{:keys [onyx.core/id onyx.core/results onyx.core/lifecycle-id]}]
    (taoensso.timbre/trace (format "[%s / %s] Applied fn to %s segments" id lifecycle-id (count results)))))

(dire/with-post-hook! #'compress-batch
  (fn [{:keys [onyx.core/id onyx.core/compressed onyx.core/lifecycle-id]}]
    (taoensso.timbre/trace (format "[%s / %s] Compressed %s segments" id lifecycle-id (count compressed)))))

(dire/with-post-hook! #'write-batch
  (fn [{:keys [onyx.core/id onyx.core/lifecycle-id onyx.core/compressed]}]
    (taoensso.timbre/info (format "[%s / %s] Wrote %s segments" id lifecycle-id (count compressed)))))

(dire/with-post-hook! #'close-temporal-resources
  (fn [{:keys [onyx.core/id onyx.core/lifecycle-id]}]
    (taoensso.timbre/trace (format "[%s / %s] Closed temporal plugin resources" id lifecycle-id))))

(dire/with-post-hook! #'seal-output-targets
  (fn [{:keys [onyx.core/id onyx.core/task onyx.core/sealed? onyx.core/lifecycle-id]}]
    (taoensso.timbre/trace (format "[%s / %s] Sealing resource for %s? %s" id lifecycle-id task sealed?))))

