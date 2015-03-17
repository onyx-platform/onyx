(ns ^:no-doc onyx.peer.task-lifecycle
    (:require [clojure.core.async :refer [alts!! <!! >!! <! >! timeout chan close! thread go dropping-buffer]]
              [com.stuartsierra.component :as component]
              [dire.core :as dire]
              [taoensso.timbre :refer [info warn trace] :as timbre]
              [onyx.log.commands.common :as common]
              [onyx.log.entry :as entry]
              [onyx.static.planning :refer [find-task build-pred-fn]]
              [onyx.messaging.acking-daemon :as acker]
              [onyx.peer.task-lifecycle-extensions :as l-ext]
              [onyx.peer.pipeline-extensions :as p-ext]
              [onyx.peer.function :as function]
              [onyx.peer.operation :as operation]
              [onyx.extensions :as extensions]))

(def restartable-exceptions [])

(defn resolve-calling-params [catalog-entry opts]
  (concat (get (:onyx.peer/fn-params opts) (:onyx/name catalog-entry))
          (map (fn [param] (get catalog-entry param)) (:onyx/params catalog-entry))))

(defn munge-start-lifecycle [event]
  (l-ext/start-lifecycle?* event))

(defn add-ack-value [m]
  (assoc m :ack-val (acker/gen-ack-value)))

(defn add-acker-id [event m]
  (let [peers (get-in @(:onyx.core/replica event) [:ackers (:onyx.core/job-id event)])
        n (mod (hash (:message m)) (count peers))]
    (assoc m :acker-id (nth peers n))))

(defn add-completion-id [event m]
  (assoc m :completion-id (:onyx.core/id event)))

(defn sentinel-found? [event]
  (seq (filter (partial = :done) (map :message (:onyx.core/decompressed event)))))

(defn complete-job [{:keys [onyx.core/job-id onyx.core/task-id] :as event}]
  (let [entry (entry/create-log-entry :exhaust-input {:job job-id :task task-id})]
    (>!! (:onyx.core/outbox-ch event) entry)))

(defn sentinel-id [event]
  (:id (first (filter #(= :done (:message %)) (:onyx.core/decompressed event)))))

(defn drop-nth [n coll]
  (keep-indexed #(if (not= %1 n) %2) coll))

(defn fuse-ack-vals [task parent-ack child-ack]
  (if (= (:onyx/type task) :output)
    parent-ack
    (acker/prefuse-vals (vector parent-ack child-ack))))

(defn ack-messages [{:keys [onyx.core/acking-daemon onyx.core/children] :as event}]
  (merge
   event
   (when (and children (not (:onyx/side-effects-only? (:onyx.core/task-map event))))
     (doseq [raw-segment (keys children)]
       (when (:ack-val raw-segment)
         (let [link (operation/peer-link event (:acker-id raw-segment) :acker-peer-site)]
           (extensions/internal-ack-message
            (:onyx.core/messenger event)
            event
            link
            (:id raw-segment)
            (:completion-id raw-segment)
            (fuse-ack-vals (:onyx.core/task-map event) (:ack-val raw-segment) (get children raw-segment)))))))))

(defn join-output-paths [all to-add downstream]
  (cond (= to-add :all) (into #{} downstream)
        (= to-add :none) #{}
        :else (clojure.set/union (into #{} all) (into #{} to-add))))

(defn route-output-paths
  [{:keys [onyx.core/serialized-task onyx.core/compiled-flow-conditions] :as event}]
  (merge
   event
   {:onyx.core/result-paths
    (let [downstream (keys (:egress-ids serialized-task))]
      (map
       (fn [segment]
         (reduce
          (fn [{:keys [paths exclusions] :as all} entry]
            (if ((:flow/predicate entry) [event segment])
              (if (:flow/short-circuit? entry)
                (reduced {:paths (join-output-paths paths (:flow/to entry) downstream)
                          :exclusions (clojure.set/union (into #{} exclusions) (into #{} (:flow/exclude-keys entry)))})
                {:paths (join-output-paths paths (:flow/to entry) downstream)
                 :exclusions (clojure.set/union (into #{} exclusions) (into #{} (:flow/exclude-keys entry)))})
              all))
          {:paths #{} :exclusions #{}}
          compiled-flow-conditions)
         {:paths downstream})
       (:onyx.core/results event)))}))

(defn inject-batch-resources [event]
  (let [cycle-params {:onyx.core/lifecycle-id (java.util.UUID/randomUUID)}]
    (merge event cycle-params (l-ext/inject-batch-resources* event))))

(defn read-batch [event]
  (merge event (p-ext/read-batch event)))

(defn tag-messages [event]
  (merge
   event
   (if (= (:onyx/type (:onyx.core/task-map event)) :input)
     (let [event (update-in event 
                            [:onyx.core/batch]
                            (fn [batch]
                              (map (comp (partial add-completion-id event)
                                         (partial add-acker-id event)
                                         add-ack-value)
                                   batch)))]
       (doseq [raw-segment (:onyx.core/batch event)]
         (extensions/internal-ack-message
          (:onyx.core/messenger event)
          event
          (operation/peer-link event (:acker-id raw-segment) :acker-peer-site)
          (:id raw-segment)
          (:completion-id raw-segment)
          (:ack-val raw-segment)))
       event)
     event)))

(defn start-message-timeout-monitors [event]
  (when (= (:onyx/type (:onyx.core/task-map event)) :input)
    (doseq [m (:onyx.core/batch event)]
      (go (try (<! (timeout (or (:onyx/pending-timeout (:onyx.core/task-map event)) 60000)))
               (when (p-ext/pending? event (:id m))
                 (p-ext/retry-message event (:id m)))
               (catch Exception e
                 (taoensso.timbre/warn e))))))
  event)

(defn decompress-batch [event]
  (merge event {:onyx.core/decompressed (:onyx.core/batch event)}))

(defn try-complete-job [event]
  (when (sentinel-found? event)
    (if (p-ext/drained? event)
      (complete-job event)
      (p-ext/retry-message event (sentinel-id event))))
  event)

(defn strip-sentinel [{:keys [onyx.core/batch onyx.core/decompressed] :as event}]
  (merge
   event
   (when-let [k (.indexOf ^clojure.lang.LazySeq (map :message decompressed) :done)]
     {:onyx.core/batch (drop-nth k batch)
      :onyx.core/decompressed (drop-nth k decompressed)})))

(defn build-next-segment [prev-seg next-seg]
  {:id (:id prev-seg)
   :acker-id (:acker-id prev-seg)
   :completion-id (:completion-id prev-seg)
   :message next-seg
   :ack-val (acker/gen-ack-value)})

(defn copy-segment [prev-seg]
  (assoc (build-next-segment prev-seg (:message prev-seg))
    :ack-val (:ack-val prev-seg)))

(defn collect-next-segments [event input]
  (let [segments (p-ext/apply-fn event input)]
    (if (sequential? segments) segments (vector segments))))

(defn apply-fn-single [{:keys [onyx.core/batch onyx.core/decompressed] :as event}]
  (merge
   event
   (reduce
    (fn [rets thawed]
      (let [segments (collect-next-segments event (:message thawed))
            results (map (partial build-next-segment thawed) segments)
            tagged (acker/prefuse-vals (map :ack-val results))]
        (-> rets
            (update-in [:onyx.core/results] concat results)
            (assoc-in [:onyx.core/children thawed] tagged))))
    {:onyx.core/results []
     :onyx.core/children {}}
    decompressed)))

(defn apply-fn-batch [{:keys [onyx.core/batch onyx.core/decompressed] :as event}]
  ;; Batched functions intentionally ignore their outputs.
  (let [segments (map :message decompressed)]
    (p-ext/apply-fn event segments)
    (let [results (map copy-segment decompressed)]
      (merge event {:onyx.core/results results}))))

(defn apply-fn [event]
  (if (:onyx/side-effects-only? (:onyx.core/task-map event))
    (apply-fn-batch event)
    (apply-fn-single event)))

(defn compress-batch [event]
  (merge event (p-ext/compress-batch event)))

(defn write-batch [event]
  (merge event (p-ext/write-batch event)))

(defn close-batch-resources [event]
  (merge event (l-ext/close-batch-resources* event)))

(defn release-messages! [messenger event]
  (go
   (loop []
     (when-let [id (<! (:release-ch messenger))]
       (p-ext/ack-message event id)
       (recur)))))

(defn forward-completion-calls! [event completion-ch]
  (try
    (loop []
      (when-let [{:keys [id peer-id]} (<!! completion-ch)]
        (let [peer-link (operation/peer-link event peer-id :completion-peer-site)]
          (extensions/internal-complete-message (:onyx.core/messenger event) event id peer-link)
          (recur))))
    (catch Exception e
      (timbre/fatal e))))

(defn handle-exception [e restart-ch outbox-ch job-id]
  (warn e)
  (if (some #{(type e)} restartable-exceptions)
    (>!! restart-ch true)
    (let [entry (entry/create-log-entry :kill-job {:job job-id})]
      (>!! outbox-ch entry))))

(defn only-relevant-branches [flow task]
  (filter #(= (:flow/from %) task) flow))

(defn compile-flow-conditions [flow-conditions task-name f]
  (let [conditions (filter f (only-relevant-branches flow-conditions task-name))]
    (map
     (fn [condition]
       (assoc condition :flow/predicate (build-pred-fn (:flow/predicate condition) condition)))
     conditions)))

(defn compile-fc-norms [flow-conditions task-name]
  (compile-flow-conditions flow-conditions task-name (comp not :flow/thrown-exception?)))

(defn compile-fc-exs [flow-conditions task-name]
  (compile-flow-conditions flow-conditions task-name :flow/thrown-exception?))

(defn run-task-lifecycle [init-event kill-ch ex-f]
  (try
    (while (first (alts!! [kill-ch] :default true))
      (-> init-event
          (inject-batch-resources)
          (read-batch)
          (tag-messages)
          (decompress-batch)
          (start-message-timeout-monitors)
          (try-complete-job)
          (strip-sentinel)
          (apply-fn)
          (ack-messages)
          (route-output-paths)
          (compress-batch)
          (write-batch)
          (close-batch-resources)))
    (catch Exception e
      (ex-f e))))

(defn listen-for-sealer [job task init-event seal-ch outbox-ch]
  ;; TODO: only launch for output tasks
  (go
   (try
     (when (<! seal-ch)
       (p-ext/seal-resource init-event)
       (let [entry (entry/create-log-entry :seal-output {:job job :task task})]
         (>!! outbox-ch entry)))
     (catch Exception e
       (warn e)))))

(defrecord TaskLifeCycle [id log messenger-buffer messenger job-id task-id replica restart-ch kill-ch outbox-ch seal-resp-ch completion-ch opts]
  component/Lifecycle

  (start [component]
    (try
      (let [catalog (extensions/read-chunk log :catalog job-id)
            task (extensions/read-chunk log :task task-id)
            flow-conditions (extensions/read-chunk log :flow-conditions job-id)
            catalog-entry (find-task catalog (:name task))

            _ (taoensso.timbre/info (format "[%s] Starting Task LifeCycle for job %s, task %s" id job-id (:name task)))

            pipeline-data {:onyx.core/id id
                           :onyx.core/job-id job-id
                           :onyx.core/task-id task-id
                           :onyx.core/task (:name task)
                           :onyx.core/catalog catalog
                           :onyx.core/workflow (extensions/read-chunk log :workflow job-id)
                           :onyx.core/flow-conditions flow-conditions
                           :onyx.core/compiled-norm-fcs (compile-fc-norms flow-conditions (:name task))
                           :onyx.core/compiled-ex-fcs (compile-fc-exs flow-conditions (:name task))
                           :onyx.core/task-map catalog-entry
                           :onyx.core/serialized-task task
                           :onyx.core/params (resolve-calling-params catalog-entry opts)
                           :onyx.core/drained-back-off (or (:onyx.peer/drained-back-off opts) 400)
                           :onyx.core/log log
                           :onyx.core/messenger-buffer messenger-buffer
                           :onyx.core/messenger messenger
                           :onyx.core/outbox-ch outbox-ch
                           :onyx.core/seal-response-ch seal-resp-ch
                           :onyx.core/peer-opts opts
                           :onyx.core/replica replica
                           :onyx.core/state (atom {})}

            ex-f (fn [e] (handle-exception e restart-ch outbox-ch job-id))
            pipeline-data (merge pipeline-data (l-ext/inject-lifecycle-resources* pipeline-data))]

        (while (and (first (alts!! [kill-ch] :default true))
                    (not (:onyx.core/start-lifecycle? (munge-start-lifecycle pipeline-data))))
          (Thread/sleep (or (:onyx.peer/sequential-back-off opts) 2000)))

        (>!! outbox-ch (entry/create-log-entry :signal-ready {:id id}))

        (loop [replica-state @replica]
          (when (and (first (alts!! [kill-ch] :default true))
                     (or (not (common/job-covered? replica-state job-id))
                         (not (common/any-ackers? replica-state job-id))))
            (taoensso.timbre/info (format "[%s] Not enough virtual peers have warmed up to start the job yet, backing off and trying again..." id))
            (Thread/sleep 500)
            (recur @replica)))

        (release-messages! messenger pipeline-data)
        (thread (forward-completion-calls! pipeline-data completion-ch))
        (listen-for-sealer job-id task-id pipeline-data seal-resp-ch outbox-ch)
        (thread (run-task-lifecycle pipeline-data kill-ch ex-f))

        (assoc component :pipeline-data pipeline-data :seal-ch seal-resp-ch))
      (catch Exception e
        (handle-exception e restart-ch outbox-ch job-id)
        component)))

  (stop [component]
    (taoensso.timbre/info (format "[%s] Stopping Task LifeCycle for %s" id (:onyx.core/task (:pipeline-data component))))
    (let [event (:pipeline-data component)]
      (l-ext/close-lifecycle-resources* event)

      (close! (:seal-ch component))

      (let [state @(:onyx.core/state event)]
        (doseq [[_ link] (get-in state [:links :send-peer-site])]
          (extensions/close-peer-connection (:onyx.core/messenger event) event link))
        (doseq [[_ link] (get-in state [:links :acker-peer-site])]
          (extensions/close-peer-connection (:onyx.core/messenger event) event link))
        (doseq [[_ link] (get-in state [:links :completion-peer-site])]
          (extensions/close-peer-connection (:onyx.core/messenger event) event link))))
    component))

(defn task-lifecycle [args {:keys [id log messenger-buffer messenger job task replica
                                   restart-ch kill-ch outbox-ch seal-ch completion-ch opts]}]
  (map->TaskLifeCycle {:id id :log log :messenger-buffer messenger-buffer
                       :messenger messenger :job-id job :task-id task :restart-ch restart-ch
                       :kill-ch kill-ch :outbox-ch outbox-ch
                       :replica replica :seal-resp-ch seal-ch :completion-ch completion-ch
                       :opts opts}))

(dire/with-post-hook! #'munge-start-lifecycle
  (fn [{:keys [onyx.core/id onyx.core/lifecycle-id onyx.core/start-lifecycle?] :as event}]
    (when-not start-lifecycle?
      (timbre/info (format "[%s / %s] Sequential task currently has queue consumers. Backing off and retrying..." id lifecycle-id)))))

(dire/with-post-hook! #'inject-batch-resources
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

(dire/with-post-hook! #'close-batch-resources
  (fn [{:keys [onyx.core/id onyx.core/lifecycle-id]}]
    (taoensso.timbre/trace (format "[%s / %s] Closed batch plugin resources" id lifecycle-id))))

