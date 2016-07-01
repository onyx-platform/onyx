(ns ^:no-doc onyx.peer.task-lifecycle
    (:require [clojure.core.async :refer [alts!! <!! >!! <! >! poll! timeout chan close! thread go]]
              [com.stuartsierra.component :as component]
              [taoensso.timbre :refer [info error warn trace fatal]]
              [onyx.schema :refer [Event]]
              [schema.core :as s]
              [onyx.static.rotating-seq :as rsc]
              [onyx.log.commands.common :as common]
              [onyx.log.entry :as entry]
              [onyx.monitoring.measurements :refer [emit-latency emit-latency-value emit-count]]
              [onyx.static.planning :refer [find-task]]
              [onyx.static.uuid :as uuid]
              [onyx.messaging.acking-daemon :as acker]
              [onyx.peer.task-compile :as c]
              [onyx.windowing.window-compile :as wc]
              [onyx.lifecycles.lifecycle-invoke :as lc]
              [onyx.peer.pipeline-extensions :as p-ext]
              [onyx.peer.function :as function]
              [onyx.peer.operation :as operation]
              [onyx.extensions :as extensions]
              [onyx.types :refer [->Ack ->Results ->MonitorEvent dec-count! inc-count! map->Event map->Compiled]]
              [onyx.peer.window-state :as ws]
              [onyx.peer.transform :refer [apply-fn]]
              [onyx.peer.grouping :as g]
              [onyx.flow-conditions.fc-routing :as r]
              [onyx.log.commands.peer-replica-view :refer [peer-site]]
              [onyx.static.logging :as logger]
              [onyx.state.state-extensions :as state-extensions]
              [onyx.static.default-vals :refer [defaults arg-or-default]]))

(defrecord TaskState [timeout-pool])

(s/defn windowed-task? [event]
  (or (not-empty (:onyx.core/windows event))
      (not-empty (:onyx.core/triggers event))))

(defn exactly-once-task? [event]
  (contains? (:onyx.core/task-map event) :onyx/uniqueness-key))

(defn resolve-log [{:keys [onyx.core/peer-opts] :as pipeline}]
  (let [log-impl (arg-or-default :onyx.peer/state-log-impl peer-opts)] 
    (assoc pipeline :onyx.core/state-log (if (windowed-task? pipeline) 
                                           (state-extensions/initialize-log log-impl pipeline)))))

(defn resolve-filter-state [{:keys [onyx.core/peer-opts] :as pipeline}]
  (let [filter-impl (arg-or-default :onyx.peer/state-filter-impl peer-opts)] 
    (assoc pipeline 
           :onyx.core/filter-state 
           (if (windowed-task? pipeline)
             (if (exactly-once-task? pipeline) 
               (atom (state-extensions/initialize-filter filter-impl pipeline)))))))

(defn start-window-state-thread!
  [ex-f {:keys [onyx.core/windows] :as event}]
  (if (empty? windows) 
    event
    (let [state-ch (chan 1)
          event (assoc event :onyx.core/state-ch state-ch)
          process-state-thread-ch (thread (ws/process-state-loop event ex-f))] 
      (assoc event :onyx.core/state-thread-ch process-state-thread-ch))))

(defn stop-window-state-thread!
  [{:keys [onyx.core/windows onyx.core/state-ch onyx.core/state-thread-ch] :as event}]
  (when-not (empty? windows)
    (close! state-ch)
    ;; Drain state-ch to unblock any blocking puts
    (while (poll! state-ch))
    (<!! state-thread-ch)))

(s/defn start-lifecycle? [event]
  (let [rets (lc/invoke-start-task (:onyx.core/compiled event) event)]
    (when-not (:onyx.core/start-lifecycle? rets)
      (info (:onyx.core/log-prefix event) "Peer chose not to start the task yet. Backing off and retrying..."))
    rets))

(defn add-acker-id [id m]
  (assoc m :acker-id id))

(defn add-completion-id [id m]
  (assoc m :completion-id id))

(s/defn sentinel-found? [event :- Event]
  (seq (filter #(= :done (:message %))
               (:onyx.core/batch event))))

(s/defn complete-job
  [{:keys [onyx.core/job-id onyx.core/task-id onyx.core/emitted-exhausted?]
    :as event} :- Event]
  (when-not @emitted-exhausted?
    (let [entry (entry/create-log-entry :exhaust-input {:job job-id :task task-id})]
      (>!! (:onyx.core/outbox-ch event) entry)
      (reset! emitted-exhausted? true))))

(s/defn sentinel-id [event :- Event]
  (:id (first (filter #(= :done (:message %))
                      (:onyx.core/batch event)))))

(defrecord AccumAckSegments [ack-val segments retries])

(defn add-segments [accum routes hash-group leaf]
  (if (empty? routes)
    accum
    (if-let [route (first routes)]
      (let [ack-val (acker/gen-ack-value)
            grp (get hash-group route)
            leaf* (-> leaf
                      (assoc :ack-val ack-val)
                      (assoc :hash-group grp)
                      (assoc :route route))
            fused-ack (bit-xor ^long (:ack-val accum) ^long ack-val)]
        (-> accum
            (assoc :ack-val fused-ack)
            (update :segments (fn [s] (conj! s leaf*)))
            (add-segments (rest routes) hash-group leaf)))
      (add-segments accum (rest routes) hash-group leaf))))

(defn add-from-leaf [event {:keys [egress-ids task->group-by-fn] :as compiled} result
                     root leaves start-ack-val accum {:keys [message] :as leaf}]
  (let [routes (r/route-data event compiled result message)
        message* (r/flow-conditions-transform message routes event compiled)
        hash-group (g/hash-groups message* egress-ids task->group-by-fn)
        leaf* (if (= message message*)
                leaf
                (assoc leaf :message message*))]
    (if (= :retry (:action routes))
      (assoc accum :retries (conj! (:retries accum) root))
      (add-segments accum (:flow routes) hash-group leaf*))))

(s/defn add-from-leaves
  "Flattens root/leaves into an xor'd ack-val, and accumulates new segments and retries"
  [segments retries event :- Event result compiled]
  (let [root (:root result)
        leaves (:leaves result)
        start-ack-val (or (:ack-val root) 0)]
    (reduce (fn [accum leaf]
              (lc/invoke-flow-conditions
               add-from-leaf
               event compiled result root leaves
               start-ack-val accum leaf))
            (->AccumAckSegments start-ack-val segments retries)
            leaves)))

(defn persistent-results! [results]
  (->Results (:tree results)
             (persistent! (:acks results))
             (persistent! (:segments results))
             (persistent! (:retries results))))

(defn build-new-segments
  [compiled {:keys [onyx.core/results onyx.core/monitoring] :as event}]
  (emit-latency 
   :peer-batch-latency 
   monitoring
   #(let [results (reduce (fn [accumulated result]
                            (let [root (:root result)
                                  segments (:segments accumulated)
                                  retries (:retries accumulated)
                                  ret (add-from-leaves segments retries event result compiled)
                                  new-ack (->Ack (:id root) (:completion-id root) (:ack-val ret) (atom 1) nil)
                                  acks (conj! (:acks accumulated) new-ack)]
                              (->Results (:tree results) acks (:segments ret) (:retries ret))))
                          results
                          (:tree results))]
      (assoc event :onyx.core/results (persistent-results! results)))))

(s/defn ack-segments :- Event
  [{:keys [peer-replica-view task-map state messenger monitoring] :as compiled} 
   {:keys [onyx.core/results] :as event} :- Event]
  (doseq [[acker-id acks] (->> (:acks results)
                               (filter dec-count!)
                               (group-by :completion-id))]
    (when-let [site (peer-site peer-replica-view acker-id)]
      (emit-latency :peer-ack-segments
                    monitoring
                    #(extensions/internal-ack-segments messenger site acks))))
  event)

(s/defn flow-retry-segments :- Event
  [{:keys [peer-replica-view state messenger monitoring] :as compiled}
   event :- Event]
  (let [{:keys [retries]} (:onyx.core/results event)
        event (if (not (empty? retries))
                (update-in event
                           [:onyx.core/results :acks]
                           (fn [acks]
                             (filterv (comp not (set (map :id retries)) :id) acks)))
                event)]
    (doseq [root (:retries (:onyx.core/results event))]
      (when-let [site (peer-site peer-replica-view (:completion-id root))]
        (emit-latency
         :peer-retry-segment
         monitoring
         #(extensions/internal-retry-segment messenger (:id root) site))))
    event))

(s/defn gen-lifecycle-id
  [event]
  (assoc event :onyx.core/lifecycle-id (uuid/random-uuid)))

(defn handle-backoff! [event]
  (let [batch (:onyx.core/batch event)]
    (when (and (= (count batch) 1)
               (= (:message (first batch)) :done))
      (Thread/sleep (:onyx.core/drained-back-off event)))))

(defn read-batch
  [{:keys [peer-replica-view task-type pipeline] :as compiled}
   event]
  (if (and (= task-type :input) (:backpressure? @peer-replica-view))
    (assoc event :onyx.core/batch '())
    (let [rets (merge event (p-ext/read-batch pipeline event))
          rets (merge event (lc/invoke-after-read-batch compiled rets))]
      (handle-backoff! event)
      rets)))

(s/defn tag-messages :- Event
  [{:keys [peer-replica-view task-type id] :as compiled} event :- Event]
  (if (= task-type :input)
    (update event
            :onyx.core/batch
            (fn [batch]
              (map (fn [segment]
                     (add-acker-id ((:pick-acker-fn @peer-replica-view))
                                   (add-completion-id id segment)))
                   batch)))
    event))

(s/defn add-messages-to-timeout-pool :- Event
  [{:keys [task-type state]} event :- Event]
  (when (= task-type :input)
    (swap! state update :timeout-pool rsc/add-to-head
           (map :id (:onyx.core/batch event))))
  event)

(s/defn process-sentinel :- Event
  [{:keys [task-type monitoring pipeline]} event :- Event]
  (if (and (= task-type :input)
           (sentinel-found? event))
    (do
      (extensions/emit monitoring (->MonitorEvent :peer-sentinel-found))
      (if (p-ext/drained? pipeline event)
        (complete-job event)
        (p-ext/retry-segment pipeline event (sentinel-id event)))
      (update event
              :onyx.core/batch
              (fn [batch]
                (remove (fn [v] (= :done (:message v)))
                        batch))))
    event))

(defn replay-windows-from-log
  [{:keys [onyx.core/log-prefix onyx.core/windows-state
           onyx.core/filter-state onyx.core/state-log] :as event}]
  (when (windowed-task? event)
    (swap! windows-state 
           (fn [windows-state] 
             (let [exactly-once? (exactly-once-task? event)
                   apply-fn (fn [ws [unique-id window-logs]]
                              (if exactly-once? 
                                (swap! filter-state state-extensions/apply-filter-id event unique-id))
                              (mapv ws/play-entry ws window-logs))
                   replayed-state (state-extensions/playback-log-entries state-log event windows-state apply-fn)]
               (trace log-prefix (format "Replayed state: %s" replayed-state))
               replayed-state))))
  event)

(s/defn write-batch :- Event 
  [compiled event :- Event]
  (let [rets (merge event (p-ext/write-batch (:pipeline compiled) event))]
    (emit-count :peer-processed-segments (:onyx.core/monitoring event) (count (:onyx.core/batch event)))
    (trace (:log-prefix compiled) (format "Wrote %s segments" (count (:onyx.core/results rets))))
    rets))

(defn launch-aux-threads!
  [messenger {:keys [onyx.core/pipeline
                     onyx.core/compiled
                     onyx.core/messenger-buffer
                     onyx.core/monitoring
                     onyx.core/replica
                     onyx.core/peer-replica-view
                     onyx.core/state] :as event}
   outbox-ch seal-ch completion-ch task-kill-ch]
  (thread
   (try
     (let [{:keys [retry-ch release-ch]} messenger-buffer]
       (loop []
         (when-let [[v ch] (alts!! [task-kill-ch completion-ch seal-ch release-ch retry-ch])]
           (when v
             (cond (= ch release-ch)
                   (->> (p-ext/ack-segment pipeline event v)
                        (lc/invoke-after-ack event compiled v))

                   (= ch completion-ch)
                   (let [{:keys [id peer-id]} v
                         site (peer-site peer-replica-view peer-id)]
                     (when site 
                       (emit-latency :peer-complete-segment
                                     monitoring
                                     #(extensions/internal-complete-segment messenger id site))))

                   (= ch retry-ch)
                   (->> (p-ext/retry-segment pipeline event v)
                        (lc/invoke-after-retry event compiled v))

                   (= ch seal-ch)
                   (do
                     (p-ext/seal-resource pipeline event)
                     (let [entry (entry/create-log-entry :seal-output {:job (:onyx.core/job-id event)
                                                                       :task (:onyx.core/task-id event)})]
                       (>!! outbox-ch entry))))
             (recur)))))
     (catch Throwable e
       (fatal (logger/merge-error-keys e (:onyx.core/task-information event) "Internal error. Failed to read core.async channels"))))))

(defn input-retry-segments! [messenger {:keys [onyx.core/pipeline
                                               onyx.core/compiled]
                                        :as event}
                             input-retry-timeout task-kill-ch]
  (go
    (when (= :input (:onyx/type (:onyx.core/task-map event)))
      (loop []
        (let [timeout-ch (timeout input-retry-timeout)
              ch (second (alts!! [timeout-ch task-kill-ch]))]
          (when (= ch timeout-ch)
            (let [tail (last (get-in @(:onyx.core/state event) [:timeout-pool]))]
              (doseq [m tail]
                (when (p-ext/pending? pipeline event m)
                  (trace (:log-prefix compiled) (format "Input retry message %s" m))
                  (->> (p-ext/retry-segment pipeline event m)
                       (lc/invoke-after-retry event compiled m))))
              (swap! (:onyx.core/state event) update :timeout-pool rsc/expire-bucket)
              (recur))))))))

(defn handle-exception [task-info log e group-ch outbox-ch id job-id]
  (let [data (ex-data e)
        inner (.getCause e)]
    (if (:onyx.core/lifecycle-restart? data)
      (do (warn (logger/merge-error-keys inner task-info "Caught exception inside task lifecycle. Rebooting the task."))
          (>!! group-ch [:restart-vpeer id]))
      (do (warn (logger/merge-error-keys e task-info "Handling uncaught exception thrown inside task lifecycle - killing this job."))
          (let [entry (entry/create-log-entry :kill-job {:job job-id})]
            (extensions/write-chunk log :exception e job-id)
            (>!! outbox-ch entry))))))

(s/defn assign-windows :- Event
  [compiled {:keys [onyx.core/windows] :as event}]
  (when-not (empty? windows)
    (let [{:keys [tree acks]} (:onyx.core/results event)]
      (when-not (empty? tree)
        ;; Increment that the messages aren't fully acked until process-state has processed the data
        (run! inc-count! acks)
        (>!! (:onyx.core/state-ch event) [:new-segment event #(ack-segments compiled event)]))))
  event)

(defn run-task-lifecycle
  "The main task run loop, read batch, ack messages, etc."
  [{:keys [onyx.core/compiled onyx.core/task-information] :as init-event}
   seal-ch kill-ch ex-f]
  (try
    (while (first (alts!! [seal-ch kill-ch] :default true))
      (->> init-event
           (gen-lifecycle-id)
           (lc/invoke-before-batch compiled)
           (lc/invoke-read-batch read-batch compiled)
           (tag-messages compiled)
           (add-messages-to-timeout-pool compiled)
           (process-sentinel compiled)
           (apply-fn compiled)
           (build-new-segments compiled)
           (lc/invoke-assign-windows assign-windows compiled)
           (lc/invoke-write-batch write-batch compiled)
           (flow-retry-segments compiled)
           (lc/invoke-after-batch compiled)
           (ack-segments compiled)))
    (catch Throwable e
      (ex-f e))))

(defn validate-pending-timeout [pending-timeout opts]
  (when (> pending-timeout (arg-or-default :onyx.messaging/ack-daemon-timeout opts))
    (throw (ex-info "Pending timeout cannot be greater than acking daemon timeout"
                    {:opts opts :pending-timeout pending-timeout}))))


(defn build-pipeline [task-map pipeline-data]
  (let [kw (:onyx/plugin task-map)]
    (try
      (if (#{:input :output} (:onyx/type task-map))
        (case (:onyx/language task-map)
          :java (operation/instantiate-plugin-instance (name kw) pipeline-data)
          (let [user-ns (namespace kw)
                user-fn (name kw)
                pipeline (if (and user-ns user-fn)
                           (if-let [f (ns-resolve (symbol user-ns) (symbol user-fn))]
                             (f pipeline-data)))]
            (or pipeline
                (throw (ex-info "Failure to resolve plugin builder fn. Did you require the file that contains this symbol?" {:kw kw})))))
        (onyx.peer.function/function pipeline-data))
      (catch Throwable e
        (throw e)))))

(defrecord TaskInformation 
    [id log job-id task-id 
     workflow catalog task flow-conditions windows filtered-windows triggers lifecycles task-map]
  component/Lifecycle
  (start [component]
    (let [catalog (extensions/read-chunk log :catalog job-id)
          task (extensions/read-chunk log :task job-id task-id)
          flow-conditions (extensions/read-chunk log :flow-conditions job-id)
          windows (extensions/read-chunk log :windows job-id)
          filtered-windows (vec (wc/filter-windows windows (:name task)))
          window-ids (set (map :window/id filtered-windows))
          triggers (extensions/read-chunk log :triggers job-id)
          filtered-triggers (filterv #(window-ids (:trigger/window-id %)) triggers)
          workflow (extensions/read-chunk log :workflow job-id)
          lifecycles (extensions/read-chunk log :lifecycles job-id)
          metadata (extensions/read-chunk log :job-metadata job-id)
          task-map (find-task catalog (:name task))]
      (assoc component 
             :workflow workflow :catalog catalog :task task :task-name (:name task) :flow-conditions flow-conditions
             :windows windows :filtered-windows filtered-windows :triggers triggers :filtered-triggers filtered-triggers 
             :lifecycles lifecycles :task-map task-map :metadata metadata)))
  (stop [component]
    (assoc component 
           :catalog nil
           :task nil
           :flow-conditions nil
           :windows nil 
           :filtered-windows nil
           :triggers nil
           :lifecycles nil
           :metadata nil
           :task-map nil)))

(defn new-task-information [peer-state task-state]
  (map->TaskInformation (select-keys (merge peer-state task-state) [:id :log :job-id :task-id])))

(defn safe-start [phase f {:keys [onyx.core/compiled] :as event}]
  (lc/restartable-invocation
   event
   phase
   (:compiled-handle-exception-fn compiled)
   f
   event))

(defrecord TaskLifeCycle
    [id log messenger-buffer messenger job-id task-id replica peer-replica-view group-ch log-prefix
     kill-ch outbox-ch seal-ch completion-ch opts task-kill-ch scheduler-event task-monitoring task-information]
  component/Lifecycle

  (start [component]
    (try
      (let [{:keys [workflow catalog task flow-conditions windows filtered-windows
                    triggers filtered-triggers lifecycles task-map metadata]} task-information
            ;; Number of buckets in the timeout pool is covered over a 60 second
            ;; interval, moving each bucket back 60 seconds / N buckets
            input-retry-timeout (arg-or-default :onyx/input-retry-timeout task-map)
            pending-timeout (arg-or-default :onyx/pending-timeout task-map)
            r-seq (rsc/create-r-seq pending-timeout input-retry-timeout)
            state (atom (->TaskState r-seq))

            _ (validate-pending-timeout pending-timeout opts)

            log-prefix (logger/log-prefix task-information)

            pipeline-data {:onyx.core/id id
                           :onyx.core/job-id job-id
                           :onyx.core/task-id task-id
                           :onyx.core/task (:name task)
                           :onyx.core/catalog catalog
                           :onyx.core/workflow workflow
                           :onyx.core/flow-conditions flow-conditions
                           :onyx.core/lifecycles lifecycles
                           :onyx.core/metadata metadata
                           :onyx.core/compiled (map->Compiled {})
                           :onyx.core/task-map task-map
                           :onyx.core/serialized-task task
                           :onyx.core/drained-back-off (arg-or-default :onyx.peer/drained-back-off opts)
                           :onyx.core/log log
                           :onyx.core/messenger-buffer messenger-buffer
                           :onyx.core/messenger messenger
                           :onyx.core/monitoring task-monitoring
                           :onyx.core/task-information task-information
                           :onyx.core/outbox-ch outbox-ch
                           :onyx.core/seal-ch seal-ch
                           :onyx.core/group-ch group-ch
                           :onyx.core/task-kill-ch task-kill-ch
                           :onyx.core/kill-ch kill-ch
                           :onyx.core/peer-opts opts
                           :onyx.core/fn (operation/resolve-task-fn task-map)
                           :onyx.core/replica replica
                           :onyx.core/peer-replica-view peer-replica-view
                           :onyx.core/log-prefix log-prefix
                           :onyx.core/state state
                           :onyx.core/emitted-exhausted? (atom false)}

            _ (info log-prefix "Warming up task lifecycle" task)

            add-pipeline (fn [event]
                           (assoc event 
                                  :onyx.core/pipeline 
                                  (build-pipeline task-map event)))

            pipeline-data (->> pipeline-data
                               c/task-params->event-map
                               c/flow-conditions->event-map
                               c/lifecycles->event-map
                               (c/windows->event-map filtered-windows filtered-triggers)
                               (c/triggers->event-map filtered-triggers)
                               (safe-start :build-plugin add-pipeline)
                               c/task->event-map)

            ex-f (fn [e] (handle-exception task-information log e group-ch outbox-ch id job-id))
            _ (while (and (first (alts!! [kill-ch task-kill-ch] :default true))
                          (not (start-lifecycle? pipeline-data)))
                (Thread/sleep (arg-or-default :onyx.peer/peer-not-ready-back-off opts)))

            pipeline-data (->> pipeline-data
                               (lc/invoke-before-task-start (:onyx.core/compiled pipeline-data))
                               resolve-filter-state
                               resolve-log
                               (safe-start :replay-windows replay-windows-from-log)
                               (start-window-state-thread! ex-f))]

        (>!! outbox-ch (entry/create-log-entry :signal-ready {:id id}))

        (loop [replica-state @replica]
          (when (and (first (alts!! [kill-ch task-kill-ch] :default true))
                     (or (not (common/job-covered? replica-state job-id))
                         (not (common/any-ackers? replica-state job-id))))
            (info log-prefix "Not enough virtual peers have warmed up to start the task yet, backing off and trying again...")
            (Thread/sleep (arg-or-default :onyx.peer/job-not-ready-back-off opts))
            (recur @replica)))

        (info log-prefix "Enough peers are active, starting the task")

        (let [input-retry-segments-ch (input-retry-segments! messenger pipeline-data input-retry-timeout task-kill-ch)
              aux-ch (launch-aux-threads! messenger pipeline-data outbox-ch seal-ch completion-ch task-kill-ch)
              task-lifecycle-ch (thread (run-task-lifecycle pipeline-data seal-ch kill-ch ex-f))]
          (s/validate Event pipeline-data)
          (assoc component
                 :pipeline-data pipeline-data
                 :log-prefix log-prefix
                 :task-information task-information
                 :seal-ch seal-ch
                 :task-kill-ch task-kill-ch
                 :kill-ch kill-ch
                 :task-lifecycle-ch task-lifecycle-ch
                 :input-retry-segments-ch input-retry-segments-ch
                 :aux-ch aux-ch)))
      (catch Throwable e
        (handle-exception task-information log e group-ch outbox-ch id job-id)
        component)))

  (stop [component]
    (if-let [task-name (:onyx.core/task (:pipeline-data component))]
      (info (:log-prefix component) "Stopping task lifecycle")
      (warn (:log-prefix component) "Stopping task lifecycle, failed to initialize task set up"))

    (when-let [event (:pipeline-data component)]
      (when-not (empty? (:onyx.core/triggers event))
        (>!! (:onyx.core/state-ch event) [(:scheduler-event component) event #()]))

      (stop-window-state-thread! event)

      ;; Ensure task operations are finished before closing peer connections
      (close! (:seal-ch component))
      (<!! (:task-lifecycle-ch component))
      (close! (:task-kill-ch component))

      (<!! (:input-retry-segments-ch component))
      (<!! (:aux-ch component))

      (when-let [state-log (:onyx.core/state-log event)] 
        (state-extensions/close-log state-log event))

      (when-let [filter-state (:onyx.core/filter-state event)] 
        (when (exactly-once-task? event)
          (state-extensions/close-filter @filter-state event)))

      ((:compiled-after-task-fn (:onyx.core/compiled event)) event))

    (assoc component
           :pipeline-data nil
           :seal-ch nil
           :aux-ch nil
           :input-retry-segments-ch nil
           :task-lifecycle-ch nil)))

(defn task-lifecycle [peer-state task-state]
  (map->TaskLifeCycle (merge peer-state task-state)))
