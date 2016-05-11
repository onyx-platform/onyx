(ns ^:no-doc onyx.peer.task-lifecycle
  (:require [clojure.core.async :refer [alts!! <!! >!! <! >! poll! timeout chan close! thread go]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info error warn trace fatal]]
            [onyx.schema :as os]
            [schema.core :as s]
            [onyx.log.commands.common :as common]
            [onyx.log.entry :as entry]
            [onyx.monitoring.measurements :refer [emit-latency emit-latency-value]]
            [onyx.static.planning :refer [find-task]]
            [onyx.static.uuid :as uuid]
            [onyx.peer.task-compile :as c]
            [onyx.windowing.window-compile :as wc]
            [onyx.lifecycles.lifecycle-invoke :as lc]
            [onyx.peer.function :as function]
            [onyx.peer.operation :as operation]
            [onyx.compression.nippy :refer [messaging-decompress]]
            [onyx.messaging.messenger :as m]
            [onyx.messaging.messenger-replica :as ms]
            [onyx.log.replica :refer [base-replica]]
            [onyx.extensions :as extensions]
            [onyx.types :refer [->Results ->MonitorEvent map->Event dec-count! inc-count!]]
            [onyx.peer.window-state :as ws]
            [onyx.peer.transform :refer [apply-fn]]
            [onyx.peer.grouping :as g]
            [onyx.plugin.onyx-input :as oi]
            [onyx.plugin.onyx-output :as oo]
            [onyx.plugin.onyx-plugin :as op]
            [onyx.flow-conditions.fc-routing :as r]
            [onyx.static.logging :as logger]
            [onyx.state.state-extensions :as state-extensions]
            [onyx.static.default-vals :refer [defaults arg-or-default]]
            [onyx.messaging.aeron :as messaging]
            [onyx.messaging.common :as mc]))

(s/defn windowed-task? [event]
  (or (not-empty (:windows event))
      (not-empty (:triggers event))))

(defn exactly-once-task? [event]
  (contains? (:task-map event) :onyx/uniqueness-key))

(defn resolve-log [{:keys [peer-opts] :as pipeline}]
  (let [log-impl (arg-or-default :onyx.peer/state-log-impl peer-opts)] 
    (assoc pipeline :state-log (if (windowed-task? pipeline) 
                                           (state-extensions/initialize-log log-impl pipeline)))))

(defn resolve-filter-state [{:keys [peer-opts] :as pipeline}]
  (let [filter-impl (arg-or-default :onyx.peer/state-filter-impl peer-opts)] 
    (assoc pipeline 
           :filter-state 
           (if (windowed-task? pipeline)
             (if (exactly-once-task? pipeline) 
               (atom (state-extensions/initialize-filter filter-impl pipeline)))))))

(defn start-window-state-thread!
  [ex-f {:keys [windows] :as event}]
  (if (empty? windows) 
    event
    (let [state-ch (chan 1)
          event (assoc event :state-ch state-ch)
          process-state-thread-ch (thread (ws/process-state-loop event ex-f))] 
      (assoc event :state-thread-ch process-state-thread-ch))))

(defn stop-window-state-thread!
  [{:keys [windows state-ch state-thread-ch] :as event}]
  (when-not (empty? windows)
    (close! state-ch)
    ;; Drain state-ch to unblock any blocking puts
    (while (poll! state-ch))
    (<!! state-thread-ch)))

(s/defn start-lifecycle? [event]
  (let [rets (lc/invoke-start-task event)]
    (when-not (:start-lifecycle? rets)
      (info (:log-prefix event) "Peer chose not to start the task yet. Backing off and retrying..."))
    rets))

(defrecord SegmentRetries [segments retries])

(defn add-from-leaf 
  [{:keys [egress-ids task->group-by-fn] :as event} 
   result root leaves accum {:keys [message] :as leaf}]
  (let [routes (r/route-data event result message)
        message* (r/flow-conditions-transform message routes event)
        hash-group (g/hash-groups message* (keys egress-ids) task->group-by-fn)
        leaf* (if (= message message*)
                leaf
                (assoc leaf :message message*))]
    (if (= :retry (:action routes))
      (assoc accum :retries (conj! (:retries accum) root))
      (update accum :segments (fn [s] 
                                (conj! s (-> leaf*
                                             (assoc :flow (:flow routes))
                                             (assoc :hash-group hash-group))))))))

(s/defn add-from-leaves
  "Flattens root/leaves into an xor'd ack-val, and accumulates new segments and retries"
  [segments retries event :- os/Event result]
  (let [root (:root result)
        leaves (:leaves result)]
    (reduce (fn [accum leaf]
              (lc/invoke-flow-conditions add-from-leaf event result root leaves accum leaf))
            (->SegmentRetries segments retries)
            leaves)))

(defn persistent-results! [results]
  (->Results (:tree results)
             (persistent! (:segments results))
             (persistent! (:retries results))))

(defn build-new-segments
  [{:keys [results] :as event}]
  (let [results (reduce (fn [accumulated result]
                          (let [root (:root result)
                                segments (:segments accumulated)
                                retries (:retries accumulated)
                                ret (add-from-leaves segments retries event result)]
                            (->Results (:tree results) (:segments ret) (:retries ret))))
                        results
                        (:tree results))]
    (assoc event :results (persistent-results! results))))

; (s/defn flow-retry-segments :- Event
;   [{:keys [task-state state messenger monitoring results] :as event} 
;   (doseq [root (:retries results)]
;     (when-let [site (peer-site task-state (:completion-id root))]
;       (emit-latency :peer-retry-segment
;                     monitoring
;                     #(extensions/internal-retry-segment messenger (:id root) site))))
;   event)

(s/defn gen-lifecycle-id
  [event]
  (assoc event :lifecycle-id (uuid/random-uuid)))

(def input-readers
  {:input #'function/read-input-batch
   :function #'function/read-function-batch
   :output #'function/read-function-batch})

(defn read-batch
  [{:keys [task-type pipeline] :as event}]
  (let [f (get input-readers task-type)
        rets (merge event (f event))]
    (merge event (lc/invoke-after-read-batch rets))))

(defn replay-windows-from-log
  [{:keys [log-prefix windows-state
           filter-state state-log] :as event}]
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

(s/defn write-batch :- os/Event 
  [event :- os/Event]
  (let [rets (merge event (oo/write-batch (:pipeline event) event))]
    (trace (:log-prefix event) (format "Wrote %s segments" (count (:segments (:results rets)))))
    rets))

(defn handle-exception [task-info log e restart-ch outbox-ch job-id]
  (let [data (ex-data e)]
    (if (:lifecycle-restart? data)
      (do (warn (logger/merge-error-keys (:original-exception data) task-info "Caught exception inside task lifecycle. Rebooting the task."))
          (close! restart-ch))
      (do (warn (logger/merge-error-keys e task-info "Handling uncaught exception thrown inside task lifecycle - killing this job."))
          (let [entry (entry/create-log-entry :kill-job {:job job-id})]
            (extensions/write-chunk log :exception e job-id)
            (>!! outbox-ch entry))))))

(s/defn assign-windows :- os/Event
  [{:keys [windows] :as event}]
  (when-not (empty? windows)
    (let [{:keys [tree]} (:results event)]
      (throw (ex-info "Assign windows needs an acking implementation to use BookKeeper in async mode. Suggest use of synchronous mode for now."))))
  event)

(defn emit-barriers [{:keys [task-map messenger id pipeline barriers] :as event}]
  (cond (= :input (:onyx/type task-map)) 
        (let [new-messenger (m/emit-barrier messenger)
              barrier-info {:checkpoint (oi/checkpoint pipeline)
                            :completed? (oi/completed? pipeline)}] 
          (-> event
              (assoc :messenger new-messenger)
              (assoc-in [:barriers (m/replica-version new-messenger) (m/epoch new-messenger)] barrier-info)))

        (and (= :function (:onyx/type task-map)) 
             (m/all-barriers-seen? messenger))
        (assoc event :messenger (m/emit-barrier messenger))

        :else
        event))

(defn ack-barriers [{:keys [task-map messenger id pipeline barriers] :as event}]
  (if (and (= :output (:onyx/type task-map)) 
           (m/all-barriers-seen? messenger))
     (assoc event :messenger (m/ack-barrier messenger))
    event))

(s/defn complete-job [{:keys [job-id task-id] :as event} :- os/Event]
  (let [entry (entry/create-log-entry :exhaust-input {:job job-id :task task-id})]
    (>!! (:outbox-ch event) entry)))

(defn receive-acks [{:keys [task-map messenger id pipeline barriers opts] :as event}]
  (if (= :input (:onyx/type task-map)) 
    (let [new-messenger (m/receive-acks messenger)]
      (if-let [ack-barrier (m/all-acks-seen? new-messenger)]
        ;; TODO: Should checkpoint offsets here
        (let [completed? (get-in barriers [(:replica-version ack-barrier) (:epoch ack-barrier) :completed?])]
          (when completed?
            (complete-job event)
            (Thread/sleep (arg-or-default :onyx.peer/drained-back-off opts)))
          (assoc event :messenger (m/flush-acks messenger)))
        event))
    event))

(defn run-task-lifecycle
  "The main task run loop, read batch, ack messages, etc."
  [{:keys [messenger task-information replica] :as init-event}
   kill-ch ex-f]
  (try
    (loop [prev-replica-val base-replica
           replica-val @replica
           messenger (:messenger init-event)
           pipeline (:pipeline init-event)
           barriers (:barriers init-event)]
      ;; Safe point to update peer task state, checkpoints, etc, in-between task lifecycle runs
      ;; If replica-val has changed here, then we *may* need to recover state/rewind offset, etc.
      ;; Initially this should be implemented to rewind to the last checkpoint over all peers
      ;; that have totally been checkpointed (e.g. input tasks + windowed tasks)

      ;; TODO here
      ;; No need to emit new barrier, since this is handled by new-messenger-state, though should possibly be done here

      ;; New replica version
      ;; 1. Input: Rewind pipeline to past snapshot, flush barriers
      ;; 2. All: Rewind state to past snapshot
      ;; 2. Clear barriers

      ;; Acking
      ;; 1. Need to checkpoint offsets in receive-acks

      (let [new-messenger (ms/new-messenger-state! messenger init-event prev-replica-val replica-val)
            event (assoc init-event 
                         :barriers barriers
                         :messenger new-messenger
                         :pipeline pipeline)
            event (->> event
                       (gen-lifecycle-id)
                       (emit-barriers)
                       (receive-acks)
                       (lc/invoke-before-batch)
                       (lc/invoke-read-batch read-batch)
                       (apply-fn)
                       (build-new-segments)
                       (lc/invoke-assign-windows assign-windows)
                       (lc/invoke-write-batch write-batch)
                       ;(flow-retry-segments)
                       (lc/invoke-after-batch)
                       (ack-barriers))]
        (if (first (alts!! [kill-ch] :default true))
          (recur replica-val @replica (:messenger event) (:pipeline event) (:barriers event))
          event)))
   (catch Throwable e
     (ex-f e)
     init-event)))

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
           (if pipeline
             (op/start pipeline)
             (throw (ex-info "Failure to resolve plugin builder fn. Did you require the file that contains this symbol?" {:kw kw})))))
       ;; TODO, make this a unique type - extend-type is ugly
       (Object.))
      (catch Throwable e
        (throw e)))))

(defrecord TaskInformation 
    [id log job-id task-id 
     workflow catalog task flow-conditions windows filtered-windows triggers lifecycles task-map]
  component/Lifecycle
  (start [component]
    (let [catalog (extensions/read-chunk log :catalog job-id)
          task (extensions/read-chunk log :task task-id)
          flow-conditions (extensions/read-chunk log :flow-conditions job-id)
          windows (extensions/read-chunk log :windows job-id)
          filtered-windows (vec (wc/filter-windows windows (:name task)))
          window-ids (set (map :window/id filtered-windows))
          triggers (extensions/read-chunk log :triggers job-id)
          filtered-triggers (filterv #(window-ids (:trigger/window-id %)) triggers)
          workflow (extensions/read-chunk log :workflow job-id)
          lifecycles (extensions/read-chunk log :lifecycles job-id)
          metadata (or (extensions/read-chunk log :job-metadata job-id) {})
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

(defn new-task-information [peer task]
  (map->TaskInformation (select-keys (merge peer task) [:id :log :job-id :task-id])))

(defn add-pipeline [{:keys [task-map] :as event}]
  (assoc event 
         :pipeline 
         (build-pipeline task-map event)))

(defrecord TaskLifeCycle
    [id log messenger job-id task-id replica restart-ch log-prefix peer task
     kill-ch outbox-ch opts task-kill-ch scheduler-event task-monitoring task-information]
  component/Lifecycle

  (start [component]
    (try
      (let [{:keys [workflow catalog task flow-conditions windows filtered-windows
                    triggers filtered-triggers lifecycles task-map metadata]} task-information
            log-prefix (logger/log-prefix task-information)

            pipeline-data (map->Event 
                           {:id id
                            :job-id job-id
                            :task-id task-id
                            :task (:name task)
                            :catalog catalog
                            :workflow workflow
                            :flow-conditions flow-conditions
                            :lifecycles lifecycles
                            :metadata metadata
                            :barriers {}
                            :task-map task-map
                            :serialized-task task
                            :log log
                            :messenger messenger
                            :monitoring task-monitoring
                            :task-information task-information
                            :outbox-ch outbox-ch
                            :restart-ch restart-ch
                            :task-kill-ch task-kill-ch
                            :kill-ch kill-ch
                            :peer-opts opts
                            :fn (operation/resolve-task-fn task-map)
                            :replica replica
                            :log-prefix log-prefix})

            _ (info log-prefix "Warming up task lifecycle" task)

            pipeline-data (->> pipeline-data
                               c/task-params->event-map
                               c/flow-conditions->event-map
                               c/lifecycles->event-map
                               (c/windows->event-map filtered-windows filtered-triggers)
                               (c/triggers->event-map filtered-triggers)
                               c/task->event-map)

            _ (assert (empty? (.__extmap pipeline-data)) "Ext-map for Event record should be empty at start")

            ex-f (fn [e] (handle-exception task-information log e restart-ch outbox-ch job-id))
            _ (while (and (first (alts!! [kill-ch task-kill-ch] :default true))
                          (not (start-lifecycle? pipeline-data)))
                (Thread/sleep (arg-or-default :onyx.peer/peer-not-ready-back-off opts)))

            pipeline-data (->> pipeline-data
                               lc/invoke-before-task-start
                               add-pipeline
                               resolve-filter-state
                               resolve-log
                               replay-windows-from-log
                               (start-window-state-thread! ex-f))]

        (>!! outbox-ch (entry/create-log-entry :signal-ready {:id id}))

        (loop [replica-state @replica]
          (when (and (first (alts!! [kill-ch task-kill-ch] :default true))
                     (not (common/job-covered? replica-state job-id)))
            (info log-prefix "Not enough virtual peers have warmed up to start the task yet, backing off and trying again...")
            (Thread/sleep (arg-or-default :onyx.peer/job-not-ready-back-off opts))
            (recur @replica)))

        (info log-prefix "Enough peers are active, starting the task")

        (let [task-lifecycle-ch (thread (run-task-lifecycle pipeline-data kill-ch ex-f))]
          (s/validate os/Event pipeline-data)
          (assoc component
                 :pipeline-data pipeline-data
                 :log-prefix log-prefix
                 :task-information task-information
                 :task-kill-ch task-kill-ch
                 :kill-ch kill-ch
                 :task-lifecycle-ch task-lifecycle-ch)))
      (catch Throwable e
        (handle-exception task-information log e restart-ch outbox-ch job-id)
        component)))

  (stop [component]
    (if-let [task-name (:task (:pipeline-data component))]
      (info (:log-prefix component) "Stopping task lifecycle")
      (warn (:log-prefix component) "Stopping task lifecycle, failed to initialize task set up"))

    (when-let [event (:pipeline-data component)]

      (when-not (empty? (:triggers event))
        (>!! (:state-ch event) [(:scheduler-event component) event #()]))

      (stop-window-state-thread! event)

      ;; Ensure task operations are finished before closing peer connections
      (close! (:kill-ch component))

      (let [last-event (<!! (:task-lifecycle-ch component))]
        (when-let [pipeline (:pipeline last-event)]
          (op/stop pipeline last-event))

        (close! (:task-kill-ch component))

        (when-let [state-log (:state-log event)] 
          (state-extensions/close-log state-log event))

        (when-let [filter-state (:filter-state event)] 
          (when (exactly-once-task? event)
            (state-extensions/close-filter @filter-state event)))

        ((:compiled-after-task-fn event) event)))

    (assoc component
           :pipeline-data nil
           :task-lifecycle-ch nil)))

(defn task-lifecycle [peer task]
  (map->TaskLifeCycle (merge peer task)))

; (defn task-lifecycle []
;   (map->TaskLifeCycle {}))
