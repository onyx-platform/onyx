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

; (defn resolve-log [{:keys [peer-opts] :as pipeline}]
;   (let [log-impl (arg-or-default :onyx.peer/state-log-impl peer-opts)] 
;     (assoc pipeline :state-log (if (windowed-task? pipeline) 
;                                  (state-extensions/initialize-log log-impl pipeline)))))

; (defn resolve-filter-state [{:keys [peer-opts] :as pipeline}]
;   (let [filter-impl (arg-or-default :onyx.peer/state-filter-impl peer-opts)] 
;     (assoc pipeline 
;            :filter-state 
;            (if (windowed-task? pipeline)
;              (if (exactly-once-task? pipeline) 
;                (atom (state-extensions/initialize-filter filter-impl pipeline)))))))

; (defn start-window-state-thread!
;   [ex-f {:keys [windows] :as event}]
;   (if (empty? windows) 
;     event
;     (let [state-ch (chan 1)
;           event (assoc event :state-ch state-ch)
;           process-state-thread-ch (thread (ws/process-state-loop event ex-f))] 
;       (assoc event :state-thread-ch process-state-thread-ch))))

; (defn stop-window-state-thread!
;   [{:keys [windows state-ch state-thread-ch] :as event}]
;   (when-not (empty? windows)
;     (close! state-ch)
;     ;; Drain state-ch to unblock any blocking puts
;     (while (poll! state-ch))
;     (<!! state-thread-ch)))

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
  (emit-latency 
   :peer-batch-latency 
   monitoring
   #(let [results (reduce (fn [accumulated result]
                            (let [root (:root result)
                                  segments (:segments accumulated)
                                  retries (:retries accumulated)
                                  ret (add-from-leaves segments retries event result)]
                              (->Results (:tree results) (:segments ret) (:retries ret))))
                          results
                          (:tree results))]
      (assoc event :results (persistent-results! results)))))

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

;; TODO, good place to implement another protocol and use type dispatch
(def input-readers
  {:input #'function/read-input-batch
   :function #'function/read-function-batch
   :output #'function/read-function-batch})

(defn read-batch
<<<<<<< HEAD
  [{:keys [task-type pipeline] :as event}]
  (let [f (get input-readers task-type)
        rets (merge event (f event))]
    (lc/invoke-after-read-batch rets)))

; (defn replay-windows-from-log
;   [{:keys [log-prefix windows-state
;            filter-state state-log] :as event}]
;   (when (windowed-task? event)
;     (swap! windows-state 
;            (fn [windows-state] 
;              (let [exactly-once? (exactly-once-task? event)
;                    apply-fn (fn [ws [unique-id window-logs]]
;                               (if exactly-once? 
;                                 (swap! filter-state state-extensions/apply-filter-id event unique-id))
;                               (mapv ws/play-entry ws window-logs))
;                    replayed-state (state-extensions/playback-log-entries state-log event windows-state apply-fn)]
;                (trace log-prefix (format "Replayed state: %s" replayed-state))
;                replayed-state))))
;   event)

(s/defn write-batch :- os/Event 
  [event :- os/Event]
  (let [rets (merge event (oo/write-batch (:pipeline event) event))]
    (trace (:log-prefix event) (format "Wrote %s segments" (count (:segments (:results rets)))))
=======
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
>>>>>>> develop
    rets))

(defn handle-exception [task-info log e group-ch outbox-ch id job-id]
  (let [data (ex-data e)]
    (if (:onyx.core/lifecycle-restart? data)
      (do (warn (logger/merge-error-keys (:original-exception data) task-info "Caught exception inside task lifecycle. Rebooting the task."))
          (>!! group-ch [:restart-vpeer id]))
      (do (warn (logger/merge-error-keys e task-info "Handling uncaught exception thrown inside task lifecycle - killing this job."))
          (let [entry (entry/create-log-entry :kill-job {:job job-id})]
            (extensions/write-chunk log :exception e job-id)
            (>!! outbox-ch entry))))))

(defn emit-barriers [{:keys [task-type messenger id pipeline barriers] :as event}]
  (cond (= :input task-type) 
        (let [new-messenger (m/emit-barrier messenger)
              barrier-info {:checkpoint (oi/checkpoint pipeline)
                            :completed? (oi/completed? pipeline)}] 
          (-> event
              (assoc :messenger new-messenger)
              (assoc-in [:barriers (m/replica-version new-messenger) (m/epoch new-messenger)] barrier-info)))

        (and (= :function task-type) 
             (m/all-barriers-seen? messenger))
        (assoc event :messenger (m/emit-barrier messenger))

        :else
        event))

(defn ack-barriers [{:keys [task-type messenger] :as event}]
  (if (and (= :output task-type) 
           (m/all-barriers-seen? messenger))
    (assoc event :messenger (m/ack-barrier messenger))
    event))

(s/defn complete-job [{:keys [job-id task-id] :as event} :- os/Event]
  (let [entry (entry/create-log-entry :exhaust-input {:job job-id :task task-id})]
    (>!! (:outbox-ch event) entry)))

(defn backoff-when-drained! [event]
  (Thread/sleep (arg-or-default :onyx.peer/drained-back-off (:peer-opts event))))

(defn checkpoint-path
  [{:keys [task-id job-id slot-id] :as event} replica-version epoch]
  [job-id [replica-version epoch] [task-id slot-id]])

(defn job-input-tasks [replica job-id]
  (set (get-in replica [:input-tasks job-id])))

(defn required-checkpoints [replica job-id]
  (let [input-tasks (job-input-tasks replica job-id)] 
    (->> (get-in replica [:task-slot-ids job-id])
         (filter (fn [[task-id _]] (get input-tasks task-id)))
         (mapcat (fn [[task-id peer->slot]]
                   (map (fn [slot-id]
                          [task-id slot-id])
                        (vals peer->slot))))
         set)))

(defn retrieve-job-checkpoints [checkpoints job-id]
  (info "checkpoints: " (get @checkpoints job-id))
  (get @checkpoints job-id))

(defn max-completed-checkpoints [{:keys [job-id checkpoints] :as event} replica]
  (let [required (required-checkpoints replica job-id)] 
    ;(println "required" required)
    (->> (retrieve-job-checkpoints checkpoints job-id)
         (filter (fn [[k v]]
                   (= required (set (keys v)))))
         (sort-by key)
         last)))

(defn store-input-checkpoint! 
  [event replica-version epoch checkpoint]
  (swap! (:checkpoints event) 
         assoc-in
         (checkpoint-path event replica-version epoch)
         checkpoint))

(defn recover-slot-checkpoint
  [{:keys [job-id task-id slot-id] :as event} prev-replica next-replica]
  (assert (= slot-id (get-in next-replica [:task-slot-ids job-id task-id (:id event)])))
  (if (some #{job-id} (:jobs prev-replica))
    (do 
     (when (not= (required-checkpoints prev-replica job-id)
                 (required-checkpoints next-replica job-id))
       (throw (ex-info "Slots for input tasks must currently be stable to allow checkpoint resume" {})))
     (let [[[rv e] checkpoints] (max-completed-checkpoints event next-replica)]
       (get checkpoints [task-id slot-id])))))

(defn receive-acks [{:keys [task-type] :as event}]
  (if (= :input task-type) 
    (let [{:keys [messenger barriers]} event 
          new-messenger (m/receive-acks messenger)]
      (if-let [{:keys [replica-version epoch]} (m/all-acks-seen? new-messenger)]
        (let [barrier (get-in barriers [replica-version epoch])]
          (store-input-checkpoint! event replica-version epoch (:checkpoint barrier))
          (when (:completed? barrier)
            (complete-job event)
            (backoff-when-drained! event))
          (assoc event 
                 :barriers (assoc-in barriers [replica-version epoch] (assoc barrier :acked? true))
                 :messenger (m/flush-acks new-messenger)))
        (assoc event :messenger new-messenger)))
    event))

(defn start-event [event prev-replica replica messenger pipeline barriers]
  (if (= prev-replica replica) 
    (-> event
        (assoc :replica replica)
        (assoc :messenger messenger)
        (assoc :barriers barriers)
        (assoc :pipeline pipeline))
    (-> event
        (assoc :replica replica)
        (assoc :messenger (ms/new-messenger-state! messenger event prev-replica replica))
        (assoc :barriers {})
        (assoc :pipeline (if (= :input (:task-type event)) 
                           (let [checkpoint (recover-slot-checkpoint event prev-replica replica)]
                             (info "Recovering checkpoint " checkpoint)
                             (oi/recover pipeline checkpoint))
                           pipeline)))))

(defn event-iteration 
  [init-event prev-replica-val replica-val messenger pipeline barriers]
  ;(println "Iteration " (:version prev-replica-val) (:version replica-val))
  (->> (start-event init-event prev-replica-val replica-val messenger pipeline barriers)
       (gen-lifecycle-id)
       (emit-barriers)
       (receive-acks)
       (lc/invoke-before-batch)
       (lc/invoke-read-batch read-batch)
       (apply-fn)
       (build-new-segments)
       ;(lc/invoke-assign-windows assign-windows)
       (lc/invoke-write-batch write-batch)
       ;(flow-retry-segments)
       (lc/invoke-after-batch)
       (ack-barriers)))

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
      (let [event (event-iteration init-event prev-replica-val replica-val messenger pipeline barriers)]
        ; (assert (empty? (.__extmap event)) 
        ;         (str "Ext-map for Event record should be empty at start. Contains: " (keys (.__extmap event))))
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

(defn add-pipeline [{:keys [task-map] :as event}]
  (assoc event 
         :pipeline 
         (build-pipeline task-map event)))

(defrecord TaskInformation 
  [log job-id task-id workflow catalog task flow-conditions windows triggers lifecycles metadata]
  component/Lifecycle
  (start [component]
    (let [catalog (extensions/read-chunk log :catalog job-id)
          task (extensions/read-chunk log :task job-id task-id)
          flow-conditions (extensions/read-chunk log :flow-conditions job-id)
          windows (extensions/read-chunk log :windows job-id)
          triggers (extensions/read-chunk log :triggers job-id)
          workflow (extensions/read-chunk log :workflow job-id)
          lifecycles (extensions/read-chunk log :lifecycles job-id)
          metadata (extensions/read-chunk log :job-metadata job-id)]
      (assoc component 
             :workflow workflow :catalog catalog :task task :flow-conditions flow-conditions
             :windows windows :triggers triggers :lifecycles lifecycles :metadata metadata)))
  (stop [component]
    (assoc component 
           :catalog nil :task nil :flow-conditions nil :windows nil 
           :triggers nil :lifecycles nil :metadata nil)))

(defn new-task-information [peer task]
  (map->TaskInformation (select-keys (merge peer task) [:log :job-id :task-id])))

(defn backoff-until-task-start! [{:keys [kill-ch task-kill-ch opts] :as event}]
  (while (and (first (alts!! [kill-ch task-kill-ch] :default true))
              (not (start-lifecycle? event)))
    (Thread/sleep (arg-or-default :onyx.peer/peer-not-ready-back-off opts))))

(defn backoff-until-covered! [{:keys [id replica job-id kill-ch task-kill-ch opts outbox-ch log-prefix] :as event}]
  (loop [replica-state @replica]
    (when (and (first (alts!! [kill-ch task-kill-ch] :default true))
               (not (common/job-covered? replica-state job-id)))
      (info log-prefix "Not enough virtual peers have warmed up to start the task yet, backing off and trying again...")
      (Thread/sleep (arg-or-default :onyx.peer/job-not-ready-back-off opts))
      (recur @replica))))

(defn start-task-lifecycle! [{:keys [kill-ch] :as event} ex-f]
  (thread (run-task-lifecycle event kill-ch ex-f)))

(defrecord TaskLifeCycle
  [id log messenger-buffer messenger job-id task-id replica peer-replica-view group-ch log-prefix
   kill-ch outbox-ch seal-ch completion-ch opts task-kill-ch scheduler-event task-monitoring task-information]
  component/Lifecycle

  (start [component]
    (try
     (let [{:keys [workflow catalog task flow-conditions windows triggers lifecycles metadata]} task-information
           log-prefix (logger/log-prefix task-information id)
           task-map (find-task catalog (:name task))
           pipeline-data (map->Event 
                          {:id id
                           :job-id job-id
                           :task-id task-id
                           :slot-id (get-in replica [:task-slot-ids job-id task-id id])
                           :task (:name task)
                           :catalog catalog
                           :workflow workflow
                           :flow-conditions flow-conditions
                           :lifecycles lifecycles
                           :metadata (or metadata {})
                           :barriers {}
                           :task-map task-map
                           :serialized-task task
                           :log log
                           :messenger messenger
                           :monitoring task-monitoring
                           :task-information task-information
                           :outbox-ch outbox-ch
                           :group-ch group-ch
                           :task-kill-ch task-kill-ch
                           :kill-ch kill-ch
                           :peer-opts opts
                           :fn (operation/resolve-task-fn task-map)
                           :replica replica
                           :log-prefix log-prefix})

           _ (info log-prefix "Warming up task lifecycle" task)

           filtered-windows (vec (wc/filter-windows windows (:name task)))
           window-ids (set (map :window/id filtered-windows))
           filtered-triggers (filterv #(window-ids (:trigger/window-id %)) triggers)

           pipeline-data (->> pipeline-data
                              c/task-params->event-map
                              c/flow-conditions->event-map
                              c/lifecycles->event-map
                              (c/windows->event-map filtered-windows filtered-triggers)
                              (c/triggers->event-map filtered-triggers)
                              c/task->event-map)

           _ (assert (empty? (.__extmap pipeline-data)) (str "Ext-map for Event record should be empty at start. Contains: " (keys (.__extmap pipeline-data))))

           _ (backoff-until-task-start! pipeline-data)

           ex-f (fn [e] (handle-exception task-information log e group-ch outbox-ch id job-id))
           pipeline-data (->> pipeline-data
                              lc/invoke-before-task-start
                              add-pipeline
                              ;resolve-filter-state
                              ;resolve-log
                              ;replay-windows-from-log
                              ;(start-window-state-thread! ex-f)
                              )]

       (>!! outbox-ch (entry/create-log-entry :signal-ready {:id id}))
       (info log-prefix "Enough peers are active, starting the task")
       (let [task-lifecycle-ch (start-task-lifecycle! pipeline-data ex-f)]
         ;; FIXME TURN BACK ON
         ;(s/validate os/Event pipeline-data)
         (assoc component
                :event pipeline-data
                :log-prefix log-prefix
                :task-information task-information
                :task-kill-ch task-kill-ch
                :kill-ch kill-ch
                :task-lifecycle-ch task-lifecycle-ch)))
     (catch Throwable e
       (handle-exception task-information log e group-ch outbox-ch id job-id)
       component)))

  (stop [component]
    (if-let [task-name (:name (:task (:task-information component)))]
      (info (:log-prefix component) "Stopping task lifecycle")
      (warn (:log-prefix component) "Stopping task lifecycle, failed to initialize task set up"))
    (info "event from component is " (:event component))

    (when-let [event (:event component)]

      ; (when-not (empty? (:triggers event))
      ;   (>!! (:state-ch event) [(:scheduler-event component) event #()]))

      ; (stop-window-state-thread! event)

      ;; Ensure task operations are finished before closing peer connections
      (close! (:kill-ch component))

      (let [last-event (<!! (:task-lifecycle-ch component))]
        (when-let [pipeline (:pipeline last-event)]
          (op/stop pipeline last-event))

        (close! (:task-kill-ch component))

        ; (when-let [state-log (:state-log event)] 
        ;   (state-extensions/close-log state-log event))

        ; (when-let [filter-state (:filter-state event)] 
        ;   (when (exactly-once-task? event)
        ;     (state-extensions/close-filter @filter-state event)))

        ((:compiled-after-task-fn event) event)))

    (assoc component
           :event nil
           :task-lifecycle-ch nil)))

(defn task-lifecycle [peer task]
  (map->TaskLifeCycle (merge peer task)))
