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
            [onyx.peer.coordinator :as coordinator]
            [onyx.peer.task-compile :as c]
            [onyx.windowing.window-compile :as wc]
            [onyx.lifecycles.lifecycle-invoke :as lc]
            [onyx.peer.function :as function]
            [onyx.peer.operation :as operation]
            [onyx.compression.nippy :refer [messaging-decompress]]
            [onyx.messaging.messenger :as m]
            [onyx.messaging.messenger-state :as ms]
            [onyx.log.replica]
            [onyx.extensions :as extensions]
            [onyx.types :refer [->Results ->MonitorEvent map->Event dec-count! inc-count!]]
            [onyx.peer.event-state :as event-state]
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

(defn resolve-log [{:keys [peer-opts] :as pipeline}]
  (let [log-impl (arg-or-default :onyx.peer/state-log-impl peer-opts)] 
    (assoc pipeline :state-log (if (:windowed-task? pipeline) 
                                 (state-extensions/initialize-log log-impl pipeline)))))

; (defn resolve-filter-state [{:keys [peer-opts] :as pipeline}]
;   (let [filter-impl (arg-or-default :onyx.peer/state-filter-impl peer-opts)] 
;     (assoc pipeline 
;            :filter-state 
;            (if (and (:windowed-task? pipeline)
;                     (:uniquesness-task? pipeline))
;              (atom (state-extensions/initialize-filter filter-impl pipeline))))))

(s/defn start-lifecycle? [event]
  (let [rets (lc/invoke-start-task event)]
    (when-not (:start-lifecycle? rets)
      (info (:log-prefix event) "Peer chose not to start the task yet. Backing off and retrying..."))
    rets))

(defrecord SegmentRetries [segments retries])

(defn add-from-leaf 
  [{:keys [egress-tasks task->group-by-fn] :as event} 
   result root leaves accum {:keys [message] :as leaf}]
  (let [routes (r/route-data event result message)
        message* (r/flow-conditions-transform message routes event)
        hash-group (g/hash-groups message* egress-tasks task->group-by-fn)
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
  [{:keys [results monitoring] :as event}]
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
  [{:keys [task-type pipeline] :as event}]
  (let [f (get input-readers task-type)
        rets (merge event (f event))]
    (lc/invoke-after-read-batch rets)))



(s/defn write-batch :- os/Event 
  [event :- os/Event]
  (let [rets (merge event (oo/write-batch (:pipeline event) event))]
    (trace (:log-prefix event) (format "Wrote %s segments" (count (:segments (:results rets)))))
    rets))

(defn handle-exception [task-info log e group-ch outbox-ch id job-id]
  (let [data (ex-data e)
        ;; Default to original exception if Onyx didn't wrap the original exception
        inner (or (.getCause ^Throwable e) e)]
    (if (:onyx.core/lifecycle-restart? data)
      (do (warn (logger/merge-error-keys inner task-info "Caught exception inside task lifecycle. Rebooting the task."))
          (>!! group-ch [:restart-vpeer id]))
      (do (warn (logger/merge-error-keys e task-info "Handling uncaught exception thrown inside task lifecycle - killing this job."))
          (let [entry (entry/create-log-entry :kill-job {:job job-id})]
            ;(println "writing chunk " e inner entry)
            (extensions/write-chunk log :exception inner job-id)
            (>!! outbox-ch entry))))))

(defn emit-barriers [{:keys [task-type messenger id pipeline barriers] :as event}]
  ;; TODO, checkpoint state here - do it for all types
  ;; But only if all barriers are seen, or if we're an input and we're going to emit
  
  ;; For input tasks we may need to do it for both input checkpoint as well as state
  ;; Though for input I think we just need to take a copy which we will put in the barrier 
  ;; state ready to be acked? That does seem quite ugly. Maybe we write the state and the 
  ;; checkpoint value here, but then modify the ack state when it actually gets acked.
  (cond (= :input task-type) 
        ;; this will normally only happen on a timer, not every loop iteration
        (let [new-messenger (m/emit-barrier messenger)
              barrier-info {:checkpoint (oi/checkpoint pipeline)
                            :completed? (oi/completed? pipeline)}] 

          ;;; Add me later
          ;(extensions/write-checkpoint log job-id replica-version epoch task-id slot-id :input (:checkpoint barrier))

          (-> event
              (assoc :messenger new-messenger)
              (assoc-in [:barriers (m/replica-version new-messenger) (m/epoch new-messenger)] barrier-info)))

        (and (= :function task-type) 
             (m/all-barriers-seen? messenger))
        (let [new-messenger (m/emit-barrier messenger)
              replica-version (m/replica-version new-messenger)
              epoch (m/epoch new-messenger)
              {:keys [job-id task-id slot-id log windows-state]} event]
          ;(println "Writing state checkpoint " replica-version epoch (mapv ws/state windows-state))
          (extensions/write-checkpoint log job-id replica-version epoch 
                                       task-id slot-id :state (mapv ws/state windows-state))
          (assoc event :messenger new-messenger))

        :else
        event))

(defn ack-barriers [{:keys [task-type messenger] :as event}]
  (if (and (= :output task-type) 
           (m/all-barriers-seen? messenger))
    ;; Add me later
    ;;(extensions/write-checkpoint log job-id replica-version epoch task-id slot-id :state {:3 4})
    (assoc event :messenger (m/ack-barrier messenger))
    event))

(s/defn complete-job [{:keys [job-id task-id] :as event} :- os/Event]
  (let [entry (entry/create-log-entry :exhaust-input {:job job-id :task task-id})]
    (>!! (:outbox-ch event) entry)))

(defn backoff-when-drained! [event]
  (Thread/sleep (arg-or-default :onyx.peer/drained-back-off (:peer-opts event))))

(s/defn assign-windows :- os/Event
  [{:keys [windows] :as event}]
  (if-not (empty? windows)
    ;; shouldn't update atom, should update window-state value
    (ws/assign-windows event :new-segment)
    event))

;; Taken from clojure core incubator
(defn dissoc-in
  "Dissociates an entry from a nested associative structure returning a new
  nested structure. keys is a sequence of keys. Any empty maps that result
  will not be present in the new structure."
  [m [k & ks :as keys]]
  (if ks
    (if-let [nextmap (get m k)]
      (let [newmap (dissoc-in nextmap ks)]
        (if (seq newmap)
          (assoc m k newmap)
          (dissoc m k)))
      m)
    (dissoc m k)))

(defn receive-acks [{:keys [task-type] :as event}]
  (if (= :input task-type) 
    (let [{:keys [messenger barriers]} event 
          new-messenger (m/receive-acks messenger)
          ack-result (m/all-acks-seen? new-messenger)]
      (info "Ack result " ack-result)
      (if ack-result
        (let [{:keys [replica-version epoch]} ack-result]
          (if-let [barrier (get-in barriers [(:replica-version ack-result) (:epoch ack-result)])] 
            (do
             ;(println "Acking result, barrier:" (into {} barrier) replica-version epoch)
             ;(println barriers)
             (let [{:keys [job-id task-id slot-id log]} event] 
               (info "writing checkpoint " (:checkpoint barrier))
               ;; Is this the right place to checkpoint inputs?
               ;; Possibly should move write-checkpoint to 
               ;; Write checkpoint further segmenting by type?
               (extensions/write-checkpoint log job-id replica-version 
                                            epoch task-id slot-id :input (:checkpoint barrier))
               (when (:completed? barrier)
                 (complete-job event)
                 (backoff-when-drained! event))
               (assoc event 
                      :barriers (dissoc-in barriers [replica-version epoch])
                      :messenger (m/flush-acks new-messenger))))
            (assoc event :messenger new-messenger)))
        (assoc event :messenger new-messenger)))
    event))

(defn print-stage [stage event]
  ;(println  "Stage " stage)
  event)

(defn event-iteration 
  [init-event prev-replica-val replica-val messenger pipeline barriers windows-states]
  ;(println "Iteration " (:version prev-replica-val) (:version replica-val))
  (let [start-event (event-state/next-state init-event prev-replica-val replica-val 
                                            messenger pipeline barriers windows-states)] 
    (->> start-event 
         (print-stage 1)
         (gen-lifecycle-id)
         (print-stage 2)
         (emit-barriers)
         (print-stage 3)
         (receive-acks)
         (print-stage 4)
         (lc/invoke-before-batch)
         (print-stage 5)
         (lc/invoke-read-batch read-batch)
         (print-stage 6)
         (apply-fn)
         (print-stage 7)
         (build-new-segments)
         (print-stage 8)
         (assign-windows)
         (lc/invoke-write-batch write-batch)
         (print-stage 9)
         ;(flow-retry-segments)
         (lc/invoke-after-batch)
         (print-stage 10)
         (ack-barriers))))

(defn run-task-lifecycle
  "The main task run loop, read batch, ack messages, etc."
  [{:keys [messenger task-information replica replica-atom opts] :as init-event}
   kill-ch ex-f]
  (try
    (loop [prev-replica-val replica
           replica-val @replica-atom
           messenger (:messenger init-event)
           pipeline (:pipeline init-event)
           barriers (:barriers init-event)
           windows-state (:windows-state init-event)]
      (let [event (event-iteration init-event prev-replica-val replica-val messenger pipeline barriers windows-state)]
        ; (assert (empty? (.__extmap event)) 
        ;         (str "Ext-map for Event record should be empty at start. Contains: " (keys (.__extmap event))))
        (if (first (alts!! [kill-ch] :default true))
          (recur replica-val @replica-atom 
                 (:messenger event) (:pipeline event) 
                 (:barriers event) (:windows-state event))
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
  (map->TaskInformation (select-keys (merge peer task) [:log :job-id :task-id :id])))

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

(defn final-event [component]
  (<!! (:task-lifecycle-ch component)))

(defrecord TaskLifeCycle
  [id log messenger job-id task-id replica group-ch log-prefix
   kill-ch outbox-ch seal-ch completion-ch peer-group opts task-kill-ch scheduler-event task-monitoring task-information]
  component/Lifecycle

  (start [component]
    (assert (zero? (count (m/publications messenger))))
    (assert (zero? (count (m/subscriptions messenger))))
    (assert (zero? (count (m/ack-subscriptions messenger))))
    (try
     (let [{:keys [workflow catalog task flow-conditions windows triggers lifecycles metadata]} task-information
           log-prefix (logger/log-prefix task-information)
           task-map (find-task catalog (:name task))
           filtered-windows (vec (wc/filter-windows windows (:name task)))
           window-ids (set (map :window/id filtered-windows))
           filtered-triggers (filterv #(window-ids (:trigger/window-id %)) triggers)
           coordinator (coordinator/new-peer-coordinator (:messaging-group component) opts id job-id)
           pipeline-data (map->Event 
                          {:id id
                           :job-id job-id
                           :task-id task-id
                           :slot-id (get-in @replica [:task-slot-ids job-id task-id id])
                           :task (:name task)
                           :catalog catalog
                           :workflow workflow
                           :windows filtered-windows
                           :triggers filtered-triggers
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
                           :group-ch group-ch
                           :task-kill-ch task-kill-ch
                           :kill-ch kill-ch
                           :peer-opts opts
                           :coordinator coordinator 
                           :fn (operation/resolve-task-fn task-map)
                           :replica (onyx.log.replica/starting-replica opts)
                           :replica-atom replica
                           :log-prefix log-prefix})

           _ (info log-prefix "Warming up task lifecycle" task)

           pipeline-data (->> pipeline-data
                              c/task-params->event-map
                              c/flow-conditions->event-map
                              c/lifecycles->event-map
                              c/windows->event-map
                              ;c/triggers->event-map
                              c/task->event-map)

           _ (assert (empty? (.__extmap pipeline-data)) (str "Ext-map for Event record should be empty at start. Contains: " (keys (.__extmap pipeline-data))))

           _ (backoff-until-task-start! pipeline-data)

           ex-f (fn [e] (handle-exception task-information log e group-ch outbox-ch id job-id))
           pipeline-data (->> pipeline-data
                              lc/invoke-before-task-start
                              add-pipeline
                              ;resolve-filter-state
                              resolve-log)]
       ;; TODO: we may need some kind of a signal ready to assure that subscribers do not blow past
       ;; messages in aeron
       ;(>!! outbox-ch (entry/create-log-entry :signal-ready {:id id}))
       (info log-prefix "Enough peers are active, starting the task")
       (let [task-lifecycle-ch (start-task-lifecycle! pipeline-data ex-f)]
         (s/validate os/Event pipeline-data)
         (assoc component
                :event pipeline-data
                :log-prefix log-prefix
                :task-information task-information
                :task-kill-ch task-kill-ch
                :kill-ch kill-ch
                :task-lifecycle-ch task-lifecycle-ch)))
     (catch Throwable e
       ;; FIXME in main branch, we weren't wrapping up exception in here
       (handle-exception task-information log e group-ch outbox-ch id job-id)
       component)))

  (stop [component]
    (if-let [task-name (:name (:task (:task-information component)))]
      (info (:log-prefix component) "Stopping task lifecycle")
      (warn (:log-prefix component) "Stopping task lifecycle, failed to initialize task set up"))

    (when-let [event (:event component)]
      ;; Ensure task operations are finished before closing peer connections
      (close! (:kill-ch component))

      (let [last-event (final-event component)]
        (when-not (empty? (:triggers event))
          (ws/assign-windows last-event (:scheduler-event component)))

        (some-> last-event :coordinator coordinator/stop)
        (some-> last-event :pipeline (op/stop last-event))

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
