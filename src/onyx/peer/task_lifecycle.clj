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
            [onyx.protocol.task-state :refer :all]
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
            [onyx.types :refer [->Results ->MonitorEvent map->Event]]
            [onyx.peer.window-state :as ws]
            [onyx.peer.transform :refer [apply-fn]]
            [onyx.plugin.onyx-input :as oi]
            [onyx.plugin.onyx-output :as oo]
            [onyx.plugin.onyx-plugin :as op]
            [onyx.flow-conditions.fc-routing :as r]
            [onyx.static.logging :as logger]
            [onyx.state.state-extensions :as state-extensions]
            [onyx.static.default-vals :refer [defaults arg-or-default]]
            [onyx.messaging.aeron :as messaging]
            [onyx.messaging.common :as mc]))

(s/defn start-lifecycle? [event]
  (let [rets (lc/invoke-start-task event)]
    (when-not (:start-lifecycle? rets)
      (info (:log-prefix event) "Peer chose not to start the task yet. Backing off and retrying..."))
    rets))

(defrecord SegmentRetries [segments retries])

(defn add-from-leaf 
  [event result root leaves accum {:keys [message] :as leaf}]
  (let [routes (r/route-data event result message)
        message* (r/flow-conditions-transform message routes event)
        leaf* (if (= message message*)
                leaf
                (assoc leaf :message message*))]
    (if (= :retry (:action routes))
      (assoc accum :retries (conj! (:retries accum) root))
      (update accum :segments (fn [s] (conj! s (assoc leaf* :flow (:flow routes))))))))

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
  [state]
  (-> state
      (set-event! (let [{:keys [results monitoring] :as event} (get-event state)]
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
                         (assoc event :results (persistent-results! results))))))
      (advance)))

; (s/defn flow-retry-segments :- Event
;   [{:keys [task-state state messenger monitoring results] :as event} 
;   (doseq [root (:retries results)]
;     (when-let [site (peer-site task-state (:completion-id root))]
;       (emit-latency :peer-retry-segment
;                     monitoring
;                     #(extensions/internal-retry-segment messenger (:id root) site))))
;   event)

(s/defn start-processing
  [state]
  {:post [(empty? (:batch (:event %)))
          (empty? (:segments (:results (:event %))))]}
  (-> state 
      (set-event! (assoc (get-event state) :lifecycle-id (uuid/random-uuid)))
      (advance)))

;; TODO, good place to implement another protocol and use type dispatch
(def input-readers
  {:input #'function/read-input-batch
   :function #'function/read-function-batch
   :output #'function/read-function-batch})

(defn read-batch [state]
  (let [task-type (:task-type (get-event state))
        _ (assert task-type)
        _ (assert (:apply-fn (get-event state)))
        f (get input-readers task-type)]
    ;; FIXME ADD INVOKE AFTER READ BATCH
    (advance (f state))))

(defn prepare-batch [state] 
  ;; FIXME invoke-prepare-batch/write-batch
  (advance (oo/prepare-batch (get-pipeline state) state)))

(defn write-batch [state] 
  ;; FIXME invoke-write-batch
  (advance (oo/write-batch (get-pipeline state) state)))

(defn handle-exception [task-info log e group-ch outbox-ch id job-id]
  (let [data (ex-data e)
        ;; Default to original exception if Onyx didn't wrap the original exception
        inner (or (.getCause ^Throwable e) e)]
    (if (:onyx.core/lifecycle-restart? data)
      (do (warn (logger/merge-error-keys inner task-info "Caught exception inside task lifecycle. Rebooting the task."))
          (>!! group-ch [:restart-vpeer id]))
      (do (warn (logger/merge-error-keys e task-info "Handling uncaught exception thrown inside task lifecycle - killing this job."))
          (let [entry (entry/create-log-entry :kill-job {:job job-id})]
            (extensions/write-chunk log :exception inner job-id)
            (>!! outbox-ch entry))))))

(defn input-poll-barriers [state]
  (m/poll (get-messenger state))
  (advance state))

(defn record-pipeline-barrier [state]
  (let [messenger (get-messenger state)] 
    (advance 
     (if (m/all-barriers-seen? messenger)
       (let [pipeline (get-pipeline state)] 
         (add-barrier! state 
                       (m/epoch messenger)
                       {:checkpoint (oi/checkpoint pipeline)
                        :completed? (oi/completed? pipeline)}))
       state))))

(defn prepare-offer-barriers [state]
  (let [messenger (get-messenger state)] 
    (if (m/all-barriers-seen? messenger)
      (do 
       (m/next-epoch! messenger)
       (-> state 
           (set-context! {:barrier-opts {}
                          :publications (m/publications messenger)})
           (advance)))
      ;; skip over offer-barriers
      (advance (advance state)))))

(defn write-state-checkpoint! [state]
  (let [messenger (get-messenger state)
        event (get-event state)
        replica-version (m/replica-version messenger)
        epoch (m/epoch messenger)]
    (when (:windowed-task? event) 
      (let [{:keys [job-id task-id slot-id log]} event] 
        (extensions/write-checkpoint log job-id replica-version epoch task-id slot-id 
                                   :state 
                                   (mapv ws/export-state (get-windows-state state)))))))

(defn do-offer-barriers [state]
  (let [event (get-event state)
        messenger (get-messenger state)
        context (get-context state)] 
    (loop [pubs (:publications context)]
      (if-not (empty? pubs)
        (let [pub (first pubs)
              ret (m/offer-barrier messenger pub (:barrier-opts context))]
          (if (pos? ret)
            (recur (rest pubs))
            (set-context! state (assoc context :publications pubs))))
        (do
         (println "Unblocking for " (m/replica-version messenger) (m/epoch messenger))
         (m/unblock-subscriptions! messenger)
         (-> state 
             (set-context! nil)
             (advance)))))))

(defn offer-barriers [state]
  (let [new-state (do-offer-barriers state)]
    (when (advanced? new-state)
      (write-state-checkpoint! new-state))
    new-state))

(defn prepare-ack-barriers [state]
  (let [messenger (get-messenger state)] 
    (if (m/all-barriers-seen? messenger)
      (let [publications (m/publications messenger)]
        (advance (set-context! state {:publications publications})))
      ;; skip over ack-barriers
      (advance (advance state)))))

(defn barriers-acked! [state]
  (-> state
      (get-messenger)
      (m/next-epoch!)
      (m/unblock-subscriptions!))
  (set-context! state nil))

(defn ack-barriers [state]
  (let [messenger (get-messenger state)
        context (get-context state)] 
    (loop [pubs (:publications context)]
      (if-not (empty? pubs)
        (let [pub (first pubs)
              ret (m/offer-barrier-ack messenger pub)]
          (if (pos? ret)
            (recur (rest pubs))
            (set-context! state (assoc context :publications pubs))))
        (advance (barriers-acked! state))))))

(defn complete-job! [state]
  (let [event (get-event state)
        messenger (get-messenger state)
        {:keys [job-id task-id slot-id outbox-ch]} event
        entry (entry/create-log-entry :exhaust-input 
                                      {:replica-version (m/replica-version messenger)
                                       :job-id job-id 
                                       :task-id task-id
                                       :slot-id slot-id})]
    (println "Job completed" job-id task-id (:args entry))
    (info "job completed:" job-id task-id (:args entry))
    (>!! outbox-ch entry)))

(defn backoff-when-drained! [event]
  (Thread/sleep (arg-or-default :onyx.peer/drained-back-off (:peer-opts event))))

(s/defn assign-windows :- os/Event
  [state]
  (ws/assign-windows state :new-segment))

(defn poll-acks [state]
  (advance 
   (let [messenger (get-messenger state)] 
     (if-let [ack-result (-> messenger
                             (m/poll-acks)
                             (m/all-acks-seen?))]
       (let [{:keys [replica-version epoch]} ack-result
             barrier (get-barrier state epoch)]
         (println "Got ack result" (into {} ack-result))
         (assert (= replica-version (m/replica-version messenger)))
         (let [{:keys [job-id task-id slot-id log]} (get-event state)
               completed? (:completed? barrier)] 
           (extensions/write-checkpoint log job-id replica-version epoch task-id slot-id :input (:checkpoint barrier))
           (println "Removing barrier for epoch" epoch barrier)
           (when completed?
             (if (not (exhausted? state))
               (complete-job! state)
               (backoff-when-drained! (get-event state))))
           (m/unblock-ack-subscriptions! messenger)
           (-> state
               (set-exhausted! completed?)
               (remove-barrier! epoch))))
       state))))

(defn before-batch [state]
  (advance (set-event! state (lc/invoke-before-batch (get-event state)))))

(defn after-batch [state]
  (advance (set-event! state (lc/invoke-after-batch (get-event state)))))

(defn recover-stored-checkpoint
  [{:keys [log job-id task-id slot-id] :as event} checkpoint-type recover]
  ;(println "Read checkpoints" (extensions/read-checkpoints log job-id))
  (let [checkpointed (-> (extensions/read-checkpoints log job-id)
                         (get recover)
                         (get [task-id slot-id checkpoint-type]))]
    (if-not (= :beginning checkpointed)
      checkpointed)))

(defn recover-pipeline-input [state]
  (let [recover (:recover (get-context state))
        pipeline (get-pipeline state)
        event (get-event state)
        stored (recover-stored-checkpoint event :input recover)
        _ (info "Recovering checkpoint" (:job-id event) (:task-id event) stored)
        next-pipeline (oi/recover pipeline stored)]
    (-> state
        (set-pipeline! next-pipeline)
        (add-barrier! (m/epoch (get-messenger state)) 
                      {:checkpoint (oi/checkpoint next-pipeline)
                       :completed? (oi/completed? next-pipeline)})
        (advance))))

(defn recover-windows-state
  [state]
  (let [{:keys [recover] :as context} (get-context state)
        {:keys [log-prefix task-map windows triggers] :as event} (get-event state)
        stored (recover-stored-checkpoint event :state recover)
        recovered-windows (->> windows
                               (mapv (fn [window] (wc/resolve-window-state window triggers task-map)))
                               (mapv (fn [stored ws]
                                       (if stored
                                         (let [recovered (ws/recover-state ws stored)] 
                                           (info "Recovered state" stored (:id event))
                                           recovered) 
                                         ws))
                                     (or stored (repeat nil))))]
    ;; Log playback
    ;(update :windows-state
    ;         (fn [windows-state] 
    ;           (mapv (fn [ws entries]
    ;                   (-> ws 
    ;                       ;; Restore the accumulated log as a hack
    ;                       ;; To allow us to dump the full state each time
    ;                       (assoc :event-results (mapv ws/log-entry->state-event entries))
    ;                       (ws/play-entry entries)))
    ;                 windows-state
    ;                 (or stored (repeat [])))))
    (-> state 
        (set-windows-state! recovered-windows)
        (ws/assign-windows :recovered)
        (advance))))

(defn poll-recover-input-function [state]
  (let [messenger (get-messenger state)] 
    (if-let [recover (m/poll-recover messenger)]
      (do
       (m/next-epoch! messenger)
       (-> state
           (set-context! {:recover recover
                          :offer-barriers? true
                          :barrier-opts {:recover recover}
                          :publications (m/publications (get-messenger state))})
           (advance)))
      state)))

(defn poll-recover-output [state]
  (let [messenger (get-messenger state)] 
    (if-let [recover (m/poll-recover messenger)]
      (do
       (m/next-epoch! messenger)
       ;; don't need to ack the recover barrier
       (m/unblock-subscriptions! messenger)
       (advance state))
      state)))

(defn iteration [state-machine replica]
  (if-not (= (get-replica state-machine) replica)
    (next-replica! state-machine replica)
    (loop [sm state-machine]
      (print-state sm)
      (let [next-sm (exec sm)]
        (if (or (not (advanced? sm)) (initial-state? sm))
          next-sm
          (recur next-sm))))))

(defn run-task-lifecycle
  "The main task run loop, read batch, ack messages, etc."
  [state-machine ex-f]
  (try
    (let [{:keys [task-kill-ch kill-ch task-information replica-atom opts state]} (get-event state-machine)] 
      (loop [sm state-machine 
             replica-val @replica-atom]
        ;; TODO add here :offer-barriers, emit-ack-barriers?
        ;(println "Iteration " (:state prev-state))
        (info "Task Dropping back in " (:task-type (get-event sm)))
        (let [next-sm (iteration sm replica-val)]
          (if-not (killed? next-sm)
            (recur next-sm @replica-atom)
            next-sm))))
   (catch Throwable e
     (ex-f e)
     state-machine)))

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
       (function/->FunctionPlugin))
      (catch Throwable e
        (throw e)))))

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

(defn epoch-gaps? [barriers]
  (and (not (empty? barriers))
       (let [epochs (keys barriers)
             mn (apply min epochs)
             mx (apply max epochs)]
         (not= (set epochs) (set (range mn (inc mx)))))))

(def all #{:input :output :function})

(defn filter-task-lifecycles [task-type windowed?]
  (cond-> []
    (#{:input :function} task-type)         (conj {:lifecycle :poll-recover-input-function
                                                   :fn poll-recover-input-function
                                                   :blockable? true})
    (#{:output} task-type)                  (conj {:lifecycle :poll-recover-output
                                                   :fn poll-recover-output
                                                   :blockable? true})
    (#{:input :function} task-type)         (conj {:lifecycle :offer-barriers
                                                   :fn offer-barriers
                                                   :blockable? true})
    (#{:input} task-type)                   (conj {:lifecycle :recover-pipeline-input 
                                                   :fn recover-pipeline-input})
    windowed?                                (conj {:lifecycle :recover-pipeline-input 
                                                   :fn recover-windows-state})
    (all task-type)                         (conj {:lifecycle :start-processing
                                                   :fn start-processing})
    (#{:input} task-type)                   (conj {:lifecycle :input-poll-barriers
                                                   :fn input-poll-barriers})
    (#{:input} task-type)                   (conj {:lifecycle :recover-pipeline-barrier
                                                   :fn record-pipeline-barrier})
    (#{:input :function} task-type)         (conj {:lifecycle :prepare-offer-barriers
                                                   :fn prepare-offer-barriers})
    ;; prepare-offer-barriers must come before offer-barriers
    (#{:input :function} task-type)         (conj {:lifecycle :offer-barriers
                                                   :fn offer-barriers
                                                   :blockable? true})
    (#{:input} task-type)                   (conj {:lifecycle :poll-acks
                                                   :fn poll-acks})
    (#{:input :function :output} task-type) (conj {:lifecycle :before-batch
                                                   :fn before-batch})
    (#{:input :function :output} task-type) (conj {:lifecycle :read-batch
                                                   :fn read-batch})
    (#{:input :function :output} task-type) (conj {:lifecycle :apply-fn
                                                   :fn apply-fn})
    (#{:input :function :output} task-type) (conj {:lifecycle :build-new-segments
                                                   :fn build-new-segments})
    windowed?                               (conj {:lifecycle :assign-windows
                                                   :fn assign-windows})
    (#{:input :function :output} task-type) (conj {:lifecycle :prepare-batch
                                                   :fn prepare-batch})
    (#{:input :function :output} task-type) (conj {:lifecycle :write-batch
                                                   :fn write-batch
                                                   :blockable? true})
    (#{:input :function :output} task-type) (conj {:lifecycle :after-batch
                                                   :fn after-batch})
    (#{:output} task-type)                  (conj {:lifecycle :prepare-ack-barriers
                                                   :fn prepare-ack-barriers})
    ;; prepare-ack-barriers must come before ack-barriers
    (#{:output} task-type)                  (conj {:lifecycle :ack-barriers
                                                   :fn ack-barriers
                                                   :blockable? true})))

(deftype TaskStateMachine [^int processing-idx 
                           ^int nstates 
                           lifecycle-names
                           #^"[Lclojure.lang.IFn;" lifecycle-fns 
                           ^:unsynchronized-mutable ^int idx 
                           ^:unsynchronized-mutable ^java.lang.Boolean advanced 
                           ^:unsynchronized-mutable exhausted
                           ^:unsynchronized-mutable replica 
                           ^:unsynchronized-mutable messenger 
                           ^:unsynchronized-mutable coordinator
                           ^:unsynchronized-mutable pipeline
                           ^:unsynchronized-mutable init-event 
                           ^:unsynchronized-mutable event
                           ^:unsynchronized-mutable barriers
                           ^:unsynchronized-mutable windows-state
                           ^:unsynchronized-mutable context]
  PTaskStateMachine
  (stop [this]
    (when coordinator (coordinator/stop coordinator))
    (when messenger (component/stop messenger))
    (when pipeline (op/stop pipeline event))
    this)
  (killed? [this]
    (first (alts!! [(:task-kill-ch event) (:kill-ch event)] :default true)))
  (initial-state? [this]
    (= idx processing-idx))
  (advanced? [this]
    advanced)
  (print-state [this]
    (let [task-map (:task-map event)] 
      (println "Task state" 
               [(:onyx/type task-map)
                (:onyx/name task-map)
                :slot
                (:slot-id event)
                :id
                (:id event)
                (get lifecycle-names idx)
                :adv? advanced
                :rv
                (m/replica-version messenger)
                :e
                (m/epoch messenger)
                :n-subs
                (count (m/subscriptions messenger))
                :n-pubs
                (count (m/publications messenger))
                ;:port
                ;(:port (:messenger-group messenger))
                :batch
                (:batch event)
                :segments-gen
                (:segments (:results event))
                (if (= :input (:onyx/type task-map))
                  [:barriers-first-5 (vec (take 5 (sort-by key barriers)))])]))
    this)
  (set-context! [this new-context]
    (set! context new-context)
    this)
  (get-context [this]
    context)
  (set-exhausted! [this new-exhausted]
    (set! exhausted new-exhausted)
    this)
  (exhausted? [this]
    exhausted)
  (set-pipeline! [this new-pipeline]
    (set! pipeline new-pipeline)
    this)
  (get-pipeline [this]
    pipeline)
  (next-replica! [this new-replica]
    (let [job-id (get event :job-id)
          old-version (get-in replica [:allocation-version job-id])
          new-version (get-in new-replica [:allocation-version job-id])]
      (if (= old-version new-version)
        this
        (let [next-messenger (ms/next-messenger-state! messenger event replica new-replica)
              ;; Coordinator must be transitioned before recovery, as the coordinator
              ;; emits the barrier with the recovery information in 
              next-coordinator (coordinator/next-state coordinator replica new-replica)]
          (set! barriers {})
          (-> this
              (set-exhausted! false)
              (set-coordinator! next-coordinator)
              (set-messenger! next-messenger)
              (set-replica! new-replica)
              (start-recover!))))))
  (set-windows-state! [this new-windows-state]
    (set! windows-state new-windows-state)
    this)
  (get-windows-state [this]
    windows-state)
  (add-barrier! [this epoch barrier]
    (set! barriers (assoc barriers epoch barrier))
    this)
  (get-barrier [this epoch]
    (assert (not (epoch-gaps? barriers)))
    (assert (contains? barriers epoch) barriers)
    (get barriers epoch))
  (remove-barrier! [this epoch]
    (set! barriers (dissoc barriers epoch))
    this)
  (set-replica! [this new-replica]
    (set! replica new-replica)
    this)
  (get-replica [this]
    replica)
  (set-event! [this new-event]
    (set! event new-event)
    this)
  (get-event [this] event)
  (set-messenger! [this new-messenger]
    (set! messenger new-messenger)
    this)
  (get-messenger [this]
    messenger)
  (set-coordinator! [this new-coordinator]
    (set! coordinator new-coordinator)
    this)
  (start-recover! [this]
    (set! idx (int 0))
    this)
  (next-cycle! [this]
    ;; wrap around to start-processing
    (set-event! this init-event)
    (set! idx processing-idx)
    this)
  (get-coordinator [this]
    coordinator)
  (exec [this]
    (set! advanced false)
    (let [task-fn (aget lifecycle-fns idx)]
      (task-fn this)))
  (advance [this]
    (let [new-idx ^int (unchecked-add-int idx 1)]
      (set! advanced true)
      (if (= new-idx nstates)
        (next-cycle! this)
        (set! idx new-idx))
      this)))

(defn new-task-state-machine [{:keys [task-map windows triggers] :as event} replica messenger coordinator pipeline event]
  (let [windowed? (or (not (empty? windows))
                      (not (empty? triggers)))
        lifecycles (filter-task-lifecycles (:onyx/type task-map) windowed?)
        names (mapv :lifecycle lifecycles)
        arr (into-array clojure.lang.IFn (map :fn lifecycles))
        processing-idx (->> lifecycles
                            (map-indexed (fn [idx v]
                                           (if (= :start-processing (:lifecycle v))
                                             idx)))
                            (remove nil?)
                            (first))]
    (->TaskStateMachine (int processing-idx) (alength arr) names arr (int 0) false false replica messenger coordinator 
                        pipeline event event nil (c/event->windows-states event) nil)))

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

(defn start-task-lifecycle! [state ex-f]
  (thread (run-task-lifecycle state ex-f)))

(defn final-state [component]
  (<!! (:task-lifecycle-ch component)))

(defrecord TaskLifeCycle
  [id log messenger job-id task-id replica group-ch log-prefix kill-ch outbox-ch seal-ch 
   completion-ch peer-group opts task-kill-ch scheduler-event task-monitoring task-information]
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
            coordinator (coordinator/new-peer-coordinator log (:messenger-group component) opts id job-id group-ch)
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
                             :task-map task-map
                             :serialized-task task
                             :log log
                             :monitoring task-monitoring
                             :task-information task-information
                             :outbox-ch outbox-ch
                             :group-ch group-ch
                             :task-kill-ch task-kill-ch
                             :kill-ch kill-ch
                             ;; Rename to peer-config
                             :peer-opts opts
                             :fn (operation/resolve-task-fn task-map)
                             :replica-atom replica
                             :log-prefix log-prefix})

            _ (info log-prefix "Warming up task lifecycle" task)

            pipeline-data (->> pipeline-data
                               c/task-params->event-map
                               c/flow-conditions->event-map
                               c/lifecycles->event-map
                               c/task->event-map)

            _ (assert (empty? (.__extmap pipeline-data)) 
                      (str "Ext-map for Event record should be empty at start. Contains: " (keys (.__extmap pipeline-data))))

            _ (backoff-until-task-start! pipeline-data)

            ex-f (fn [e] (handle-exception task-information log e group-ch outbox-ch id job-id))
            event (lc/invoke-before-task-start pipeline-data)
            initial-state (new-task-state-machine 
                           event
                           (onyx.log.replica/starting-replica opts)
                           messenger
                           coordinator
                           (build-pipeline task-map event)
                           event)]
       ;; TODO: we may need some kind of a signal ready to assure that 
       ;; subscribers do not blow past messages in aeron
       ;(>!! outbox-ch (entry/create-log-entry :signal-ready {:id id}))
       (info log-prefix "Enough peers are active, starting the task")
       (let [task-lifecycle-ch (start-task-lifecycle! initial-state ex-f)]
         (s/validate os/Event event)
         (assoc component
                :event event
                :state initial-state
                :log-prefix log-prefix
                :task-information task-information
                ;; atom for storing peer test state in property test
                :holder (atom nil)
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

      (when-let [last-state (final-state component)]
        (stop last-state)
        (when-not (empty? (:triggers (get-event last-state)))
          (ws/assign-windows last-state (:scheduler-event component)))

        (close! (:task-kill-ch component))

        ((:compiled-after-task-fn event) event)))

    (assoc component
           :event nil
           :state nil
           :holder nil
           :log-prefix nil
           :task-information nil
           :task-kill-ch nil
           :kill-ch nil
           :task-lifecycle-ch nil)))

(defn task-lifecycle [peer task]
  (map->TaskLifeCycle (merge peer task)))
