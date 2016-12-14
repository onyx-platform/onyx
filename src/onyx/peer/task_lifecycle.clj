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
            [onyx.messaging.protocols.messenger :as m]
            [onyx.messaging.protocols.publisher :as pub]
            [onyx.messaging.protocols.subscriber :as sub]
            [onyx.messaging.protocols.status-publisher :as status-pub]
            [onyx.peer.visualization :as viz]
            [onyx.messaging.messenger-state :as ms]
            [onyx.log.replica]
            [onyx.extensions :as extensions]
            [onyx.types :refer [->Results ->MonitorEvent map->Event]]
            [onyx.peer.window-state :as ws]
            [onyx.peer.transform :refer [apply-fn]]
            [onyx.plugin.protocols.input :as oi]
            [onyx.plugin.protocols.output :as oo]
            [onyx.plugin.protocols.plugin :as op]
            [onyx.flow-conditions.fc-routing :as r]
            [onyx.static.logging :as logger]
            [onyx.state.state-extensions :as state-extensions]
            [onyx.static.default-vals :refer [defaults arg-or-default]]
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

;; TODO REMOVE
(s/defn add-from-leaves
  "Flattens root/leaves and accumulates new segments and retries"
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

(s/defn next-iteration
  [state]
  {:post [(empty? (:batch (:event %)))
          (empty? (:segments (:results (:event %))))]}
  (-> state 
      (set-context! nil)
      (init-event!)
      (set-event! (assoc (get-event state) :lifecycle-id (uuid/random-uuid)))
      (advance)))

(defn prepare-batch [state] 
  (advance (oo/prepare-batch (get-pipeline state) state)))

(defn write-batch [state] 
  (oo/write-batch (get-pipeline state) state))

(defn handle-exception-action [state lifecycle t]
  (if state
    (let [event (get-event state)
          handler-fn (:compiled-handle-exception-fn event)]
      (handler-fn event lifecycle t))
    :restart))

(defn handle-exception [task-info log e lifecycle exception-action group-ch outbox-ch id job-id]
  (let [data (ex-data e)
        ;; Default to original exception if Onyx didn't wrap the original exception
        inner (or (.getCause ^Throwable e) e)]
    (if (= exception-action :restart)
      (let [msg (format "Caught exception inside task lifecycle %s. Rebooting the task." lifecycle)]         (warn (logger/merge-error-keys inner task-info id msg)) 
        (>!! group-ch [:restart-vpeer id]))
      (let [msg (format "Handling uncaught exception thrown inside task lifecycle %s. Killing the job." lifecycle)
            entry (entry/create-log-entry :kill-job {:job job-id})]
        (warn (logger/merge-error-keys e task-info id msg))
        (extensions/write-chunk log :exception inner job-id)
        (>!! outbox-ch entry)))))

(defn input-poll-barriers [state]
  (m/poll (get-messenger state))
  (advance state))

(defn poll-heartbeats [state]
  (run! pub/poll-heartbeats! (m/publishers (get-messenger state)))
  (advance state))

;; In the future this shouldn't be neccesary
(defn strip-coordinator [src-peer-id]
  (if (vector? src-peer-id)
    (second src-peer-id)
    src-peer-id))

(defn evict-dead-peers! [state timed-out-peer-ids]
  (let [replica (get-replica state)
        {:keys [id outbox-ch]} (get-event state)]
    (info "Should be killing " (vec timed-out-peer-ids))
    (run! (fn [peer-id] 
            (let [entry {:fn :leave-cluster
                         :peer-parent id
                         :args {:id peer-id
                                :group-id (get-in replica [:groups-reverse-index peer-id])}}]
              (info "Peer timed out with no heartbeats. Emitting leave cluster." entry)
              (>!! outbox-ch entry)))
          timed-out-peer-ids)
    ;; FIXME, needs to jump into a mode here where we wait it out for a new replica
    (Thread/sleep 500)
    (info "Replica is" replica)))

(defn dead-peer-detection [state]
  (let [messenger (get-messenger state)]
    (run! pub/poll-heartbeats! (m/publishers (get-messenger state)))
    (when-let [timed-out (seq (sub/timed-out-publishers (m/subscriber messenger)))] 
      (println "UPSTREAM TIMED OUT" timed-out "detected from" 
               (:id (get-event state))
               (:task (get-event state)))
      (evict-dead-peers! state (map strip-coordinator timed-out)))
    (advance state)))

(defn offer-heartbeats [state]
  (if (zero? (rand-int 10))
    (let [messenger (get-messenger state)]
      (run! pub/offer-heartbeat! (m/publishers messenger)) ;; rename publishers to publications?
      (sub/offer-heartbeat! (m/subscriber messenger))
      (advance state)) 
    (advance state)))

(defn checkpoint-input [state]
  (let [messenger (get-messenger state)
        {:keys [job-id task-id slot-id log]} (get-event state)
        replica-version (m/replica-version messenger)
        epoch (m/epoch messenger)
        pipeline (get-pipeline state)
        checkpoint (oi/checkpoint pipeline)] 
    (extensions/write-checkpoint log job-id replica-version epoch task-id slot-id :input checkpoint)
    (println "Checkpointed input" job-id replica-version epoch task-id slot-id :input)
    (advance state)))

(defn checkpoint-state [state]
  (let [messenger (get-messenger state)
        {:keys [job-id task-id slot-id log]} (get-event state)
        replica-version (m/replica-version messenger)
        epoch (m/epoch messenger)] 
    (extensions/write-checkpoint log job-id replica-version epoch task-id slot-id 
                                 :state 
                                 (mapv ws/export-state (get-windows-state state)))
    (println "Checkpointed state" job-id replica-version epoch task-id slot-id :state)
    (advance state)))

(defn checkpoint-output [state]
  (let [messenger (get-messenger state)
        {:keys [job-id task-id slot-id log]} (get-event state)
        replica-version (m/replica-version messenger)
        epoch (m/epoch messenger)] 
    (extensions/write-checkpoint log job-id replica-version epoch task-id slot-id :output true)
    (println "Checkpointed output" job-id replica-version epoch task-id slot-id :output)
    (advance state)))

(defn prepare-barrier-sync [state]
  (let [messenger (get-messenger state)] 
    (if (m/barriers-aligned? messenger)
      (let [_ (m/next-epoch! messenger)
            e (m/epoch messenger)
            input? (= :input (:task-type (get-event state)))
            pipeline (cond-> (get-pipeline state)
                       input? (oi/next-epoch e))
            completed? (if input? 
                         (oi/completed? pipeline)
                         (m/all-barriers-completed? messenger))]
        (-> state 
            (set-pipeline! pipeline)
            (set-context! {:barrier-opts {:completed? completed?}
                           :src-peers (sub/src-peers (m/subscriber messenger))
                           :publishers (m/publishers messenger)})
            (advance)))
      (goto-next-batch! state))))

(defn offer-barriers [state]
  ;; FIXME, also poll on subscribers? Probably can't really because
  ;; Then might also hit regular messages?
  (run! pub/poll-heartbeats! (m/publishers (get-messenger state)))
  (let [messenger (get-messenger state)
        context (get-context state)] 
    ;; TODO, offer all barriers in one go, only keep the ones that weren't able to offer
    ;; do filter. etc
    (loop [pubs (:publishers context)]
      (if-not (empty? pubs)
        (let [pub (first pubs)
              ret (m/offer-barrier messenger pub (:barrier-opts context))]
          (if (pos? ret)
            (recur (rest pubs))
            (set-context! state (assoc context :publishers pubs))))
        (advance state)))))

;; Gonna have to move this into the subscriber logic
(defn offer-barrier-status [state]
  (let [messenger (get-messenger state)
        ;; FIXME
        _ (run! pub/poll-heartbeats! (m/publishers (get-messenger state)))
        context (get-context state)] 
    ;; TODO, NEXT
    ;; Move session-id capturing into the status-pubs themselves
    ;; Move ready reply response into the handler
    ;; Move to capturing the status pubs
    ;; Move to single heartbeat back which includes the epoch
    (loop [src-peers (:src-peers context)]
      (if-not (empty? src-peers)
        (let [src-peer-id (first src-peers)
              ret (sub/offer-barrier-status! (m/subscriber messenger) src-peer-id)]
          (if (pos? ret)
            (recur (rest src-peers))
            (set-context! state (assoc context :src-peers src-peers))))
        (advance state)))))

(defn unblock-subscribers [state]
  ;; TODO, should put next epoch in here?
  (m/unblock-subscriber! (get-messenger state))
  (advance (set-context! state nil)))

;; After offer barriers, have notify publishers?
;; Then we can unblock subscribers
;; When we notify publishers, we notify publisher with each position of the subscriber
;; Only want to look at notifications with same replica version, track epoch

(defn complete-job! [state]
  (let [event (get-event state)
        messenger (get-messenger state)
        {:keys [job-id task-id slot-id outbox-ch]} event
        entry (entry/create-log-entry :seal-output 
                                      {:replica-version (m/replica-version messenger)
                                       :job-id job-id 
                                       :task-id task-id
                                       :slot-id slot-id})]
    (info "job completed:" job-id task-id (:args entry))
    (>!! outbox-ch entry)
    (set-sealed! state true)))

(defn seal-barriers [state]
  (let [messenger (get-messenger state)] 
    (if (m/barriers-aligned? messenger)
      (do
       (when (and (m/all-barriers-completed? messenger) 
                  (not (sealed? state)))
         (complete-job! state))
       (m/next-epoch! messenger)
       (-> state 
           (set-context! {:src-peers (sub/src-peers (m/subscriber messenger))})
           (advance)))
      (goto-next-batch! state))))

;; Re-enable to prevent CPU burn?
; (defn backoff-when-drained! [event]
;   (Thread/sleep (arg-or-default :onyx.peer/drained-back-off (:peer-opts event))))

(defn assign-windows [state]
  (ws/assign-windows state :new-segment))

(defn build-lifecycle-invoke-fn [event lifecycle-key]
  (let [f (get event lifecycle-key)]
    (fn [state]
      (advance (set-event! state (f (get-event state)))))))

(defn read-checkpoint
  [{:keys [log job-id task-id slot-id] :as event} checkpoint-type recover]
  (println "RECOVER IS " recover)
  (if-not (= :beginning recover)
    (extensions/read-checkpoint log job-id (first recover) (second recover) task-id slot-id checkpoint-type)))

(defn recover-input [state]
  (let [{:keys [recover] :as context} (get-context state)
        _ (assert recover)
        event (get-event state)
        stored (read-checkpoint event :input recover)
        messenger (get-messenger state)
        replica-version (m/replica-version messenger)
        epoch (m/epoch messenger)
        _ (info "RECOVER pipeline checkpoint" (:job-id event) (:task-id event) stored)
        next-pipeline (-> state
                          (get-pipeline)
                          (oi/recover replica-version stored)
                          (oi/next-epoch epoch))]
    (-> state
        (set-pipeline! next-pipeline)
        (advance))))

(defn recover-state
  [state]
  (let [{:keys [log-prefix task-map windows triggers] :as event} (get-event state)
        {:keys [recover] :as context} (get-context state)
        _ (assert recover)
        stored (read-checkpoint event :state recover)
        recovered-windows (->> windows
                               (mapv (fn [window] (wc/resolve-window-state window triggers task-map)))
                               (mapv (fn [stored ws]
                                       (if stored
                                         (let [recovered (ws/recover-state ws stored)] 
                                           (info "RECOVER state" (:id event) stored)
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
        ;; Notify triggers that we have recovered our windows
        ;; ws/assign-windows will advance
        (ws/assign-windows :recovered))))

(defn poll-recover-input-function [state]
  (let [messenger (get-messenger state)] 
    (if-let [recover (m/poll-recover messenger)]
      (do
       (m/next-epoch! messenger)
       (-> state
           (set-context! {:recover recover
                          :barrier-opts {:recover recover 
                                         :completed? false}
                          :src-peers (sub/src-peers (m/subscriber messenger))
                          :publishers (m/publishers messenger)})
           (advance)))
      state)))

(defn poll-recover-output [state]
  (let [messenger (get-messenger state)] 
    (if-let [recover (m/poll-recover messenger)]
      (do
       (m/next-epoch! messenger)
       (-> state
           (set-context! {:src-peers (sub/src-peers (m/subscriber messenger))})
           (advance)))
      state)))

(def DEBUG false)

(defn iteration [state-machine replica]
  (when DEBUG (viz/update-monitoring! state-machine))
  (loop [sm (if-not (= (get-replica state-machine) replica)
              (next-replica! state-machine replica)
              state-machine)]
    (let [next-sm (exec sm)]
      (when DEBUG (print-state sm))
      ;(println "Next state for loop. advanced:" (advanced? sm) "new iter" (new-iteration? sm))
      (if (and (advanced? sm) 
               (not (new-iteration? sm)))
        (recur next-sm)
        ;; Blocked for whatever reason
        (do
         ;; sleep to prevent cpu burn
         ;; TODO: improve methods here
         (Thread/sleep 5)
         (info "Polling heartbeats because blocked, keep things going")
         (run! pub/poll-heartbeats! (m/publishers (get-messenger sm)))
         next-sm)))))

(defn run-task-lifecycle
  "The main task run loop, read batch, ack messages, etc."
  [state-machine ex-f]
  (try
    (let [{:keys [replica-atom]} (get-event state-machine)] 
      (loop [sm state-machine 
             replica-val @replica-atom]
        (info "New task iteration:" (:task-type (get-event sm)))
        (let [next-sm (iteration sm replica-val)]
          (if-not (killed? next-sm)
            (recur next-sm @replica-atom)
            (do
             (println "Fell out of task lifecycle loop")
             next-sm)))))
   (catch Throwable e
     (ex-f e state-machine)
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
             (throw (ex-info "Failure to resolve plugin builder fn.
                              Did you require the file that contains this symbol?" 
                             {:kw kw})))))
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

;; Publisher heartbeats
;; At the start of each cycle, set all publications to used? false
;; Whenever you send a message / barrier / whatever to a publication, set used? true
;; At the end of the cycle, send heartbeats to used? false publications
;; Make all of this done by the messenger

;; Subscriber heartbeats
;; At the end of each cycle, check whether subscriber is still alive
;; If it is not, do not send a message and consider re-establishing via log message / peer reboot
;; If it is, then send a heartbeat in the backchannel to the publisher

;; TODO, try to filter out lifecycles that are actually identity
(defn filter-task-lifecycles [{:keys [task-map windows triggers] :as event}]
  (let [task-type (:onyx/type task-map)
        windowed? (or (not (empty? windows))
                      (not (empty? triggers)))] 
    (cond-> []

      ;; compile / initialise

      ;; backoff until task start

      ;; invoke before task start

      ;; build pipeline

      ;; start coordinator

      (#{:input} task-type)                   (conj {:lifecycle :poll-recover-input
                                                     :fn poll-recover-input-function
                                                     :blockable? true})
      (#{:function} task-type)                (conj {:lifecycle :poll-recover-function
                                                     :fn poll-recover-input-function
                                                     :blockable? true})
      (#{:output} task-type)                  (conj {:lifecycle :poll-recover-output
                                                     :fn poll-recover-output
                                                     :blockable? true})
      (#{:input :function} task-type)         (conj {:lifecycle :offer-barriers
                                                     :fn offer-barriers
                                                     :blockable? true})
      (#{:input :function :output} task-type) (conj {:lifecycle :offer-barrier-status
                                                     :fn offer-barrier-status
                                                     :blockable? true})
      (#{:input} task-type)                   (conj {:lifecycle :recover-input 
                                                     :fn recover-input})
      windowed?                               (conj {:lifecycle :recover-state 
                                                     :fn recover-state})
      (#{:input :function :output} task-type) (conj {:lifecycle :unblock-subscribers
                                                     :fn unblock-subscribers})
      (#{:input :function :output} task-type) (conj {:lifecycle :next-iteration
                                                     :fn next-iteration})
      (#{:input :function :output} task-type) (conj {:lifecycle :poll-heartbeats
                                                     :fn poll-heartbeats})
      (#{:input} task-type)                   (conj {:lifecycle :input-poll-barriers
                                                     :fn input-poll-barriers})
      (#{:input :function} task-type)         (conj {:lifecycle :prepare-barrier-sync
                                                     :fn prepare-barrier-sync})
      (#{:output} task-type)                  (conj {:lifecycle :seal-barriers
                                                     :fn seal-barriers
                                                     :blockable? false})
      ;; TODO: double check that checkpoint doesn't occur immediately after recovery
      (#{:input} task-type)                   (conj {:lifecycle :checkpoint-input
                                                     :fn checkpoint-input
                                                     :blockable? true})
      windowed?                               (conj {:lifecycle :checkpoint-state
                                                     :fn checkpoint-state
                                                     :blockable? true})
      ;; OUTPUT IS TOO AHEAD?
      (#{:output} task-type)                  (conj {:lifecycle :checkpoint-output
                                                     :fn checkpoint-output
                                                     :blockable? true})
      (#{:input :function} task-type)         (conj {:lifecycle :offer-barriers
                                                     :fn offer-barriers
                                                     :blockable? true})
      ;; rename aligneds -> aligments
      (#{:input :function :output} task-type) (conj {:lifecycle :offer-barrier-status
                                                     :fn offer-barrier-status
                                                     :blockable? true})
      (#{:input :function :output} task-type) (conj {:lifecycle :unblock-subscribers
                                                     :fn unblock-subscribers})
      (#{:input :function :output} task-type) (conj {:lifecycle :before-batch
                                                     :fn (build-lifecycle-invoke-fn event :compiled-before-batch-fn)})
      (#{:input} task-type)                   (conj {:lifecycle :read-batch
                                                     :fn function/read-input-batch})
      (#{:function :output} task-type)        (conj {:lifecycle :read-batch
                                                     :fn function/read-function-batch})
      (#{:input :function :output} task-type) (conj {:lifecycle :dead-peer-detection
                                                     :fn dead-peer-detection})
      (#{:input :function :output} task-type) (conj {:lifecycle :after-read-batch
                                                     :fn (build-lifecycle-invoke-fn event :compiled-after-read-batch-fn)})
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
                                                     :fn (build-lifecycle-invoke-fn event :compiled-after-batch-fn)})
      (#{:input :function :output} task-type) (conj {:lifecycle :offer-heartbeats
                                                     :fn offer-heartbeats}))))

(deftype TaskStateMachine [^int recover-idx 
                           ^int iteration-idx 
                           ^int batch-idx
                           ^int nstates 
                           lifecycle-names
                           #^"[Lclojure.lang.IFn;" lifecycle-fns 
                           ^:unsynchronized-mutable ^int idx 
                           ^:unsynchronized-mutable ^java.lang.Boolean advanced 
                           ^:unsynchronized-mutable sealed
                           ^:unsynchronized-mutable replica 
                           ^:unsynchronized-mutable messenger 
                           messenger-group
                           ^:unsynchronized-mutable coordinator
                           ^:unsynchronized-mutable pipeline
                           ^:unsynchronized-mutable init-event 
                           ^:unsynchronized-mutable event
                           ^:unsynchronized-mutable windows-state
                           ^:unsynchronized-mutable context]
  PTaskStateMachine
  (start [this] this)
  (stop [this]
    (when coordinator (coordinator/stop coordinator))
    (when messenger (component/stop messenger))
    (when pipeline (op/stop pipeline event))
    this)
  (killed? [this]
    (or @(:task-kill-flag event) @(:kill-flag event)))
  (new-iteration? [this]
    (= idx iteration-idx))
  (advanced? [this]
    advanced)
  (get-lifecycle [this]
    (get lifecycle-names idx))
  (print-state [this]
    (let [task-map (:task-map event)] 
      (info "Task state" 
               [(:onyx/type task-map)
                (:onyx/name task-map)
                :slot
                (:slot-id event)
                :id
                (:id event)
                (get-lifecycle this)
                :adv? advanced
                :rv
                (m/replica-version messenger)
                :e
                (m/epoch messenger)
                ; Replace with expected from subscriber itself
                ;:n-subs
                ;(count (m/subscriber messenger))
                :n-pubs
                (count (m/publishers messenger))
                :batch
                (:batch event)
                :segments-gen
                (:segments (:results event))]))
    this)
  (set-context! [this new-context]
    (set! context new-context)
    this)
  (get-context [this]
    context)
  (set-sealed! [this new-sealed]
    (set! sealed new-sealed)
    this)
  (sealed? [this]
    sealed)
  (set-pipeline! [this new-pipeline]
    (set! pipeline new-pipeline)
    this)
  (get-pipeline [this]
    pipeline)
  (next-replica! [this new-replica]
    (let [job-id (get event :job-id)
          old-version (get-in replica [:allocation-version job-id])
          new-version (get-in new-replica [:allocation-version job-id])]
      (if (or (= old-version new-version)
              (killed? this))
        this
        (let [next-messenger (ms/next-messenger-state! messenger event replica new-replica)
              ;; Coordinator must be transitioned before recovery, as the coordinator
              ;; emits the barrier with the recovery information in 
              next-coordinator (coordinator/next-state coordinator replica new-replica)]
          (-> this
              (set-sealed! false)
              (set-coordinator! next-coordinator)
              (set-messenger! next-messenger)
              (set-replica! new-replica)
              (goto-recover!))))))
  (set-windows-state! [this new-windows-state]
    (set! windows-state new-windows-state)
    this)
  (get-windows-state [this]
    windows-state)
  (set-replica! [this new-replica]
    (set! replica new-replica)
    this)
  (get-replica [this]
    replica)
  (init-event! [this]
    (set! event init-event)
    this)
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
  (goto-recover! [this]
    (set! idx recover-idx)
    (-> this 
        (set-context! nil)
        (init-event!)))
  (goto-next-iteration! [this]
    (set! idx iteration-idx))
  (goto-next-batch! [this]
    (set! advanced true)
    (set! idx batch-idx)
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
        (goto-next-iteration! this)
        (set! idx new-idx))
      this)))

(defn lookup-lifecycle-idx [lifecycles name]
  (->> lifecycles
       (map-indexed (fn [idx v]
                      (if (= name (:lifecycle v))
                        idx)))
       (remove nil?)
       (first)))

(defn new-state-machine [event opts messenger messenger-group coordinator pipeline]
  (let [{:keys [task-map windows triggers]} event
        base-replica (onyx.log.replica/starting-replica opts)
        lifecycles (filter-task-lifecycles event)
        names (mapv :lifecycle lifecycles)
        arr #^"[Lclojure.lang.IFn;" (into-array clojure.lang.IFn (map :fn lifecycles))
        recover-idx (int 0)
        iteration-idx (int (lookup-lifecycle-idx lifecycles :next-iteration))
        batch-idx (int (lookup-lifecycle-idx lifecycles :before-batch))]
    (->TaskStateMachine recover-idx iteration-idx batch-idx (alength arr) names arr 
                        (int 0) false false base-replica messenger messenger-group coordinator 
                        pipeline event event (c/event->windows-states event) nil)))

(defn backoff-until-task-start! [{:keys [kill-flag task-kill-flag opts] :as event}]
  (while (and (not (or @kill-flag @task-kill-flag))
              (not (start-lifecycle? event)))
    (Thread/sleep (arg-or-default :onyx.peer/peer-not-ready-back-off opts))))

(defn backoff-until-covered! [{:keys [id replica job-id kill-flag task-kill-flag opts outbox-ch log-prefix] :as event}]
  (loop [replica-state @replica]
    (when (not (or @kill-flag @task-kill-flag))
      (info log-prefix "Not enough virtual peers have warmed up to start the task yet, backing off and trying again...")
      (Thread/sleep (arg-or-default :onyx.peer/job-not-ready-back-off opts))
      (recur @replica))))

(defn start-task-lifecycle! [state ex-f]
  (thread (run-task-lifecycle state ex-f)))

(defn final-state [component]
  (<!! (:task-lifecycle-ch component)))

(defn compile-task [{:keys [task-information job-id task-id id monitoring log
                            replica-origin replica opts outbox-ch group-ch task-kill-flag kill-flag]}]
  (let [{:keys [workflow catalog task flow-conditions 
                windows triggers lifecycles metadata]} task-information
        ;; KILL JOB IF ANYTHING FROM HERE TO BELOW LINE FAILS
        log-prefix (logger/log-prefix task-information)
        task-map (find-task catalog (:name task))
        filtered-windows (vec (wc/filter-windows windows (:name task)))
        window-ids (set (map :window/id filtered-windows))
        filtered-triggers (filterv #(window-ids (:trigger/window-id %)) triggers)
        _ (info log-prefix "Compiling lifecycle")]
    (->> {:id id
          :job-id job-id
          :task-id task-id
          :slot-id (get-in replica-origin [:task-slot-ids job-id task-id id])
          :messenger-slot-id (common/messenger-slot-id replica-origin job-id task-id id)
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
          :monitoring monitoring
          :task-information task-information
          :outbox-ch outbox-ch
          :group-ch group-ch
          :task-kill-flag task-kill-flag
          :kill-flag kill-flag
          :peer-opts opts
          :fn (operation/resolve-task-fn task-map)
          :replica-atom replica
          :log-prefix log-prefix}
         map->Event 
         c/task-params->event-map
         c/flow-conditions->event-map
         c/lifecycles->event-map
         c/task->event-map)))

(defrecord TaskLifeCycle
  [id log messenger messenger-group job-id task-id replica group-ch log-prefix
   kill-flag outbox-ch completion-ch peer-group opts task-kill-flag
   scheduler-event task-monitoring task-information replica-origin]
  component/Lifecycle

  (start [component]
    (assert (zero? (count (m/publishers messenger))))
    (assert (nil? (m/subscriber messenger)))
    (try
     (let [;; kill job
           event (compile-task component)
           _ (info log-prefix "Warming up task lifecycle" (:serialized-task event))
           _ (backoff-until-task-start! event)
           event (lc/invoke-before-task-start event)
           task-map (:task-map event)
           ;; stop, and put through handle
           pipeline (build-pipeline task-map event)
           ;; stop, and put through handle
           coordinator (coordinator/new-peer-coordinator log messenger-group opts id job-id group-ch)
           initial-state (new-state-machine event opts messenger messenger-group coordinator pipeline)
           ex-f (fn [e state] 
                  (let [lifecycle (get-lifecycle state)
                        action (handle-exception-action state lifecycle e)] 
                    (stop state)
                    (handle-exception task-information log e lifecycle action 
                                      group-ch outbox-ch id job-id)))]
       (info log-prefix "Enough peers are active, starting the task")
       (let [task-lifecycle-ch (start-task-lifecycle! initial-state ex-f)]
         ;; need extra try to clean up state in cases where state machine was actually created
         (s/validate os/Event event)
         (assoc component
                :event event
                :state initial-state
                :log-prefix log-prefix
                :task-information task-information
                ;; atom for storing peer test state in property test
                :holder (atom nil)
                :task-kill-flag task-kill-flag
                :kill-flag kill-flag
                :task-lifecycle-ch task-lifecycle-ch)))
     (catch Throwable e
       (handle-exception task-information log e :initilizing :kill group-ch outbox-ch id job-id)
       component)))

  (stop [component]
    (if-let [task-name (:name (:task (:task-information component)))]
      (info (:log-prefix component) "Stopping task lifecycle")
      (warn (:log-prefix component) "Stopping task lifecycle, failed to initialize task set up"))

    (when-let [event (:event component)]
      ;; Ensure task operations are finished before closing peer connections
      (println "Killing waiting to fall out")
      (reset! (:kill-flag component) true)
      (when-let [last-state (final-state component)]
        ;;;;; FIXME FIXME FIXME LIFECYCLE ISSUES HERE
        ;;;;; WE NEED TASK LIFECYLCE LOOP DO THE SHUTDOWN
        ;;;;; MIGHT AS WELL DO THE STARTUP TOO
        (stop last-state)
        (when-not (empty? (:triggers (get-event last-state)))
          (ws/assign-windows last-state (:scheduler-event component)))

        (reset! (:task-kill-flag component) true)

        ;; Move these compiled versions to outside of event
        ((:compiled-after-task-fn event) event)))

    (assoc component
           :event nil
           :state nil
           :holder nil
           :log-prefix nil
           :task-information nil
           :task-kill-flag nil
           :kill-flag nil
           :task-lifecycle-ch nil)))

(defn task-lifecycle [peer task]
  (map->TaskLifeCycle (merge peer task)))
