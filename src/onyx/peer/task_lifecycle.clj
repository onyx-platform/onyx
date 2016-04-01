(ns ^:no-doc onyx.peer.task-lifecycle
  (:require [clojure.core.async :refer [alts!! <!! >!! <! >! poll! timeout chan close! thread go]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info error warn trace fatal]]
            [onyx.schema :refer [Event]]
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
            [onyx.extensions :as extensions]
            [onyx.types :refer [->Ack ->Results ->MonitorEvent dec-count! inc-count! map->Event map->Compiled]]
            [onyx.peer.window-state :as ws]
            [onyx.peer.transform :refer [apply-fn]]
            [onyx.peer.grouping :as g]
            [onyx.plugin.onyx-input :as oi]
            [onyx.plugin.onyx-output :as oo]
            [onyx.plugin.onyx-plugin :as op]
            [onyx.flow-conditions.fc-routing :as r]
            [onyx.log.commands.peer-replica-view :refer [peer-site]]
            [onyx.static.logging :as logger]
            [onyx.state.state-extensions :as state-extensions]
            [onyx.static.default-vals :refer [defaults arg-or-default]]
            [onyx.messaging.protocol-aeron :as protocol]
            [onyx.messaging.common :as mc])
  (:import [uk.co.real_logic.aeron Aeron Aeron$Context FragmentAssembler Publication Subscription AvailableImageHandler]
           [uk.co.real_logic.aeron.driver MediaDriver MediaDriver$Context ThreadingMode]
           [uk.co.real_logic.aeron.logbuffer FragmentHandler]
           [uk.co.real_logic.agrona ErrorHandler]
           [uk.co.real_logic.agrona.concurrent 
            UnsafeBuffer IdleStrategy BackoffIdleStrategy BusySpinIdleStrategy]
           [java.util.function Consumer]
           [java.util.concurrent TimeUnit]))

;;;;

(defn aeron-channel [addr port]
  (format "udp://%s:%s" addr port))

(def no-op-error-handler
  (reify ErrorHandler
    (onError [this x] (taoensso.timbre/warn x))))

(defn backoff-strategy [strategy]
  (case strategy
    :busy-spin (BusySpinIdleStrategy.)
    :low-restart-latency (BackoffIdleStrategy. 100
                                               10
                                               (.toNanos TimeUnit/MICROSECONDS 1)
                                               (.toNanos TimeUnit/MICROSECONDS 100))
    :high-restart-latency (BackoffIdleStrategy. 1000
                                                100
                                                (.toNanos TimeUnit/MICROSECONDS 10)
                                                (.toNanos TimeUnit/MICROSECONDS 1000))))

;; Available image handler could be used to setup a lookup for which streamId matches which sourceIdentity i.e. host
;; Once we know what host it corresponds to, then we should be able to lookup the full peer id via the short peer id + the source identity
(def log-available-image-handler 
  (reify AvailableImageHandler
    (onAvailableImage [this image]
      (let [subscription (.subscription image)] 
        (info "Available image on:" (.channel subscription) (.streamId subscription) (.sessionId image) (.sourceIdentity image)))))) 

(defn start-subscribers!
  [ingress-task-ids conn bind-addr port stream-id idle-strategy task-type replica job-id this-task-id]
  (if (= task-type :input)
    []
    (map
     (fn [src-peer-id]
       (let [channel (aeron-channel bind-addr port)
             subscription (.addSubscription conn channel stream-id)]
         {:subscription subscription
          :src-peer-id src-peer-id}))
     (common/src-peers replica ingress-task-ids job-id))))

(defn stream-observer-handler [event this-task-id buffer offset length header]
  (let [ba (byte-array length)
        _ (.getBytes buffer offset ba)
        res (messaging-decompress ba)]
    (when (and (map? res)
               (= (:type res) :job-completed)
               (= (:onyx.core/id event) (:peer-id res)))
      (info "Should ack barrier on peer" (:peer-id res) (:barrier-epoch res))
      (swap! (:onyx.core/pipeline event)
             (fn [pipeline]
               (oi/ack-barrier pipeline (:barrier-epoch res))))
      (when (oi/completed? @(:onyx.core/pipeline event))
        (let [entry (entry/create-log-entry
                     :exhaust-input
                     {:job (:onyx.core/job-id event) :task this-task-id})]
          (>!! (:onyx.core/outbox-ch event) entry))))))

(defn fragment-data-handler [f]
  (FragmentAssembler.
   (reify FragmentHandler
     (onFragment [this buffer offset length header]
       (f buffer offset length header)))))

(defn consumer [handler ^IdleStrategy idle-strategy limit]
  (reify Consumer
    (accept [this subscription]
      (while (not (Thread/interrupted))
        (let [fragments-read (.poll ^Subscription subscription ^FragmentHandler handler ^int limit)]
          (.idle idle-strategy fragments-read))))))

(defn start-stream-observer! [conn bind-addr port stream-id idle-strategy event task-id]
  (let [channel (aeron-channel bind-addr port)
        subscription (.addSubscription conn channel stream-id)
        handler (fragment-data-handler
                 (fn [buffer offset length header]
                   (stream-observer-handler event task-id buffer offset length header)))
        subscription-fut (future (try (.accept ^Consumer (consumer handler idle-strategy 10) subscription)
                                    (catch Throwable e (fatal e))))]
    {:subscription subscription
     :subscription-fut subscription-fut}))

;;;;

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

(s/defn complete-job [{:keys [onyx.core/job-id onyx.core/task-id] :as event} :- Event]
  (let [entry (entry/create-log-entry :exhaust-input {:job job-id :task task-id})]
    (>!! (:onyx.core/outbox-ch event) entry)))

(defrecord AccumAckSegments [ack-val segments retries])

(defn add-segments [accum routes hash-group leaf]
  (if (empty? routes)
    accum
    (if-let [route (first routes)]
      (let [ack-val 0
            grp (get hash-group route)
            leaf* (-> leaf
                      (assoc :ack-val ack-val)
                      (assoc :hash-group grp)
                      (assoc :route route))
            fused-ack (bit-xor (:ack-val accum) ack-val)]
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
  [compiled {:keys [onyx.core/results] :as event}]
  (let [results (reduce (fn [accumulated result]
                          (let [root (:root result)
                                segments (:segments accumulated)
                                retries (:retries accumulated)
                                ret (add-from-leaves segments retries event result compiled)
                                new-ack (->Ack (:id root) (:completion-id root) (:ack-val ret) (atom 1) nil)
                                acks (conj! (:acks accumulated) new-ack)]
                            (->Results (:tree results) acks (:segments ret) (:retries ret))))
                        results
                        (:tree results))]
    (assoc event :onyx.core/results (persistent-results! results))))

(s/defn flow-retry-segments :- Event
  [{:keys [peer-replica-view state messenger monitoring] :as compiled} 
   {:keys [onyx.core/results] :as event} :- Event]
  (doseq [root (:retries results)]
    (when-let [site (peer-site peer-replica-view (:completion-id root))]
      (emit-latency :peer-retry-segment
                    monitoring
                    #(extensions/internal-retry-segment messenger (:id root) site))))
  event)

(s/defn gen-lifecycle-id
  [event]
  (assoc event :onyx.core/lifecycle-id (uuid/random-uuid)))

(def input-readers
  {:input #'function/read-input-batch
   :function #'function/read-function-batch
   :output #'function/read-function-batch})

(defn read-batch
  [{:keys [peer-replica-view task-type pipeline] :as compiled}
   event]
  (if (and (= task-type :input) (:backpressure? @peer-replica-view))
    (assoc event :onyx.core/batch '())
    (let [f (get input-readers task-type)
          rets (merge event (f event))]
      (merge event (lc/invoke-after-read-batch compiled rets)))))

(s/defn tag-messages :- Event
  [{:keys [peer-replica-view task-type id] :as compiled} event :- Event]
  (if (= task-type :input)
    (update event
            :onyx.core/batch
            (fn [batch]
              (map (fn [leaf]
                     (assoc leaf :src-task-id (:onyx.core/task-id event)))
                   batch)))
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
  (let [rets (merge event (oo/write-batch @(:onyx.core/pipeline event) event))]
    (trace (:log-prefix compiled) (format "Wrote %s segments" (count (:onyx.core/results rets))))
    rets))

(defn handle-exception [task-info log e restart-ch outbox-ch job-id]
  (let [data (ex-data e)]
    (if (:onyx.core/lifecycle-restart? data)
      (do (warn (logger/merge-error-keys (:original-exception data) task-info "Caught exception inside task lifecycle. Rebooting the task."))
          (close! restart-ch))
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
        (run! inc-count! acks))))
  event)

(defn run-task-lifecycle
  "The main task run loop, read batch, ack messages, etc."
  [{:keys [onyx.core/compiled onyx.core/task-information] :as init-event}
   kill-ch ex-f]
  (try
    (while (first (alts!! [kill-ch] :default true))
      (->> init-event
           (gen-lifecycle-id)
           (lc/invoke-before-batch compiled)
           (lc/invoke-read-batch read-batch compiled)
           (tag-messages compiled)
           (apply-fn compiled)
           (build-new-segments compiled)
           (lc/invoke-assign-windows assign-windows compiled)
           (lc/invoke-write-batch write-batch compiled)
           (flow-retry-segments compiled)
           (function/ack-barrier!)
           (lc/invoke-after-batch compiled)))
    (catch Throwable e
      (ex-f e))))

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

(defn new-task-information [peer-state task-state]
  (map->TaskInformation (select-keys (merge peer-state task-state) [:id :log :job-id :task-id])))

(defrecord TaskLifeCycle
    [id log messenger-buffer messenger job-id task-id replica peer-replica-view restart-ch log-prefix
     kill-ch outbox-ch opts task-kill-ch scheduler-event task-monitoring task-information]
  component/Lifecycle

  (start [component]
    (try
      (let [{:keys [workflow catalog task flow-conditions windows filtered-windows
                    triggers filtered-triggers lifecycles task-map metadata]} task-information
            ctx (.availableImageHandler (.errorHandler (Aeron$Context.) no-op-error-handler)
                                        log-available-image-handler)
            aeron-conn (Aeron/connect ctx)

            log-prefix (logger/log-prefix task-information)
            subscriptions (start-subscribers!
                           (:ingress-ids task)
                           aeron-conn
                           (mc/bind-addr opts)
                           (:onyx.messaging/peer-port opts)
                           1
                           (backoff-strategy (arg-or-default :onyx.messaging.aeron/poll-idle-strategy opts))
                           (:onyx/type task-map)
                           @replica
                           job-id
                           task-id)
            subscription-maps (atom subscriptions)

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
                           :onyx.core/restart-ch restart-ch
                           :onyx.core/task-kill-ch task-kill-ch
                           :onyx.core/kill-ch kill-ch
                           :onyx.core/peer-opts opts
                           :onyx.core/fn (operation/resolve-task-fn task-map)
                           :onyx.core/replica replica
                           :onyx.core/peer-replica-view peer-replica-view
                           :onyx.core/log-prefix log-prefix
                           :onyx.core/n-sent-messages (atom 0)
                           :onyx.core/epoch (atom -1)
                           :onyx.core/message-counter (atom {})
                           :onyx.core/global-watermarks (:global-watermarks (:messaging-group messenger))
                           :onyx.core/subscription-maps subscription-maps
                           :onyx.core/aeron-conn aeron-conn
                           :onyx.core/subscriptions subscriptions}

            _ (info log-prefix "Warming up task lifecycle" task)

            add-pipeline (fn [event]
                           (assoc event 
                                  :onyx.core/pipeline 
                                  (atom (build-pipeline task-map event))))

            pipeline-data (->> pipeline-data
                               c/task-params->event-map
                               c/flow-conditions->event-map
                               c/lifecycles->event-map
                               (c/windows->event-map filtered-windows filtered-triggers)
                               (c/triggers->event-map filtered-triggers)
                               add-pipeline
                               c/task->event-map)

            ex-f (fn [e] (handle-exception task-information log e restart-ch outbox-ch job-id))
            _ (while (and (first (alts!! [kill-ch task-kill-ch] :default true))
                          (not (start-lifecycle? pipeline-data)))
                (Thread/sleep (arg-or-default :onyx.peer/peer-not-ready-back-off opts)))

            pipeline-data (->> pipeline-data
                               (lc/invoke-before-task-start (:onyx.core/compiled pipeline-data))
                               resolve-filter-state
                               resolve-log
                               replay-windows-from-log
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

        (let [task-lifecycle-ch (thread (run-task-lifecycle pipeline-data kill-ch ex-f))]
          (s/validate Event pipeline-data)
          (assoc component
                 :pipeline-data pipeline-data
                 :log-prefix log-prefix
                 :task-information task-information
                 :task-kill-ch task-kill-ch
                 :kill-ch kill-ch
                 :task-lifecycle-ch task-lifecycle-ch
                 :stream-observer (start-stream-observer! aeron-conn
                                                          (mc/bind-addr opts)
                                                          (:onyx.messaging/peer-port opts)
                                                          1
                                                          (backoff-strategy (arg-or-default :onyx.messaging.aeron/poll-idle-strategy opts))
                                                          pipeline-data
                                                          task-id))))
      (catch Throwable e
        (handle-exception task-information log e restart-ch outbox-ch job-id)
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
      (close! (:kill-ch component))
      (<!! (:task-lifecycle-ch component))
      (close! (:task-kill-ch component))

      (when-let [state-log (:onyx.core/state-log event)] 
        (state-extensions/close-log state-log event))

      (when-let [filter-state (:onyx.core/filter-state event)] 
        (when (exactly-once-task? event)
          (state-extensions/close-filter @filter-state event)))

      (doseq [subscriber (:onyx.core/subscribers event)]
        (.close ^Subscription (:subscription subscriber)))

      (future-cancel (:subscription-fut (:stream-observer component)))
      (.close ^Subscription (:subscription (:stream-observer component)))
      (.close ^Aeron (:onyx.core/aeron-conn event))

      ((:compiled-after-task-fn (:onyx.core/compiled event)) event)
      (op/stop @(:onyx.core/pipeline event) event))

    (assoc component
           :pipeline-data nil
           :task-lifecycle-ch nil)))

(defn task-lifecycle [peer-state task-state]
  (map->TaskLifeCycle (merge peer-state task-state)))
