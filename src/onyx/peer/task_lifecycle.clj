(ns ^:no-doc onyx.peer.task-lifecycle
    (:require [clojure.core.async :refer [alts!! alt!! <!! >!! <! >! timeout chan close! thread go]]
              [com.stuartsierra.component :as component]
              [taoensso.timbre :refer [info warn trace fatal] :as timbre]
              [onyx.static.rotating-seq :as rsc]
              [onyx.log.commands.common :as common]
              [onyx.log.entry :as entry]
              [onyx.monitoring.measurements :refer [emit-latency]]
              [onyx.static.planning :refer [find-task find-task-fast build-pred-fn]]
              [onyx.messaging.acking-daemon :as acker]
              [onyx.peer.pipeline-extensions :as p-ext]
              [onyx.peer.function :as function]
              [onyx.peer.operation :as operation]
              [onyx.extensions :as extensions]
              [onyx.compression.nippy]
              [onyx.types :refer [->Route ->Ack ->Results ->Result ->MonitorEvent]]
              [clj-tuple :as t]
              [onyx.interop]
              [onyx.static.default-vals :refer [defaults arg-or-default]]))

(defn resolve-calling-params [catalog-entry opts]
  (concat (get (:onyx.peer/fn-params opts) (:onyx/name catalog-entry))
          (map (fn [param] (get catalog-entry param)) (:onyx/params catalog-entry))))

(defn munge-start-lifecycle [event]
  (let [rets ((:onyx.core/compiled-start-task-fn event) event)]
    (when-not (:onyx.core/start-lifecycle? rets)
      (timbre/info (format "[%s / %s] Lifecycle chose not to start the task yet. Backing off and retrying..."
                           (:onyx.core/id rets) (:onyx.core/lifecycle-id rets))))
    rets))

(defn add-acker-id [id m]
  (assoc m :acker-id id))

(defn add-completion-id [id m]
  (assoc m :completion-id id))

(defn sentinel-found? [event]
  (seq (filter #(= :done (:message %)) 
               (:onyx.core/batch event))))

(defn complete-job [{:keys [onyx.core/job-id onyx.core/task-id] :as event}]
  (let [entry (entry/create-log-entry :exhaust-input {:job job-id :task task-id})]
    (>!! (:onyx.core/outbox-ch event) entry)))

(defn sentinel-id [event]
  (:id (first (filter #(= :done (:message %)) 
                      (:onyx.core/batch event)))))

(defn join-output-paths [all to-add downstream]
  (cond (= to-add :all) (set downstream)
        (= to-add :none) #{}
        :else (into (set all) to-add)))

(defn choose-output-paths
  [event compiled-flow-conditions result message downstream]
  (reduce
    (fn [{:keys [flow exclusions] :as all} entry]
      (if ((:flow/predicate entry) [event (:message (:root result)) message (map :message (:leaves result))])
        (if (:flow/short-circuit? entry)
          (reduced (->Route (join-output-paths flow (:flow/to entry) downstream)
                            (into (set exclusions) (:flow/exclude-keys entry))
                            (:flow/post-transform entry)
                            (:flow/action entry)))
          (->Route (join-output-paths flow (:flow/to entry) downstream)
                   (into (set exclusions) (:flow/exclude-keys entry))
                   nil
                   nil))
        all))
    (->Route #{} #{} nil nil)
    compiled-flow-conditions))

(defn route-data
  [event result message flow-conditions downstream]
  (if (nil? flow-conditions)
    (->Route downstream nil nil nil)
    (let [compiled-ex-fcs (:onyx.core/compiled-ex-fcs event)]
      (if (operation/exception? message)
        (if (seq compiled-ex-fcs)
          (choose-output-paths event compiled-ex-fcs result 
                               (:exception (ex-data message)) downstream)  
          (throw (:exception (ex-data message))))
        (let [compiled-norm-fcs (:onyx.core/compiled-norm-fcs event)]
          (if (seq compiled-norm-fcs) 
            (choose-output-paths event compiled-norm-fcs result message downstream)
            (->Route downstream nil nil nil)))))))

(defn apply-post-transformation [message routes event]
  (let [post-transformation (:post-transformation routes)
        msg (if (and (operation/exception? message) post-transformation)
              (let [data (ex-data message)
                    f (operation/kw->fn post-transformation)] 
                (f event (:segment data) (:exception data)))
              message)]
    (reduce dissoc msg (:exclusions routes))))

(defn hash-groups [message next-tasks task->group-by-fn]
  (if (not-empty task->group-by-fn)
    (reduce (fn [groups t]
              (if-let [group-fn (task->group-by-fn t)]
                (assoc groups t (hash (group-fn message)))
                groups))
            (t/hash-map)
            next-tasks)))

(defn flow-conditions-transform 
  [message routes next-tasks flow-conditions event]
  (if flow-conditions
    (apply-post-transformation message routes event)
    message))

(defrecord AccumAckSegments [ack-val segments])

(defn add-from-leaves 
  "Flattens root/leaves into an xor'd ack-val and accumulates new segments"
  [segments event result egress-ids task->group-by-fn flow-conditions]
  (let [root (:root result)
        leaves (:leaves result)
        start-ack-val (or (:ack-val root) 0)] 
    (reduce (fn process-leaf [accum {:keys [message] :as leaf}]
            (let [routes (route-data event result message flow-conditions egress-ids)
                  message* (flow-conditions-transform message routes egress-ids flow-conditions event)
                  hash-group (hash-groups message* egress-ids task->group-by-fn)
                  leaf* (if (= message message*)
                          leaf
                          (assoc leaf :message message*))] 
              (if (= :retry (:action routes))
                accum
                (reduce (fn process-route [accum2 route]
                          (if route 
                            (let [ack-val (acker/gen-ack-value)
                                  grp (get hash-group route)
                                  leaf** (-> leaf* 
                                             (assoc :ack-val ack-val)
                                             (assoc :hash-group grp)
                                             (assoc :route route))]
                              (->AccumAckSegments 
                                (bit-xor ^long (:ack-val accum2) ^long ack-val)
                                (conj! (:segments accum2) leaf**)))
                            accum2))
                        accum 
                        (:flow routes)))))
          (->AccumAckSegments start-ack-val segments)
          leaves)))

(defn persistent-results! [results]
  (->Results (:tree results)
             (persistent! (:acks results))
             (persistent! (:segments results))))

(defn build-new-segments
  [egress-ids task->group-by-fn flow-conditions {:keys [onyx.core/results] :as event}]
  (let [results (reduce (fn [accumulated result]
                          (let [root (:root result)
                                segments (:segments accumulated)
                                ret (add-from-leaves segments event result egress-ids 
                                                     task->group-by-fn flow-conditions)
                                new-ack (->Ack (:id root) (:completion-id root) (:ack-val ret) nil)] 
                            (->Results (:tree results)
                                       (conj! (:acks accumulated) new-ack)
                                       (:segments ret))))
                        results
                        (:tree results))]
    (assoc event :onyx.core/results (persistent-results! results))))

(defn ack-messages [task-map replica state messenger monitoring {:keys [onyx.core/results] :as event}]
  (doseq [[acker-id acks] (group-by :completion-id (:acks results))]
    (let [link (operation/peer-link @replica state event acker-id)]
      (emit-latency :peer-ack-messages
                    monitoring
                    #(extensions/internal-ack-messages messenger event link acks))))
  event)

(defn flow-retry-messages [replica state messenger monitoring {:keys [onyx.core/results] :as event}]
  (doseq [result results]
    (when (seq (filter (fn [leaf] (= :retry (:action (:routes leaf)))) (:leaves result)))
      (let [root (:root result)
            link (operation/peer-link @replica state event (:completion-id root))]
        (emit-latency :peer-retry-message
                      monitoring
                      #(extensions/internal-retry-message messenger event (:id root) link)))))
  event)

(defn inject-batch-resources [compiled-before-batch-fn pipeline event]
  (let [rets (-> (compiled-before-batch-fn event)
                 (assoc :onyx.core/lifecycle-id (java.util.UUID/randomUUID)))]
    (taoensso.timbre/trace (format "[%s / %s] Started a new batch"
                                   (:onyx.core/id rets) (:onyx.core/lifecycle-id rets)))
    rets))

(defn handle-backoff! [event]
  (let [batch (:onyx.core/batch event)]
    (when (and (= (count batch) 1)
               (= (:message (first batch)) :done))
      (Thread/sleep (:onyx.core/drained-back-off event)))))

(defn read-batch [task-type replica peer-replica-view job-id pipeline event]
  (if (and (= task-type :input) (:backpressure? peer-replica-view)) 
    (assoc event :onyx.core/batch '())
    (let [rets (p-ext/read-batch pipeline event)]
      (handle-backoff! event)
      (merge event rets))))

(defn validate-ackable! [peers event]
  (when-not (seq peers)
    (do (warn (format "[%s] This job no longer has peers capable of acking. This job will now pause execution." (:onyx.core/id event)))
        (throw (ex-info "Not enough acker peers" {:peers peers}))))) 

(defn tag-messages [task-type replica peer-replica-view id event]
  (if (= task-type :input)
    (let [candidates (:acker-candidates @peer-replica-view)
          _ (validate-ackable! candidates event)] 
      (assoc event 
             :onyx.core/batch
             (map (fn [segment] 
                    (add-acker-id (rand-nth candidates)
                                  (add-completion-id id segment)))
                  (:onyx.core/batch event))))
    event))

(defn add-messages-to-timeout-pool [task-type state event]
  (when (= task-type :input)
    (swap! state update-in [:timeout-pool] rsc/add-to-head
           (map :id (:onyx.core/batch event))))
  event)

(defn process-sentinel [task-type pipeline monitoring event]
  (if (and (= task-type :input) 
           (sentinel-found? event))
    (do 
      (extensions/emit monitoring (->MonitorEvent :peer-sentinel-found))
      (if (p-ext/drained? pipeline event)
          (complete-job event)
          (p-ext/retry-message pipeline event (sentinel-id event)))
        (update-in event
                   [:onyx.core/batch]
                   (fn [batch]
                     (remove (fn [v] (= :done (:message v)))
                             batch))))
    event))

(defn collect-next-segments [f input]
  (let [segments (try (f input)
                      (catch Throwable e
                        (ex-info "Segment threw exception"
                                 {:exception e :segment input})))]
    (if (sequential? segments) segments (t/vector segments))))

(defn apply-fn-single [f {:keys [onyx.core/batch] :as event}]
  (assoc
   event
   :onyx.core/results
    (->Results (doall
                 (map
                   (fn [segment]
                     (let [segments (collect-next-segments f (:message segment))
                           leaves (map (fn [message]
                                         (-> segment
                                             (assoc :message message)
                                             ;; not actually required, but it's safer
                                             (assoc :ack-val nil))) 
                                       segments)]
                       (->Result segment leaves)))
                   batch))
               (transient (t/vector))
               (transient (t/vector)))))

(defn apply-fn-bulk [f {:keys [onyx.core/batch] :as event}]
  ;; Bulk functions intentionally ignore their outputs.
  (let [segments (map :message batch)]
    (f segments)
    (assoc
      event
      :onyx.core/results
      (->Results (doall 
                   (map
                     (fn [segment]
                       (->Result segment (t/vector (assoc segment :ack-val nil))))
                     batch))
                 (transient (t/vector))
                 (transient (t/vector))))))

(defn curry-params [f params]
  (reduce #(partial %1 %2) f params))

(defn apply-fn [f bulk? event]
  (let [g (curry-params f (:onyx.core/params event))
        rets
        (if bulk?
          (apply-fn-bulk g event)
          (apply-fn-single g event))]
    (taoensso.timbre/trace (format "[%s / %s] Applied fn to %s segments"
                                   (:onyx.core/id rets)
                                   (:onyx.core/lifecycle-id rets)
                                   (count (:onyx.core/results rets))))
    rets))

(defn write-batch [pipeline event]
  (let [rets (merge event (p-ext/write-batch pipeline event))]
    (taoensso.timbre/trace (format "[%s / %s] Wrote %s segments"
                                   (:onyx.core/id rets)
                                   (:onyx.core/lifecycle-id rets)
                                   (count (:onyx.core/results rets))))
    rets))

(defn close-batch-resources [event]
  (let [rets ((:onyx.core/compiled-after-batch-fn event) event)]
    (taoensso.timbre/trace (format "[%s / %s] Closed batch plugin resources"
                                   (:onyx.core/id rets)
                                   (:onyx.core/lifecycle-id rets)))
    rets))

(defn launch-aux-threads!
  [{:keys [release-ch retry-ch] :as messenger} {:keys [onyx.core/pipeline
                                                       onyx.core/compiled-after-ack-message-fn 
                                                       onyx.core/compiled-after-retry-message-fn 
                                                       onyx.core/monitoring
                                                       onyx.core/replica
                                                       onyx.core/state] :as event} 
   outbox-ch seal-ch completion-ch task-kill-ch]
  (thread
   (try
     (loop []
       (when-let [[v ch] (alts!! [task-kill-ch completion-ch seal-ch release-ch retry-ch])]
         (when v
           (cond (= ch release-ch)
                 (->> (p-ext/ack-message pipeline event v)
                      (compiled-after-ack-message-fn event v))

                 (= ch completion-ch)
                 (let [{:keys [id peer-id]} v
                       peer-link (operation/peer-link @replica state event peer-id)]
                   (emit-latency :peer-complete-message
                                 monitoring 
                                 #(extensions/internal-complete-message messenger event id peer-link)))

                 (= ch retry-ch)
                 (->> (p-ext/retry-message pipeline event v)
                      (compiled-after-retry-message-fn event v))

                 (= ch seal-ch)
                 (do
                   (p-ext/seal-resource pipeline event)
                   (let [entry (entry/create-log-entry :seal-output {:job (:onyx.core/job-id event)
                                                                     :task (:onyx.core/task-id event)})]
                     (>!! outbox-ch entry))))
           (recur))))
     (catch Throwable e
       (fatal e)))))

(defn input-retry-messages! [messenger {:keys [onyx.core/pipeline
                                               onyx.core/compiled-after-retry-message-fn] 
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
                  (taoensso.timbre/trace (format "Input retry message %s" m))
                  (->> (p-ext/retry-message pipeline event m)
                       (compiled-after-retry-message-fn event m))))
              (swap! (:onyx.core/state event) update-in [:timeout-pool] rsc/expire-bucket)
              (recur))))))))

(defn handle-exception [restart-pred-fn e restart-ch outbox-ch job-id]
  (warn e)
  (if (restart-pred-fn e)
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

(defn run-task-lifecycle 
  "The main task run loop, read batch, ack messages, etc."
  [{:keys [onyx.core/task-map
           onyx.core/pipeline
           onyx.core/replica
           onyx.core/peer-replica-view
           onyx.core/state
           onyx.core/compiled-before-batch-fn
           onyx.core/task->group-by-fn 
           onyx.core/flow-conditions
           onyx.core/serialized-task
           onyx.core/messenger
           onyx.core/monitoring
           onyx.core/id 
           onyx.core/params
           onyx.core/fn
           onyx.core/max-acker-links 
           onyx.core/job-id] :as init-event} seal-ch kill-ch ex-f]
  (let [task-type (:onyx/type task-map)
        bulk? (:onyx/bulk? task-map)
        egress-ids (keys (:egress-ids serialized-task))] 
    (try
      (while (first (alts!! [seal-ch kill-ch] :default true))
        (->> init-event
             (inject-batch-resources compiled-before-batch-fn pipeline)
             (read-batch task-type replica peer-replica-view job-id pipeline)
             (tag-messages task-type replica peer-replica-view id)
             (add-messages-to-timeout-pool task-type state)
             (process-sentinel task-type pipeline monitoring)
             (apply-fn fn bulk?)
             (build-new-segments egress-ids task->group-by-fn flow-conditions)
             (write-batch pipeline)
             (flow-retry-messages replica state messenger monitoring)
             (ack-messages task-map replica state messenger monitoring)
             (close-batch-resources)))
      (catch Throwable e
        (ex-f e)))))

(defn resolve-compression-fn-impls [opts]
  (assoc opts
    :onyx.peer-decompress-fn-impl
    (if-let [f (:onyx.peer-decompress-fn opts)]
      (operation/resolve-fn f)
      onyx.compression.nippy/decompress)
    :onyx.peer-compress-fn-impl
    (if-let [f (:onyx.peer-compress-fn opts)]
      (operation/resolve-fn f)
      onyx.compression.nippy/compress)))

(defn gc-peer-links [event state opts]
  (let [interval (arg-or-default :onyx.messaging/peer-link-gc-interval opts)
        idle (arg-or-default :onyx.messaging/peer-link-idle-timeout opts)
        monitoring (:onyx.core/monitoring event)]
    (loop []
      (try
        (Thread/sleep interval)
        (let [t (System/currentTimeMillis)
              snapshot @state
              to-remove (map first 
                             (filter (fn [[k v]] (>= (- t @(:timestamp v)) idle)) 
                                     (:links snapshot)))]
          (doseq [k to-remove]
            (swap! state dissoc k)
            (extensions/close-peer-connection (:onyx.core/messenger event) 
                                              event 
                                              (:link (get (:links snapshot) k)))
            (extensions/emit monitoring (->MonitorEvent :peer-gc-peer-link))))
        (catch InterruptedException e
          (throw e))
        (catch Throwable e
          (fatal e)))
      (recur))))

(defn compile-start-task-functions [lifecycles task-name]
  (let [matched (filter #(= (:lifecycle/task %) task-name) lifecycles)
        fs
        (remove
         nil?
         (map
          (fn [lifecycle]
            (let [calls-map (var-get (operation/kw->fn (:lifecycle/calls lifecycle)))]
              (when-let [g (:lifecycle/start-task? calls-map)]
                (fn [x] (g x lifecycle)))))
          matched))]
    (fn [event]
      (if (seq fs)
        (every? true? ((apply juxt fs) event))
        true))))

(defn compile-lifecycle-functions [lifecycles task-name kw]
  (let [matched (filter #(= (:lifecycle/task %) task-name) lifecycles)]
    (reduce
     (fn [f lifecycle]
       (let [calls-map (var-get (operation/kw->fn (:lifecycle/calls lifecycle)))]
         (if-let [g (get calls-map kw)]
           (comp (fn [x] (merge x (g x lifecycle))) f)
           f)))
     identity
     matched)))

(defn compile-ack-retry-lifecycle-functions [lifecycles task-name kw]
  (let [matched (filter #(= (:lifecycle/task %) task-name) lifecycles)
        fns (keep (fn [lifecycle] 
                    (let [calls-map (var-get (operation/kw->fn (:lifecycle/calls lifecycle)))]
                      (if-let [g (get calls-map kw)]
                        (vector lifecycle g)))) 
                  matched)]
    (reduce (fn [g [lifecycle f]] 
              (fn [event message-id rets] 
                (g event message-id rets)
                (f event message-id rets lifecycle)))
            (fn [event message-id rets])
            fns)))

(defn compile-before-task-start-functions [lifecycles task-name]
  (compile-lifecycle-functions lifecycles task-name :lifecycle/before-task-start))

(defn compile-before-batch-task-functions [lifecycles task-name]
  (compile-lifecycle-functions lifecycles task-name :lifecycle/before-batch))

(defn compile-after-batch-task-functions [lifecycles task-name]
  (compile-lifecycle-functions lifecycles task-name :lifecycle/after-batch))

(defn compile-after-task-functions [lifecycles task-name]
  (compile-lifecycle-functions lifecycles task-name :lifecycle/after-task-stop))

(defn compile-after-ack-message-functions [lifecycles task-name]
  (compile-ack-retry-lifecycle-functions lifecycles task-name :lifecycle/after-ack-message))

(defn compile-after-retry-message-functions [lifecycles task-name]
  (compile-ack-retry-lifecycle-functions lifecycles task-name :lifecycle/after-retry-message))

(defn resolve-task-fn [entry]
  (let [f (if (or (:onyx/fn entry) 
                  (= (:onyx/type entry) :function))
            (case (:onyx/language entry)
              :java (operation/build-fn-java (:onyx/fn entry))
              (operation/kw->fn (:onyx/fn entry))))]
    (or f identity)))

(defn resolve-restart-pred-fn [entry]
  (if-let [kw (:onyx/restart-pred-fn entry)]
    (operation/kw->fn kw)
    (constantly false)))

(defn instantiate-plugin-instance [class-name pipeline-data]
  (.newInstance (.getDeclaredConstructor ^Class (Class/forName class-name)
                                         (into-array Class [clojure.lang.IPersistentMap]))
                (into-array [pipeline-data])))

(defn build-pipeline [task-map pipeline-data]
  (let [kw (:onyx/plugin task-map)]
    (try 
      (if (#{:input :output} (:onyx/type task-map))
        (case (:onyx/language task-map)
          :java (instantiate-plugin-instance (name kw) pipeline-data)
          (let [user-ns (namespace kw)
                user-fn (name kw)
                pipeline (if (and user-ns user-fn)
                           (if-let [f (ns-resolve (symbol user-ns) (symbol user-fn))]
                             (f pipeline-data)))]
            (or pipeline
                (throw (ex-info "Failure to resolve plugin builder fn. 
                                 Did you require the file that contains this symbol?" {:kw kw})))))
        (onyx.peer.function/function pipeline-data))
      (catch Throwable e 
        (throw (ex-info "Failed to resolve or build plugin on the classpath, did you require/import the file that contains this plugin?" {:symbol kw :exception e})))))) 

(defn validate-pending-timeout [pending-timeout opts]
  (when (> pending-timeout (arg-or-default :onyx.messaging/ack-daemon-timeout opts))
    (throw (ex-info "Pending timeout cannot be greater than acking daemon timeout"
                    {:opts opts :pending-timeout pending-timeout}))))

(defn compile-grouping-fn 
  "Compiles grouping outgoing grouping task info into a task->group-fn map
  for quick lookup and group fn calls"
  [catalog egress-ids]
  (merge (->> catalog
              (filter (fn [entry] 
                        (and (:onyx/group-by-key entry)
                             egress-ids
                             (egress-ids (:onyx/name entry)))))
              (map (fn [entry] 
                     (let [group-key (:onyx/group-by-key entry)
                           group-fn (cond (keyword? group-key)
                                          group-key
                                          (sequential? group-key)
                                          #(select-keys % group-key)
                                          :else
                                          #(get % group-key))] 
                       [(:onyx/name entry) group-fn])))
              (into (t/hash-map)))
         (->> catalog 
              (filter (fn [entry] 
                        (and (:onyx/group-by-fn entry)
                             egress-ids
                             (egress-ids (:onyx/name entry)))))
              (map (fn [entry]
                     [(:onyx/name entry)
                      (operation/resolve-fn {:onyx/fn (:onyx/group-by-fn entry)})]))
              (into (t/hash-map)))))

(defn clear-messenger-buffer! 
  "Clears the messenger buffer of all messages related to prevous task lifecycle.
  In an ideal case, this might transfer messages over to another peer first as it help with retries."
  [{:keys [inbound-ch] :as messenger-buffer}]
  (while (first (alts!! [inbound-ch] :default false))))

(defrecord TaskState [timeout-pool links])

(defrecord TaskLifeCycle
    [id log messenger-buffer messenger job-id task-id replica peer-replica-view restart-ch
     kill-ch outbox-ch seal-resp-ch completion-ch opts task-kill-ch monitoring]
  component/Lifecycle

  (start [component]
    (try
      (let [catalog (extensions/read-chunk log :catalog job-id)
            task (extensions/read-chunk log :task task-id)
            flow-conditions (extensions/read-chunk log :flow-conditions job-id)
            lifecycles (extensions/read-chunk log :lifecycles job-id)
            catalog-entry (find-task catalog (:name task))

            ;; Number of buckets in the timeout pool is covered over a 60 second
            ;; interval, moving each bucket back 60 seconds / N buckets
            input-retry-timeout (arg-or-default :onyx/input-retry-timeout catalog-entry) 
            pending-timeout (arg-or-default :onyx/pending-timeout catalog-entry) 
            r-seq (rsc/create-r-seq pending-timeout input-retry-timeout)
            links {}
            state (atom (->TaskState r-seq links))

            _ (taoensso.timbre/info (format "[%s] Warming up Task LifeCycle for job %s, task %s" id job-id (:name task)))
            _ (validate-pending-timeout pending-timeout opts)

            pipeline-data {:onyx.core/id id
                           :onyx.core/job-id job-id
                           :onyx.core/task-id task-id
                           :onyx.core/task (:name task)
                           :onyx.core/catalog catalog
                           :onyx.core/workflow (extensions/read-chunk log :workflow job-id)
                           :onyx.core/flow-conditions flow-conditions
                           :onyx.core/compiled-start-task-fn (compile-start-task-functions lifecycles (:name task))
                           :onyx.core/compiled-before-task-start-fn (compile-before-task-start-functions lifecycles (:name task))
                           :onyx.core/compiled-before-batch-fn (compile-before-batch-task-functions lifecycles (:name task))
                           :onyx.core/compiled-after-batch-fn (compile-after-batch-task-functions lifecycles (:name task))
                           :onyx.core/compiled-after-task-fn (compile-after-task-functions lifecycles (:name task))
                           :onyx.core/compiled-after-ack-message-fn (compile-after-ack-message-functions lifecycles (:name task))
                           :onyx.core/compiled-after-retry-message-fn (compile-after-retry-message-functions lifecycles (:name task))
                           :onyx.core/compiled-norm-fcs (compile-fc-norms flow-conditions (:name task))
                           :onyx.core/compiled-ex-fcs (compile-fc-exs flow-conditions (:name task))
                           :onyx.core/task->group-by-fn (compile-grouping-fn catalog (:egress-ids task))
                           :onyx.core/task-map catalog-entry
                           :onyx.core/serialized-task task
                           :onyx.core/params (resolve-calling-params catalog-entry opts)
                           :onyx.core/drained-back-off (or (:onyx.peer/drained-back-off opts) 400)
                           :onyx.core/log log
                           :onyx.core/messenger-buffer messenger-buffer
                           :onyx.core/messenger messenger
                           :onyx.core/monitoring monitoring
                           :onyx.core/outbox-ch outbox-ch
                           :onyx.core/seal-ch seal-resp-ch
                           :onyx.core/peer-opts (resolve-compression-fn-impls opts)
                           :onyx.core/max-downstream-links (arg-or-default :onyx.messaging/max-downstream-links opts)
                           :onyx.core/max-acker-links (arg-or-default :onyx.messaging/max-acker-links opts)
                           :onyx.core/fn (resolve-task-fn catalog-entry)
                           :onyx.core/replica replica
                           :onyx.core/peer-replica-view peer-replica-view
                           :onyx.core/state state}

            pipeline (build-pipeline catalog-entry pipeline-data)
            pipeline-data (assoc pipeline-data :onyx.core/pipeline pipeline)

            restart-pred-fn (resolve-restart-pred-fn catalog-entry)
            ex-f (fn [e] (handle-exception restart-pred-fn e restart-ch outbox-ch job-id))
            _ (while (and (first (alts!! [kill-ch task-kill-ch] :default true))
                          (not (munge-start-lifecycle pipeline-data)))
                (Thread/sleep (or (:onyx.peer/peer-not-ready-back-off opts) 2000)))

            pipeline-data ((:onyx.core/compiled-before-task-start-fn pipeline-data) pipeline-data)]

        (clear-messenger-buffer! messenger-buffer)
        (>!! outbox-ch (entry/create-log-entry :signal-ready {:id id}))

        (loop [replica-state @replica]
          (when (and (first (alts!! [kill-ch task-kill-ch] :default true))
                     (or (not (common/job-covered? replica-state job-id))
                         (not (common/any-ackers? replica-state job-id))))
            (taoensso.timbre/info (format "[%s] Not enough virtual peers have warmed up to start the task yet, backing off and trying again..." id))
            (Thread/sleep 500)
            (recur @replica)))

        (taoensso.timbre/info (format "[%s] Enough peers are active, starting the task" id))

        (let [input-retry-messages-ch (input-retry-messages! messenger pipeline-data input-retry-timeout task-kill-ch)
              aux-ch (launch-aux-threads! messenger pipeline-data outbox-ch seal-resp-ch completion-ch task-kill-ch)
              task-lifecycle-ch (thread (run-task-lifecycle pipeline-data seal-resp-ch kill-ch ex-f))
              peer-link-gc-thread (future (gc-peer-links pipeline-data state opts))]
          (assoc component 
                 :pipeline pipeline
                 :pipeline-data pipeline-data
                 :seal-ch seal-resp-ch
                 :task-kill-ch task-kill-ch
                 :task-lifecycle-ch task-lifecycle-ch
                 :input-retry-messages-ch input-retry-messages-ch
                 :aux-ch aux-ch
                 :peer-link-gc-thread peer-link-gc-thread)))
      (catch Throwable e
        (handle-exception (constantly false) e restart-ch outbox-ch job-id)
        component)))

  (stop [component]
    (taoensso.timbre/info (format "[%s] Stopping Task LifeCycle for %s" id (:onyx.core/task (:pipeline-data component))))
    (when-let [event (:pipeline-data component)]
      ;; Ensure task operations are finished before closing peer connections
      (close! (:seal-ch component))
      (<!! (:task-lifecycle-ch component))
      (close! (:task-kill-ch component))

      (<!! (:input-retry-messages-ch component))
      (<!! (:aux-ch component))
      
      ((:onyx.core/compiled-after-task-fn event) event)
      
      (let [state @(:onyx.core/state event)]
        (doseq [[_ link-map] (:links state)]
          (extensions/close-peer-connection (:onyx.core/messenger event) event (:link link-map)))))

    (when-let [t (:peer-link-gc-thread component)]
      (future-cancel t))

    (assoc component
      :pipeline nil
      :pipeline-data nil
      :seal-ch nil
      :aux-ch nil
      :input-retry-messages-ch nil
      :task-lifecycle-ch nil
      :peer-link-gc-thread nil)))

(defn task-lifecycle [args {:keys [id log messenger-buffer messenger job task replica peer-replica-view
                                   restart-ch kill-ch outbox-ch seal-ch completion-ch opts task-kill-ch
                                   monitoring]}]
  (map->TaskLifeCycle {:id id :log log :messenger-buffer messenger-buffer
                       :messenger messenger :job-id job :task-id task :restart-ch restart-ch
                       :peer-replica-view peer-replica-view
                       :kill-ch kill-ch :outbox-ch outbox-ch
                       :replica replica :seal-resp-ch seal-ch :completion-ch completion-ch
                       :opts opts :task-kill-ch task-kill-ch :monitoring monitoring}))
