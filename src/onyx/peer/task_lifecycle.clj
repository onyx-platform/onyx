(ns ^:no-doc onyx.peer.task-lifecycle
    (:require [clojure.core.async :refer [alts!! <!! >!! <! >! timeout chan close! thread go]]
              [com.stuartsierra.component :as component]
              [taoensso.timbre :refer [info warn trace fatal level-compile-time] :as timbre]
              [rotating-seq.core :as rsc]
              [onyx.log.commands.common :as common]
              [onyx.log.entry :as entry]
              [onyx.static.planning :refer [find-task find-task-fast build-pred-fn]]
              [onyx.messaging.acking-daemon :as acker]
              [onyx.peer.pipeline-extensions :as p-ext]
              [onyx.peer.function :as function]
              [onyx.peer.operation :as operation]
              [onyx.extensions :as extensions]
              [onyx.compression.nippy]
              [onyx.types :refer [->Leaf leaf ->Route ->Ack ->Result]]
              [onyx.static.default-vals :refer [defaults arg-or-default]])
    (:import [java.security MessageDigest]))

;; TODO: Are there any exceptions that a peer should autoreboot itself?
(def restartable-exceptions [])

(defn at-least-one-active? [replica peers]
  (->> peers
       (map #(get-in replica [:peer-state %]))
       (filter (partial = :active))
       (seq)))

(defn job-covered? [replica job]
  (let [tasks (get-in replica [:tasks job])
        active? (partial at-least-one-active? replica)]
    (every? identity (map #(active? (get-in replica [:allocations job %])) tasks))))

(defn resolve-calling-params [catalog-entry opts]
  (concat (get (:onyx.peer/fn-params opts) (:onyx/name catalog-entry))
          (map (fn [param] (get catalog-entry param)) (:onyx/params catalog-entry))))

(defn munge-start-lifecycle [event]
  (let [rets ((:onyx.core/compiled-start-task-fn event) event)]
    (when-not (:onyx.core/start-lifecycle? rets)
      (timbre/info (format "[%s / %s] Lifecycle chose not to start the task yet. Backing off and retrying..."
                           (:onx.core/id rets) (:onyx.core/lifecycle-id rets))))
    rets))

(defn add-acker-id [peers m]
  (assoc m :acker-id (rand-nth peers)))

(defn add-completion-id [id m]
  (assoc m :completion-id id))

(defn sentinel-found? [event]
  (seq (filter (partial = :done) (map :message (:onyx.core/batch event)))))

(defn complete-job [{:keys [onyx.core/job-id onyx.core/task-id] :as event}]
  (let [entry (entry/create-log-entry :exhaust-input {:job job-id :task task-id})]
    (>!! (:onyx.core/outbox-ch event) entry)))

(defn sentinel-id [event]
  (:id (first (filter #(= :done (:message %)) (:onyx.core/batch event)))))

(defn join-output-paths [all to-add downstream]
  (cond (= to-add :all) (set downstream)
        (= to-add :none) #{}
        :else (into (set all) to-add)))

(defn choose-output-paths
  [event compiled-flow-conditions result message serialized-task downstream]
  (if (empty? compiled-flow-conditions)
    {:flow downstream}
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
     compiled-flow-conditions)))

(defn add-route-data
  [{:keys [onyx.core/serialized-task onyx.core/compiled-norm-fcs onyx.core/compiled-ex-fcs]
    :as event} result leaf downstream]
  (if (operation/exception? (:message leaf))
    (if (seq compiled-ex-fcs)
      (choose-output-paths event compiled-ex-fcs result (:exception (ex-data (:message leaf))) serialized-task downstream)
      (throw (:exception (ex-data (:message leaf)))))
    (choose-output-paths event compiled-norm-fcs result (:message leaf) serialized-task downstream)))

(defn group-message [segment catalog task]
  (let [t (find-task-fast catalog task)]
    (if-let [k (:onyx/group-by-key t)]
      (hash (get segment k))
      (when-let [f (:onyx/group-by-fn t)]
        (hash ((operation/resolve-fn {:onyx/fn f}) segment))))))

(defn group-segments [leaf next-tasks catalog event]
  (let [post-transformation (:post-transformation (:routes leaf))
        message (:message leaf)
        msg (if (and (operation/exception? message) post-transformation)
              (let [data (ex-data message)]
                (operation/apply-function (operation/kw->fn post-transformation)
                                          [event (:segment data)]
                                          (:exception data)))
              message)]
    (-> leaf 
        (assoc :message (reduce dissoc msg (:exclusions (:routes leaf))))
        (assoc :hash-group (reduce (fn [groups t]
                                     (assoc groups t (group-message msg catalog t)))
                                   {} 
                                   next-tasks)))))

(defn add-ack-vals [leaf]
  (assoc leaf 
         :ack-vals 
         (acker/generate-acks (count (:flow (:routes leaf))))))

(defn build-new-segments
  [{:keys [onyx.core/results onyx.core/serialized-task onyx.core/catalog] :as event}]
  (let [downstream (keys (:egress-ids serialized-task))
        results (doall
                  (map (fn [{:keys [root leaves] :as result}]
                         (let [{:keys [id acker-id completion-id]} root] 
                           (assoc result 
                                  :leaves 
                                  (map (fn [leaf]
                                         (-> leaf 
                                             (assoc :routes (add-route-data event result leaf downstream))
                                             (assoc :id id)
                                             (assoc :acker-id acker-id)
                                             (assoc :completion-id completion-id)
                                             (group-segments downstream catalog event)
                                             (add-ack-vals)))
                                       leaves))))
                       results))]
    (assoc event :onyx.core/results results)))

(defn ack-routes? [routes]
  (and (not-empty (:flow routes))
       (not= (:action routes) 
             :retry)))

(defn gen-ack-fusion-vals 
  "Prefuses acks to reduce packet size"
  [task-map leaves]
  (if-not (= (:onyx/type task-map) :output)
    (reduce (fn [fused-ack leaf]
              (if (ack-routes? (:routes leaf))
                (reduce bit-xor fused-ack (:ack-vals leaf))
                fused-ack)) 
            0
            leaves)
    0))

(defn ack-messages [{:keys [onyx.core/results onyx.core/task-map] :as event}]
  (doseq [[acker-id results-by-acker] (group-by (comp :acker-id :root) results)]
    (let [link (operation/peer-link event acker-id)
          acks (doall 
                 (map (fn [result] (let [fused-leaf-vals (gen-ack-fusion-vals task-map (:leaves result))
                                         fused-vals (if-let [ack-val (:ack-val (:root result))] 
                                                      (bit-xor fused-leaf-vals ack-val)
                                                      fused-leaf-vals)]
                                     (->Ack (:id (:root result))
                                            (:completion-id (:root result))
                                            ;; or'ing by zero covers the case of flow conditions where an
                                            ;; input task produces a segment that goes nowhere.
                                            (or fused-vals 0)
                                            (System/currentTimeMillis))))
                      results-by-acker))] 
      (extensions/internal-ack-messages (:onyx.core/messenger event) event link acks)))
  event)

(defn flow-retry-messages [{:keys [onyx.core/results] :as event}]
  (doseq [result results]
    (when (seq (filter (fn [leaf] (= :retry (:action (:routes leaf)))) (:leaves result)))
      (let [link (operation/peer-link event (:completion-id (:root result)))]
        (extensions/internal-retry-message
          (:onyx.core/messenger event)
          event
          (:id (:root result))
          link))))
  event)

(defn inject-batch-resources [event]
  (let [rets (-> event 
                 (merge ((:onyx.core/compiled-before-batch-fn event) event))
                 (assoc :onyx.core/lifecycle-id (java.util.UUID/randomUUID)))]
    (taoensso.timbre/trace (format "[%s / %s] Started a new batch"
                                   (:onyx.core/id rets) (:onyx.core/lifecycle-id rets)))
    rets))

(defn read-batch [event]
  (let [rets (p-ext/read-batch event)]
    (when (and (= (count (:onyx.core/batch rets)) 1)
               (= (:message (first (:onyx.core/batch rets))) :done))
      (Thread/sleep (:onyx.core/drained-back-off event)))
    (merge event rets)))

(defn tag-messages [event]
  (merge
   event
   (when (= (:onyx/type (:onyx.core/task-map event)) :input)
     (let [{:keys [onyx.core/id onyx.core/max-acker-links]} event
           peers (get (:ackers @(:onyx.core/replica event)) (:onyx.core/job-id event))
           candidates (operation/select-n-peers id peers max-acker-links)] 
       (when-not (seq peers)
         (do (warn (format "[%s] This job no longer has peers capable of acking. This job will now pause execution." (:onyx.core/id event)))
             (throw (ex-info "Not enough acker peers" {:peers peers}))))
       (update-in
         event
         [:onyx.core/batch]
         (fn [batch]
           (map (comp (partial add-completion-id id)
                      (partial add-acker-id candidates))
                batch)))))))

(defn add-messages-to-timeout-pool [{:keys [onyx.core/state] :as event}]
  (when (= (:onyx/type (:onyx.core/task-map event)) :input)
    (swap! state update-in [:timeout-pool] rsc/add-to-head
           (map :id (:onyx.core/batch event))))
  event)

(defn try-complete-job [event]
  (when (sentinel-found? event)
    (if (p-ext/drained? event)
      (complete-job event)
      (p-ext/retry-message event (sentinel-id event))))
  event)

(defn strip-sentinel
  [event]
  (update-in event
             [:onyx.core/batch]
             (fn [batch]
               (remove (fn [v] (= :done (:message v)))
                       batch))))

(defn collect-next-segments [event input]
  (let [segments (try (function/apply-fn event input)
                      (catch Throwable e
                        (ex-info "Segment threw exception"
                                 {:exception e :segment input})))]
    (if (sequential? segments) segments (vector segments))))

(defn apply-fn-single [{:keys [onyx.core/batch] :as event}]
  (assoc
   event
   :onyx.core/results
   (doall
     (map
       (fn [segment]
         (let [segments (collect-next-segments event (:message segment))
               leaves (map leaf segments)]
           (->Result segment leaves)))
       batch))))

(defn apply-fn-bulk [{:keys [onyx.core/batch] :as event}]
  ;; Bulk functions intentionally ignore their outputs.
  (let [segments (map :message batch)]
    (function/apply-fn event segments)
    (assoc
      event
      :onyx.core/results
      (doall 
        (map
          (fn [segment]
            (->Result segment (list (leaf (:message segment)))))
          batch)))))

(defn apply-fn [event]
  (let [rets
        (if (:onyx/bulk? (:onyx.core/task-map event))
          (apply-fn-bulk event)
          (apply-fn-single event))]
    (taoensso.timbre/trace (format "[%s / %s] Applied fn to %s segments"
                                   (:onyx.core/id rets)
                                   (:onyx.core/lifecycle-id rets)
                                   (count (:onyx.core/results rets))))
    rets))

(defn write-batch [event]
  (let [rets (merge event (p-ext/write-batch event))]
    (taoensso.timbre/trace (format "[%s / %s] Wrote %s segments"
                                   (:onyx.core/id rets)
                                   (:onyx.core/lifecycle-id rets)
                                   (count (:onyx.core/results rets))))
    rets))

(defn close-batch-resources [event]
  (let [rets (merge event ((:onyx.core/compiled-after-batch-fn event) event))]
    (taoensso.timbre/trace (format "[%s / %s] Closed batch plugin resources"
                                   (:onyx.core/id rets)
                                   (:onyx.core/lifecycle-id rets)))
    rets))

(defn launch-aux-threads!
  [{:keys [release-ch retry-ch] :as messenger} event outbox-ch seal-ch completion-ch task-kill-ch]
  (thread
   (try
     (loop []
       (when-let [[v ch] (alts!! [task-kill-ch completion-ch seal-ch release-ch retry-ch])]
         (when v
           (cond (= ch release-ch)
                 (p-ext/ack-message event v)

                 (= ch retry-ch)
                 (p-ext/retry-message event v)

                 (= ch completion-ch)
                 (let [{:keys [id peer-id]} v
                       peer-link (operation/peer-link event peer-id)]
                   (extensions/internal-complete-message (:onyx.core/messenger event) event id peer-link))

                 (= ch seal-ch)
                 (do
                   (p-ext/seal-resource event)
                   (let [entry (entry/create-log-entry :seal-output {:job (:onyx.core/job-id event)
                                                                     :task (:onyx.core/task-id event)})]
                     (>!! outbox-ch entry))))
           (recur))))
     (catch Throwable e
       (fatal e)))))

(defn input-retry-messages! [messenger event input-retry-timeout task-kill-ch]
  (go
   (when (= :input (:onyx/type (:onyx.core/task-map event)))
     (loop []
       (let [timeout-ch (timeout input-retry-timeout)
             ch (second (alts!! [timeout-ch task-kill-ch]))]
         (when (= ch timeout-ch)
           (let [tail (last (get-in @(:onyx.core/state event) [:timeout-pool]))]
             (doseq [m tail]
               (when (p-ext/pending? event m)
                 (taoensso.timbre/trace (format "Input retry message %s" m))
                 (p-ext/retry-message event m)))
             (swap! (:onyx.core/state event) update-in [:timeout-pool] rsc/expire-bucket)
             (recur))))))))

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

(defn run-task-lifecycle 
  "The main task run loop, read batch, ack messages, etc."
  [init-event seal-ch kill-ch ex-f]
  (try
    (while (first (alts!! [seal-ch kill-ch] :default true))
      (-> init-event
          (inject-batch-resources)
          (read-batch)
          (tag-messages)
          (add-messages-to-timeout-pool)
          (try-complete-job)
          (strip-sentinel)
          (apply-fn)
          (build-new-segments)
          (write-batch)
          (flow-retry-messages)
          (ack-messages)
          (close-batch-resources)))
    (catch Throwable e
      (ex-f e))))

(defn resolve-compression-fn-impls [opts]
  (assoc opts
    :onyx.peer/decompress-fn-impl
    (if-let [f (:onyx.peer/decompress-fn opts)]
      (operation/resolve-fn f)
      onyx.compression.nippy/decompress)
    :onyx.peer/compress-fn-impl
    (if-let [f (:onyx.peer/compress-fn opts)]
      (operation/resolve-fn f)
      onyx.compression.nippy/compress)))

(defn gc-peer-links [event state opts]
  (let [interval (or (:onyx.messaging/peer-link-gc-interval opts)
                     (:onyx.messaging/peer-link-gc-interval defaults))
        idle (or (:onyx.messaging/peer-link-idle-timeout opts)
                 (:onyx.messaging/peer-link-idle-timeout defaults))]
    (loop []
      (try
        (Thread/sleep interval)
        (let [t (System/currentTimeMillis)
              snapshot @state
              to-remove (map first 
                             (filter (fn [[k v]] (>= (- t (:timestamp v)) idle)) 
                                     (:links snapshot)))]
          (doseq [k to-remove]
            (swap! state dissoc k)
            (extensions/close-peer-connection (:onyx.core/messenger event) 
                                              event 
                                              (:link (get (:links snapshot) k)))))
        (catch InterruptedException e
          (throw e))
        (catch Throwable e
          (fatal e)))
      (recur))))

(defn any-ackers? [replica job-id]
  (> (count (get-in replica [:ackers job-id])) 0))

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

(defn compile-before-task-start-functions [lifecycles task-name]
  (compile-lifecycle-functions lifecycles task-name :lifecycle/before-task-start))

(defn compile-before-batch-task-functions [lifecycles task-name]
  (compile-lifecycle-functions lifecycles task-name :lifecycle/before-batch))

(defn compile-after-batch-task-functions [lifecycles task-name]
  (compile-lifecycle-functions lifecycles task-name :lifecycle/after-batch))

(defn compile-after-task-functions [lifecycles task-name]
  (compile-lifecycle-functions lifecycles task-name :lifecycle/after-task-stop))

(defn resolve-task-fn [entry]
  (when (= (:onyx/type entry) :function)
    (operation/kw->fn (:onyx/fn entry))))

(defn validate-pending-timeout [pending-timeout opts]
  (when (> pending-timeout (arg-or-default :onyx.messaging/ack-daemon-timeout opts))
    (throw (ex-info "Pending timeout cannot be greater than acking daemon timeout"
                    {:opts opts :pending-timeout pending-timeout}))))

(defrecord TaskLifeCycle
    [id log messenger-buffer messenger job-id task-id replica restart-ch
     kill-ch outbox-ch seal-resp-ch completion-ch opts task-kill-ch]
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
            input-retry-timeout (or (:onyx/input-retry-timeout catalog-entry) 
                                    (:onyx/input-retry-timeout defaults))
            pending-timeout (or (:onyx/pending-timeout catalog-entry) 
                                (:onyx/pending-timeout defaults))
            r-seq (rsc/create-r-seq pending-timeout input-retry-timeout)
            state (atom {:timeout-pool r-seq})

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
                           :onyx.core/seal-ch seal-resp-ch
                           :onyx.core/peer-opts (resolve-compression-fn-impls opts)
                           :onyx.core/max-downstream-links (or (:onyx.messaging/max-downstream-links opts)
                                                               (:onyx.messaging/max-downstream-links defaults))
                           :onyx.core/max-acker-links (or (:onyx.messaging/max-acker-links opts)
                                                          (:onyx.messaging/max-acker-links defaults))
                           :onyx.core/fn (resolve-task-fn catalog-entry)
                           :onyx.core/replica replica
                           :onyx.core/state state}

            ex-f (fn [e] (handle-exception e restart-ch outbox-ch job-id))
            _ (while (and (first (alts!! [kill-ch task-kill-ch] :default true))
                          (not (munge-start-lifecycle pipeline-data)))
                (Thread/sleep (or (:onyx.peer/peer-not-ready-back-off opts) 2000)))
            pipeline-data (merge pipeline-data ((:onyx.core/compiled-before-task-start-fn pipeline-data) pipeline-data))]

        (>!! outbox-ch (entry/create-log-entry :signal-ready {:id id}))

        (loop [replica-state @replica]
          (when (and (first (alts!! [kill-ch task-kill-ch] :default true))
                     (or (not (job-covered? replica-state job-id))
                         (not (any-ackers? replica-state job-id))))
            (taoensso.timbre/info (format "[%s] Not enough virtual peers have warmed up to start the task yet, backing off and trying again..." id))
            (Thread/sleep 500)
            (recur @replica)))

        (taoensso.timbre/info (format "[%s] Enough peers are active, starting the task" id))

        (let [input-retry-messages-ch (input-retry-messages! messenger pipeline-data input-retry-timeout task-kill-ch)
              aux-ch (launch-aux-threads! messenger pipeline-data outbox-ch seal-resp-ch completion-ch task-kill-ch)
              task-lifecycle-ch (thread (run-task-lifecycle pipeline-data seal-resp-ch kill-ch ex-f))
              peer-link-gc-thread (future (gc-peer-links pipeline-data state opts))]
          (assoc component 
            :pipeline-data pipeline-data
            :seal-ch seal-resp-ch
            :task-kill-ch task-kill-ch
            :task-lifecycle-ch task-lifecycle-ch
            :input-retry-messages-ch input-retry-messages-ch
            :aux-ch aux-ch
            :peer-link-gc-thread peer-link-gc-thread)))
      (catch Throwable e
        (handle-exception e restart-ch outbox-ch job-id)
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
      :pipeline-data nil
      :seal-ch nil
      :aux-ch nil
      :input-retry-messages-ch nil
      :task-lifecycle-ch nil
      :task-lifecycle-ch nil
      :peer-link-gc-thread nil)))

(defn task-lifecycle [args {:keys [id log messenger-buffer messenger job task replica
                                   restart-ch kill-ch outbox-ch seal-ch completion-ch opts task-kill-ch]}]
  (map->TaskLifeCycle {:id id :log log :messenger-buffer messenger-buffer
                       :messenger messenger :job-id job :task-id task :restart-ch restart-ch
                       :kill-ch kill-ch :outbox-ch outbox-ch
                       :replica replica :seal-resp-ch seal-ch :completion-ch completion-ch
                       :opts opts :task-kill-ch task-kill-ch}))
