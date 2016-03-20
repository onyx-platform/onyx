(ns onyx.schema
  (:require [schema.core :as s]
            [onyx.information-model :as i]))

(def NamespacedKeyword
  (s/pred (fn [kw]
            (and (keyword? kw)
                 (namespace kw)))
          'keyword-namespaced?))

(def Function
  (s/cond-pre (s/pred var? 'var?)
              (s/pred ifn? 'ifn?)))

(def TaskName
  (s/pred (fn [v]
            (and (not= :all v)
                 (not= :none v)
                 (keyword? v)))
          'task-name?))

(defn ^{:private true} edge-two-nodes? [edge]
  (= (count edge) 2))

(def ^{:private true} edge-validator
  (s/constrained [TaskName] (fn [edge]
                              (and (= (count edge) 2)
                                   (vector? edge))) 'edge-two-nodes?))

(def Workflow
  (s/constrained [edge-validator] vector? 'vector?))

(def Language
  (apply s/enum (get-in i/model [:catalog-entry :model :onyx/language :choices])))

(def PosInt
  (s/constrained s/Int pos? 'pos?))

(def SPosInt
  (s/constrained s/Int (fn [v] (>= v 0)) 'spos?))

(defn build-allowed-key-ns [nspace]
  (s/pred (fn [k]
            (or (not (keyword? k))
                (not (= (name nspace)
                        (namespace k)))))
          'unsupported-key-combination))

(def UnsupportedTaskMapKey
  (build-allowed-key-ns :onyx))

(defn deprecated [key-seq]
  (s/pred
   (fn [_]
     (throw (ex-info (:deprecation-doc (get-in i/model key-seq)) {})))))

(def base-task-map
  {:onyx/name TaskName
   :onyx/type (apply s/enum (get-in i/model [:catalog-entry :model :onyx/type :choices]))
   :onyx/batch-size PosInt
   (s/optional-key :onyx/params) [s/Any]
   (s/optional-key :onyx/uniqueness-key) s/Any
   (s/optional-key :onyx/deduplicate?) s/Bool
   (s/optional-key :onyx/restart-pred-fn)
   (deprecated [:catalog-entry :model :onyx/restart-pred-fn])
   (s/optional-key :onyx/language) Language
   (s/optional-key :onyx/batch-timeout) SPosInt
   (s/optional-key :onyx/doc) s/Str
   (s/optional-key :onyx/bulk?) s/Bool
   (s/optional-key :onyx/max-peers) PosInt
   (s/optional-key :onyx/min-peers) PosInt
   (s/optional-key :onyx/n-peers) PosInt
   (s/optional-key :onyx/required-tags) [s/Keyword]
   UnsupportedTaskMapKey s/Any})

(def FluxPolicy
  (apply s/enum (get-in i/model [:catalog-entry :model :onyx/flux-policy :choices])))

(def FnPath
  (s/cond-pre NamespacedKeyword s/Keyword))

(def partial-grouping-task
  {(s/optional-key :onyx/group-by-key) s/Any
   (s/optional-key :onyx/group-by-fn) FnPath
   :onyx/flux-policy FluxPolicy})

(defn grouping-task? [task-map]
  (and (#{:function :output} (:onyx/type task-map))
       (or (not (nil? (:onyx/group-by-key task-map)))
           (not (nil? (:onyx/group-by-fn task-map))))))

(def partial-input-task
  {:onyx/plugin (s/cond-pre NamespacedKeyword s/Keyword)
   :onyx/medium s/Keyword
   :onyx/type (s/enum :input)
   (s/optional-key :onyx/fn) FnPath
   (s/optional-key :onyx/input-retry-timeout) PosInt
   (s/optional-key :onyx/pending-timeout) PosInt
   (s/optional-key :onyx/max-pending) PosInt})

(def partial-output-task
  {:onyx/plugin (s/cond-pre NamespacedKeyword s/Keyword)
   :onyx/medium s/Keyword
   :onyx/type (s/enum :output)
   (s/optional-key :onyx/fn) FnPath})

(def NonNamespacedKeyword
  (s/pred (fn [v]
            (and (keyword? v)
                 (not (namespace v))))
          'keyword-non-namespaced))

(def partial-java-plugin
  {:onyx/plugin NonNamespacedKeyword
   (s/optional-key :onyx/fn) FnPath})

(def partial-clojure-plugin
  {:onyx/plugin NamespacedKeyword
   (s/optional-key :onyx/fn) FnPath})

(def partial-fn-task
  {:onyx/fn (s/cond-pre NamespacedKeyword s/Keyword)
   (s/optional-key :onyx/plugin) (s/cond-pre NamespacedKeyword s/Keyword)})

(def partial-clojure-fn-task
  {:onyx/fn NamespacedKeyword})

(def partial-java-fn-task
  {:onyx/fn s/Keyword})

(defn java? [task-map]
  (= :java (:onyx/language task-map)))

(defn valid-min-peers-max-peers-n-peers? [entry]
  (case (:onyx/flux-policy entry)
    :continue
    (or (:onyx/n-peers entry)
        (:onyx/min-peers entry)
        (= (:onyx/max-peers entry) 1))
    :kill
    (or (:onyx/n-peers entry)
        (:onyx/min-peers entry)
        (= (:onyx/max-peers entry) 2))
    :recover
    (or (:onyx/n-peers entry)
        (and (:onyx/max-peers entry)
             (= (:onyx/max-peers entry)
                (:onyx/min-peers entry)))
        (= (:onyx/max-peers entry) 1))))

(def OutputTaskSchema
  (let [base-output-schema (merge base-task-map partial-output-task)
        base-output-grouping (merge base-output-schema partial-grouping-task)
        java-output-grouping (merge base-output-grouping partial-java-plugin)
        clojure-output-grouping (merge base-output-grouping partial-clojure-plugin)]
    (s/conditional grouping-task?
                   (s/conditional java?
                                  (s/constrained java-output-grouping
                                                 valid-min-peers-max-peers-n-peers?
                                                 'valid-flux-policy-min-max-n-peers)
                                  :else
                                  (s/constrained clojure-output-grouping
                                                 valid-min-peers-max-peers-n-peers?
                                                 'valid-flux-policy-min-max-n-peers))
                   :else
                   (s/conditional java?
                                  (merge base-output-schema partial-java-plugin)
                                  :else
                                  (merge base-output-schema partial-clojure-plugin)))))

(def InputTaskSchema
  (let [base-input-schema (merge base-task-map partial-input-task)]
    (s/conditional java?
                   (merge base-input-schema partial-java-plugin)
                   :else
                   base-input-schema)))

(def FunctionTaskSchema
  (let [base-function-task (merge base-task-map partial-fn-task)
        java-function-task (merge base-function-task partial-grouping-task partial-java-fn-task)
        clojure-function-task (merge base-function-task partial-grouping-task partial-clojure-fn-task)]
    (s/conditional grouping-task?
                   (s/conditional java?
                                  (s/constrained java-function-task
                                                 valid-min-peers-max-peers-n-peers?
                                                 'valid-flux-policy-min-max-n-peers)
                                  :else
                                  (s/constrained clojure-function-task
                                                 valid-min-peers-max-peers-n-peers?
                                                 'valid-flux-policy-min-max-n-peers))
                   :else
                   (s/conditional java?
                                  (merge base-function-task partial-java-fn-task)
                                  :else
                                  (merge base-function-task partial-clojure-fn-task)))))

(def TaskMap
  (s/conditional #(= (:onyx/type %) :input)
                 InputTaskSchema
                 #(= (:onyx/type %) :output)
                 OutputTaskSchema
                 #(= (:onyx/type %) :function)
                 FunctionTaskSchema))

(def Catalog
  [TaskMap])

(def Lifecycle
  {:lifecycle/task s/Keyword
   :lifecycle/calls NamespacedKeyword
   (s/optional-key :lifecycle/doc) s/Str
   s/Any s/Any})

(def LifecycleCall
  {(s/optional-key :lifecycle/doc) s/Str
   (s/optional-key :lifecycle/start-task?) Function
   (s/optional-key :lifecycle/before-task-start) Function
   (s/optional-key :lifecycle/before-batch) Function
   (s/optional-key :lifecycle/after-read-batch) Function
   (s/optional-key :lifecycle/after-batch) Function
   (s/optional-key :lifecycle/after-task-stop) Function
   (s/optional-key :lifecycle/after-ack-segment) Function
   (s/optional-key :lifecycle/after-retry-segment) Function
   (s/optional-key :lifecycle/handle-exception) Function})

(def FlowAction
  (s/enum :retry))

(def UnsupportedFlowKey
  (s/pred (fn [k]
            (or (not (keyword? k))
                (not (= "flow" (namespace k)))))
          'unsupported-flow-key))

(def FlowCondition
  {:flow/from TaskName
   :flow/to (s/cond-pre TaskName [TaskName])
   :flow/predicate (s/cond-pre s/Keyword [s/Any])
   (s/optional-key :flow/post-transform) NamespacedKeyword
   (s/optional-key :flow/thrown-exception?) s/Bool
   (s/optional-key :flow/action) FlowAction
   (s/optional-key :flow/short-circuit?) s/Bool
   (s/optional-key :flow/exclude-keys) [s/Keyword]
   (s/optional-key :flow/doc) s/Str
   UnsupportedFlowKey s/Any})

(def Unit
  [(s/one s/Int "unit-count")
   (s/one s/Keyword "unit-type")])

(def WindowType
  (apply s/enum (get-in i/model [:window-entry :model :window/type :choices])))

(def UnsupportedWindowKey
  (s/pred (fn [k]
            (or (not (keyword? k))
                (not (= "window" (namespace k)))))
          'unsupported-window-key))

(def WindowBase
  {:window/id s/Keyword
   :window/task TaskName
   :window/type WindowType
   :window/aggregation (s/cond-pre s/Keyword [s/Keyword])
   (s/optional-key :window/init) s/Any
   (s/optional-key :window/window-key) s/Any
   (s/optional-key :window/min-value) s/Int
   (s/optional-key :window/range) Unit
   (s/optional-key :window/slide) Unit
   (s/optional-key :window/timeout-gap) Unit
   (s/optional-key :window/session-key) s/Any
   (s/optional-key :window/doc) s/Str
   UnsupportedWindowKey s/Any})

(def Window
  (s/constrained 
    WindowBase
    (fn [v] (if (#{:fixed :sliding} (:window/type v))
              (:window/range v)
              true))
    ":window/range must be defined for :fixed or :sliding window"))

(def StateAggregationCall
  {(s/optional-key :aggregation/init) Function
   :aggregation/create-state-update Function
   :aggregation/apply-state-update Function
   (s/optional-key :aggregation/super-aggregation-fn) Function})

(def WindowExtension
  (s/constrained
   {:window Window
    :id s/Keyword
    :task TaskName
    :type WindowType
    :aggregation (s/cond-pre s/Keyword [s/Keyword])
    (s/optional-key :init) (s/maybe s/Any)
    (s/optional-key :window-key) (s/maybe s/Any)
    (s/optional-key :min-value) (s/maybe SPosInt)
    (s/optional-key :range) (s/maybe Unit)
    (s/optional-key :slide) (s/maybe Unit)
    (s/optional-key :timeout-gap) (s/maybe Unit)
    (s/optional-key :session-key) (s/maybe s/Any)
    (s/optional-key :doc) (s/maybe s/Str)}
   record? 'record?))

(def TriggerRefinement
  NamespacedKeyword)

(def TriggerPeriod
  (apply s/enum (get-in i/model [:trigger-entry :model :trigger/period :choices])))

(def TriggerThreshold
  (s/enum :elements :element))

(def UnsupportedTriggerKey
  (s/pred (fn [k]
            (or (not (keyword? k))
                (not (= "trigger" (namespace k)))))
          'unsupported-trigger-key))

(def TriggerPeriod
  [(s/one PosInt "trigger period") 
   (s/one TriggerPeriod "threshold type")])

(def TriggerThreshold 
  [(s/one PosInt "number elements") 
   (s/one TriggerThreshold "threshold type")])

(def Trigger
  {:trigger/window-id s/Keyword
   :trigger/refinement TriggerRefinement
   :trigger/on NamespacedKeyword
   :trigger/sync NamespacedKeyword
   (s/optional-key :trigger/fire-all-extents?) s/Bool
   (s/optional-key :trigger/pred) NamespacedKeyword
   (s/optional-key :trigger/watermark-percentage) double
   (s/optional-key :trigger/doc) s/Str
   (s/optional-key :trigger/period) TriggerPeriod
   (s/optional-key :trigger/threshold) TriggerThreshold
   (s/optional-key :trigger/id) s/Any
   UnsupportedTriggerKey s/Any})

(def RefinementCall
  {:refinement/create-state-update Function
   :refinement/apply-state-update Function})

(def TriggerCall
  {:trigger/init-state Function
   :trigger/next-state Function
   :trigger/trigger-fire? Function})

(def TriggerState
  (s/constrained
   {:window-id s/Keyword
    :refinement TriggerRefinement
    :on s/Keyword
    :sync s/Keyword
    :fire-all-extents? (s/maybe s/Bool)
    :pred (s/maybe s/Keyword)
    :watermark-percentage (s/maybe double)
    :doc (s/maybe s/Str)
    :period (s/maybe TriggerPeriod)
    :threshold (s/maybe TriggerThreshold)
    :sync-fn (s/maybe Function)
    :state s/Any
    :id s/Any
    :trigger Trigger
    :init-state Function
    :trigger-fire? Function
    :next-trigger-state Function
    :create-state-update Function
    :apply-state-update Function}
   record? 'record?))

(def Event 
  {s/Any s/Any})

(def PeerSchedulerEvent (apply s/enum i/peer-scheduler-event-types))

(defn type->schema [t]
  (let [l {:integer s/Int
           :boolean s/Bool
           :any s/Any
           :segment s/Any
           :window-entry Window
           :trigger-entry Trigger
           :event-map Event}]
    (if (sequential? t)
      (mapv l t)
      (l t))))

(def TriggerEventType (apply s/enum i/trigger-event-types))

(defn information-model->schema [model representation]
  (reduce (fn [m [k km]]
            (let [optional? (:optional? km)
                  schema-value (if-let [choices (:choices km)] 
                                 (apply s/enum choices)
                                 (type->schema (:type km)))]
              (case representation
                :record (assoc m 
                               k
                               (if optional? (s/maybe schema-value) schema-value))

                :map (assoc m 
                            (if (= optional? (s/optional-key k) k) 
                              schema-value)))))
          {}
          model))

(def StateEvent 
  (-> (:model (:state-event i/model))
      (information-model->schema :record)
      (assoc s/Any s/Any)))

(def WindowState
  (s/constrained
   {:window-extension WindowExtension
    :trigger-states [TriggerState]
    :window Window
    :state {s/Any s/Any}
    :state-event (s/maybe StateEvent)
    :event-results [StateEvent]
    :init-fn Function
    :create-state-update Function
    :apply-state-update Function
    :super-agg-fn (s/maybe Function)
    (s/optional-key :new-window-state-fn) Function
    (s/optional-key :grouping-fn) (s/cond-pre s/Keyword Function)}
   record? 'record?))

(def JobScheduler
  NamespacedKeyword)

(def TaskScheduler
  NamespacedKeyword)

(def JobMetadata
  {s/Keyword s/Any})

(def Job
  {:catalog Catalog
   :workflow Workflow
   :task-scheduler TaskScheduler
   (s/optional-key :percentage) s/Int
   (s/optional-key :flow-conditions) [FlowCondition]
   (s/optional-key :windows) [Window]
   (s/optional-key :triggers) [Trigger]
   (s/optional-key :lifecycles) [Lifecycle]
   (s/optional-key :metadata) JobMetadata
   (s/optional-key :acker/percentage) s/Int
   (s/optional-key :acker/exempt-input-tasks?) s/Bool
   (s/optional-key :acker/exempt-output-tasks?) s/Bool
   (s/optional-key :acker/exempt-tasks) [s/Keyword]})

(def TenancyId
  (s/cond-pre s/Uuid s/Str))

(def EnvConfig
  {:zookeeper/address s/Str
   (s/optional-key :onyx/id) (deprecated [:env-config :model :onyx/id])
   :onyx/tenancy-id TenancyId
   (s/optional-key :zookeeper/server?) s/Bool
   (s/optional-key :zookeeper.server/port) s/Int
   (s/optional-key :onyx.bookkeeper/server?) s/Bool
   (s/optional-key :onyx.bookkeeper/delete-server-data?) s/Bool
   (s/optional-key :onyx.bookkeeper/port) s/Int
   (s/optional-key :onyx.bookkeeper/local-quorum?) s/Bool
   (s/optional-key :onyx.bookkeeper/local-quorum-ports) [s/Int]
   (s/optional-key :onyx.bookkeeper/base-journal-dir) s/Str
   (s/optional-key :onyx.bookkeeper/base-ledger-dir) s/Str
   (s/optional-key :onyx.bookkeeper/disk-usage-threshold) (s/pred float?)
   (s/optional-key :onyx.bookkeeper/disk-usage-warn-threshold) (s/pred float?)
   s/Keyword s/Any})

(def AeronIdleStrategy
  (s/enum :busy-spin :low-restart-latency :high-restart-latency))

(def Messaging
  (s/enum :aeron :dummy-messenger))

(def StateLogImpl
  (s/enum :bookkeeper :none))

(def StateFilterImpl
  (s/enum :set :rocksdb))

(def PeerClientConfig
  {:zookeeper/address s/Str
   (s/optional-key :onyx/id) (deprecated [:env-config :model :onyx/id])
   :onyx/tenancy-id TenancyId
   s/Keyword s/Any})

(def PeerConfig
  {:zookeeper/address s/Str
   (s/optional-key :onyx/id) (deprecated [:env-config :model :onyx/id])
   :onyx/tenancy-id TenancyId
   :onyx.peer/job-scheduler JobScheduler
   :onyx.messaging/impl Messaging
   :onyx.messaging/bind-addr s/Str
   (s/optional-key :onyx.messaging/peer-port) s/Int
   (s/optional-key :onyx.messaging/external-addr) s/Str
   (s/optional-key :onyx.peer/inbox-capacity) s/Int
   (s/optional-key :onyx.peer/outbox-capacity) s/Int
   (s/optional-key :onyx.peer/retry-start-interval) s/Int
   (s/optional-key :onyx.peer/join-failure-back-off) s/Int
   (s/optional-key :onyx.peer/drained-back-off) s/Int
   (s/optional-key :onyx.peer/job-not-ready-back-off) s/Int
   (s/optional-key :onyx.peer/peer-not-ready-back-off) s/Int
   (s/optional-key :onyx.peer/fn-params) s/Any
   (s/optional-key :onyx.peer/backpressure-check-interval) s/Int
   (s/optional-key :onyx.peer/backpressure-low-water-pct) s/Int
   (s/optional-key :onyx.peer/backpressure-high-water-pct) s/Int
   (s/optional-key :onyx.peer/state-log-impl) StateLogImpl
   (s/optional-key :onyx.peer/state-filter-impl) StateFilterImpl
   (s/optional-key :onyx.peer/tags) [s/Keyword]
   (s/optional-key :onyx.peer/trigger-timer-resolution) PosInt
   (s/optional-key :onyx.bookkeeper/client-timeout) PosInt
   (s/optional-key :onyx.bookkeeper/client-throttle) PosInt
   (s/optional-key :onyx.bookkeeper/ledger-password) s/Str
   (s/optional-key :onyx.bookkeeper/ledger-id-written-back-off) PosInt
   (s/optional-key :onyx.bookkeeper/ledger-ensemble-size) PosInt
   (s/optional-key :onyx.bookkeeper/ledger-quorum-size) PosInt
   (s/optional-key :onyx.bookkeeper/write-batch-size) PosInt
   (s/optional-key :onyx.bookkeeper/write-buffer-size) PosInt
   (s/optional-key :onyx.bookkeeper/write-batch-backoff) PosInt
   (s/optional-key :onyx.bookkeeper/read-batch-size) PosInt
   (s/optional-key :onyx.rocksdb.filter/base-dir) s/Str
   (s/optional-key :onyx.rocksdb.filter/bloom-filter-bits) PosInt
   (s/optional-key :onyx.rocksdb.filter/compression) (s/enum :bzip2 :lz4 :lz4hc :none :snappy :zlib)
   (s/optional-key :onyx.rocksdb.filter/block-size) PosInt
   (s/optional-key :onyx.rocksdb.filter/peer-block-cache-size) PosInt
   (s/optional-key :onyx.rocksdb.filter/num-buckets) PosInt
   (s/optional-key :onyx.rocksdb.filter/num-ids-per-bucket) PosInt
   (s/optional-key :onyx.rocksdb.filter/rotation-check-interval-ms) PosInt
   (s/optional-key :onyx.zookeeper/backoff-base-sleep-time-ms) s/Int
   (s/optional-key :onyx.zookeeper/backoff-max-sleep-time-ms) s/Int
   (s/optional-key :onyx.zookeeper/backoff-max-retries) s/Int
   (s/optional-key :onyx.zookeeper/prepare-failure-detection-interval) s/Int
   (s/optional-key :onyx.messaging/inbound-buffer-size) s/Int
   (s/optional-key :onyx.messaging/completion-buffer-size) s/Int
   (s/optional-key :onyx.messaging/release-ch-buffer-size) s/Int
   (s/optional-key :onyx.messaging/retry-ch-buffer-size) s/Int
   (s/optional-key :onyx.messaging/peer-link-gc-interval) s/Int
   (s/optional-key :onyx.messaging/peer-link-idle-timeout) s/Int
   (s/optional-key :onyx.messaging/ack-daemon-timeout) s/Int
   (s/optional-key :onyx.messaging/ack-daemon-clear-interval) s/Int
   (s/optional-key :onyx.messaging/decompress-fn) Function
   (s/optional-key :onyx.messaging/compress-fn) Function
   (s/optional-key :onyx.messaging/allow-short-circuit?) s/Bool
   (s/optional-key :onyx.messaging.aeron/embedded-driver?) s/Bool
   (s/optional-key :onyx.messaging.aeron/embedded-media-driver-threading) (s/enum :dedicated :shared :shared-network)
   (s/optional-key :onyx.messaging.aeron/subscriber-count) s/Int
   (s/optional-key :onyx.messaging.aeron/write-buffer-size) s/Int
   (s/optional-key :onyx.messaging.aeron/poll-idle-strategy) AeronIdleStrategy 
   (s/optional-key :onyx.messaging.aeron/offer-idle-strategy) AeronIdleStrategy
   (s/optional-key :onyx.messaging.aeron/publication-creation-timeout) s/Int
   (s/optional-key :onyx.windowing/min-value) s/Int
   (s/optional-key :onyx.task-scheduler.colocated/only-send-local?) s/Bool
   s/Keyword s/Any})

(def PeerId
  (s/cond-pre s/Uuid s/Keyword))

(def PeerState
  (s/enum :idle :backpressure :active))

(def PeerSite
  {s/Any s/Any})

(def JobId
  (s/cond-pre s/Uuid s/Keyword))

(def TaskId
  (s/cond-pre s/Uuid s/Keyword))

(def TaskScheduler
  s/Keyword)

(def SlotId
  s/Int)

(def Replica
  {:job-scheduler JobScheduler
   :messaging {:onyx.messaging/impl Messaging s/Keyword s/Any}
   :peers [PeerId]
   :peer-state {PeerId PeerState}
   :peer-sites {PeerId PeerSite}
   :prepared {PeerId PeerId}
   :accepted {PeerId PeerId}
   :pairs {PeerId PeerId}
   :jobs [JobId]
   :task-schedulers {JobId TaskScheduler}
   :tasks {JobId [TaskId]}
   :allocations {JobId {TaskId [PeerId]}}
   :task-metadata {JobId {TaskId s/Any}}
   :saturation {JobId s/Num}
   :task-saturation {JobId {TaskId s/Num}}
   :flux-policies {JobId {TaskId s/Any}}
   :min-required-peers {JobId {TaskId s/Num}}
   :input-tasks {JobId [TaskId]}
   :output-tasks {JobId [TaskId]}
   :exempt-tasks  {JobId [TaskId]}
   :sealed-outputs {JobId #{TaskId}}
   :ackers {JobId [PeerId]} 
   :acker-percentage {JobId s/Int}
   :acker-exclude-inputs {TaskId s/Bool}
   :acker-exclude-outputs {TaskId s/Bool}
   :task-percentages {JobId {TaskId s/Num}}
   :percentages {JobId s/Num}
   :completed-jobs [JobId] 
   :killed-jobs [JobId] 
   :state-logs {JobId {TaskId {SlotId [s/Int]}}}
   :state-logs-marked #{s/Int}
   :task-slot-ids {JobId {TaskId {PeerId SlotId}}}
   :exhausted-inputs {JobId #{TaskId}}
   :required-tags {JobId {TaskId [s/Keyword]}}
   :peer-tags {PeerId [s/Keyword]}})

(def LogEntry
  {:fn s/Keyword
   :args {s/Any s/Any}
   (s/optional-key :message-id) s/Int
   (s/optional-key :created-at) s/Int
   (s/optional-key :peer-parent) s/Uuid
   (s/optional-key :entry-parent) s/Int})

(def Reactions
  (s/maybe [LogEntry]))

(def ReplicaDiff
  (s/maybe (s/cond-pre {s/Any s/Any} #{s/Any})))

(def State
  {s/Any s/Any})
