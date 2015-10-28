(ns onyx.schema
  (:require [schema.core :as s]))

(def NamespacedKeyword
  (s/pred (fn [kw]
                 (and (keyword? kw)
                      (namespace kw)))
               'keyword-namespaced?))

(def Function
  (s/pred fn? 'fn?))

(def TaskName
  (s/pred (fn [v]
                 (and (not= :all v)
                      (not= :none v)
                      (keyword? v)))
               'task-name?))

(defn ^{:private true} edge-two-nodes? [edge]
  (= (count edge) 2))

(def ^{:private true} edge-validator
  (s/->Both [(s/pred vector? 'vector?)
                  (s/pred edge-two-nodes? 'edge-two-nodes?)
                  [TaskName]]))

(def Workflow
  (s/->Both [(s/pred vector? 'vector?)
                  [edge-validator]]))

(def Language
  (s/enum :java :clojure))

(def ^{:private true} base-task-map
  {:onyx/name TaskName
   :onyx/type (s/enum :input :output :function)
   :onyx/batch-size (s/pred pos? 'pos?)
   (s/optional-key :onyx/uniqueness-key) s/Any
   (s/optional-key :onyx/restart-pred-fn) s/Keyword
   (s/optional-key :onyx/language) Language
   (s/optional-key :onyx/batch-timeout) (s/pred pos? 'pos?)
   (s/optional-key :onyx/doc) s/Str
   (s/optional-key :onyx/max-peers) (s/pred pos? 'pos?)
   s/Keyword s/Any})

(def FluxPolicy 
  (s/enum :continue :kill :recover))

(def ^{:private true} partial-grouping-task
  {(s/optional-key :onyx/group-by-key) s/Any
   (s/optional-key :onyx/group-by-fn) NamespacedKeyword
   :onyx/min-peers s/Int
   :onyx/flux-policy FluxPolicy})

(defn grouping-task? [task-map]
  (and (#{:function :output} (:onyx/type task-map))
       (or (not (nil? (:onyx/group-by-key task-map)))
           (not (nil? (:onyx/group-by-fn task-map))))))

(def ^{:private true} partial-input-output-task
  {:onyx/plugin NamespacedKeyword
   :onyx/medium s/Keyword
   (s/optional-key :onyx/fn) NamespacedKeyword})

(def ^{:private true} partial-fn-task
  {:onyx/fn NamespacedKeyword})

(def TaskMap
  (s/conditional #(or (= (:onyx/type %) :input) (= (:onyx/type %) :output))
                      (merge base-task-map partial-input-output-task)
                      grouping-task?
                      (merge base-task-map partial-fn-task partial-grouping-task)
                      #(= (:onyx/type %) :function)
                      (merge base-task-map partial-fn-task)))

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
   (s/optional-key :lifecycle/after-retry-segment) Function})

(def FlowCondition
  {:flow/from s/Keyword
   :flow/to (s/either s/Keyword [s/Keyword])
   (s/optional-key :flow/short-circuit?) s/Bool
   (s/optional-key :flow/exclude-keys) [s/Keyword]
   (s/optional-key :flow/doc) s/Str
   (s/optional-key :flow/params) [s/Keyword]
   :flow/predicate (s/either s/Keyword [s/Any])
   s/Keyword s/Any})

(def Unit
  [(s/one s/Int "unit-count")
   (s/one s/Keyword "unit-type")])

(def WindowType
  (s/enum :fixed :sliding :global :session))

(def Window
  {:window/id s/Keyword
   :window/task s/Keyword
   :window/type WindowType
   :window/window-key s/Any
   :window/aggregation (s/either s/Keyword [s/Keyword])
   (s/optional-key :window/range) Unit
   (s/optional-key :window/slide) Unit
   (s/optional-key :window/timeout-gap) Unit
   (s/optional-key :window/session-key) s/Keyword
   (s/optional-key :window/doc) s/Str
   s/Keyword s/Any})

(def TriggerRefinement
  (s/enum :accumulating :discarding))

(def Trigger
  {:trigger/window-id s/Keyword
   :trigger/refinement TriggerRefinement
   :trigger/on s/Keyword
   :trigger/sync s/Keyword
   (s/optional-key :trigger/fire-all-extents?) s/Bool
   (s/optional-key :trigger/doc) s/Str
   s/Keyword s/Any})

(def JobScheduler
  s/Keyword)

(def TaskScheduler
  s/Keyword)

(def Job
  {:catalog Catalog
   :workflow Workflow
   :task-scheduler TaskScheduler
   (s/optional-key :percentage) s/Int
   (s/optional-key :flow-conditions) [FlowCondition]
   (s/optional-key :windows) [Window]
   (s/optional-key :triggers) [Trigger]
   (s/optional-key :lifecycles) [Lifecycle]
   (s/optional-key :acker/percentage) s/Int
   (s/optional-key :acker/exempt-input-tasks?) s/Bool
   (s/optional-key :acker/exempt-output-tasks?) s/Bool
   (s/optional-key :acker/exempt-tasks) [s/Keyword]})

(def ClusterId
  (s/either s/Uuid s/Str))

(def EnvConfig
  {:zookeeper/address s/Str
   :onyx/id ClusterId
   (s/optional-key :zookeeper/server?) s/Bool
   (s/optional-key :zookeeper.server/port) s/Int
   (s/optional-key :bookkeeper/server?) s/Bool
   (s/optional-key :bookkeeper/local-quorum?) s/Bool
   (s/optional-key :bookkeeper/base-dir) s/Str
   s/Keyword s/Any})

(def AeronIdleStrategy
  (s/enum :busy-spin :low-restart-latency :high-restart-latency))

(def Messaging
  (s/enum :aeron :dummy-messenger))

(def StateLogImpl
  (s/enum :bookkeeper :none))

(def StateFilterImpl
  (s/enum :set :rocksdb))

(def PeerConfig
  {:zookeeper/address s/Str
   :onyx/id ClusterId
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
   (s/optional-key :onyx.bookkeeper/server?) s/Bool
   (s/optional-key :onyx.bookkeeper/port) s/Int
   (s/optional-key :onyx.bookkeeper/local-quorum?) s/Bool
   (s/optional-key :onyx.bookkeeper/local-quorum-ports) [s/Int]
   (s/optional-key :onyx.bookkeeper/base-dir) s/Str
   (s/optional-key :onyx.bookkeeper/client-timeout) s/Int
   (s/optional-key :onyx.bookkeeper/client-throttle) s/Int
   (s/optional-key :onyx.bookkeeper/ledger-password) s/Str
   (s/optional-key :onyx.bookkeeper/ledger-id-written-back-off) s/Int
   (s/optional-key :onyx.bookkeeper/ledger-ensemble-size) s/Int
   (s/optional-key :onyx.bookkeeper/ledger-quorum-size) s/Int
   (s/optional-key :onyx.zookeeper/backoff-base-sleep-time-ms) s/Int
   (s/optional-key :onyx.zookeeper/backoff-max-sleep-time-ms) s/Int
   (s/optional-key :onyx.zookeeper/backoff-max-retries) s/Int
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
   (s/optional-key :onyx.messaging.aeron/subscriber-count) s/Int
   (s/optional-key :onyx.messaging.aeron/write-buffer-size) s/Int
   (s/optional-key :onyx.messaging.aeron/poll-idle-strategy) AeronIdleStrategy 
   (s/optional-key :onyx.messaging.aeron/offer-idle-strategy) AeronIdleStrategy
   s/Keyword s/Any})

(def PeerId
  (s/either s/Uuid s/Keyword))

(def PeerState
  (s/enum :idle :backpressure :active))

(def PeerSite 
  {s/Any s/Any})

(def JobId
  (s/either s/Uuid s/Keyword))

(def TaskId
  (s/either s/Uuid s/Keyword))

(def TaskScheduler 
  s/Keyword)

(def Replica
  {:job-scheduler JobScheduler
   :messaging {:onyx.messaging/impl Messaging
               s/Keyword s/Any}
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
   :task-slot-ids {JobId {TaskId {PeerId s/Int}}}
   :exhausted-inputs {JobId #{TaskId}}})

(def LogEntry
  {:fn s/Keyword
   :args {s/Any s/Any}
   (s/optional-key :message-id) s/Int
   (s/optional-key :created-at) s/Int})

(def Reactions 
  (s/maybe [LogEntry]))

(def ReplicaDiff
  (s/maybe (s/either {s/Any s/Any} #{s/Any})))

(def State
  {s/Any s/Any})
