(ns onyx.schema
  (:require [schema.core :as schema]))

(def NamespacedKeyword
  (schema/pred (fn [kw]
                 (and (keyword? kw)
                      (namespace kw)))
               'keyword-namespaced?))

(def Function
  (schema/pred fn? 'fn?))

(def TaskName
  (schema/pred (fn [v]
                 (and (not= :all v)
                      (not= :none v)
                      (keyword? v)))
               'task-name?))

(defn ^{:private true} edge-two-nodes? [edge]
  (= (count edge) 2))

(def ^{:private true} edge-validator
  (schema/->Both [(schema/pred vector? 'vector?)
                  (schema/pred edge-two-nodes? 'edge-two-nodes?)
                  [TaskName]]))

(def Workflow
  (schema/->Both [(schema/pred vector? 'vector?)
                  [edge-validator]]))

(def ^{:private true} base-task-map
  {:onyx/name TaskName
   :onyx/type (schema/enum :input :output :function)
   :onyx/batch-size (schema/pred pos? 'pos?)
   (schema/optional-key :onyx/restart-pred-fn) schema/Keyword
   (schema/optional-key :onyx/language) (schema/enum :java :clojure)
   (schema/optional-key :onyx/batch-timeout) (schema/pred pos? 'pos?)
   (schema/optional-key :onyx/doc) schema/Str
   schema/Keyword schema/Any})

(def FluxPolicy 
  (schema/enum :continue :kill))

(def ^{:private true} partial-grouping-task
  {(schema/optional-key :onyx/group-by-key) schema/Any
   (schema/optional-key :onyx/group-by-fn) Function
   :onyx/min-peers schema/Int
   :onyx/flux-policy FluxPolicy})

(defn grouping-task? [task-map]
  (and (= (:onyx/type task-map) :function)
       (or (not (nil? (:onyx/group-by-key task-map)))
           (not (nil? (:onyx/group-by-fn task-map))))))

(def ^{:private true} partial-input-output-task
  {:onyx/plugin NamespacedKeyword
   :onyx/medium schema/Keyword
   (schema/optional-key :onyx/fn) NamespacedKeyword})

(def ^{:private true} partial-fn-task
  {:onyx/fn NamespacedKeyword})

(def TaskMap
  (schema/conditional #(or (= (:onyx/type %) :input) (= (:onyx/type %) :output))
                      (merge base-task-map partial-input-output-task)
                      grouping-task?
                      (merge base-task-map partial-fn-task partial-grouping-task)
                      :else
                      (merge base-task-map partial-fn-task)))

(def Catalog
  [TaskMap])

(def Lifecycle
  {:lifecycle/task schema/Keyword
   :lifecycle/calls NamespacedKeyword
   (schema/optional-key :lifecycle/doc) schema/Str
   schema/Any schema/Any})

(def LifecycleCall
  {(schema/optional-key :lifecycle/doc) schema/Str
   (schema/optional-key :lifecycle/start-task?) Function
   (schema/optional-key :lifecycle/before-task-start) Function
   (schema/optional-key :lifecycle/before-batch) Function
   (schema/optional-key :lifecycle/after-batch) Function
   (schema/optional-key :lifecycle/after-task-stop) Function
   (schema/optional-key :lifecycle/after-ack-segment) Function
   (schema/optional-key :lifecycle/after-retry-segment) Function})

(def FlowCondition
  {:flow/from schema/Keyword
   :flow/to (schema/either schema/Keyword [schema/Keyword])
   (schema/optional-key :flow/short-circuit?) schema/Bool
   (schema/optional-key :flow/exclude-keys) [schema/Keyword]
   (schema/optional-key :flow/doc) schema/Str
   (schema/optional-key :flow/params) [schema/Keyword]
   :flow/predicate (schema/either schema/Keyword [schema/Any])
   schema/Keyword schema/Any})

(def Job
  {:catalog Catalog
   :workflow Workflow
   :task-scheduler schema/Keyword
   (schema/optional-key :percentage) schema/Int
   (schema/optional-key :flow-conditions) [FlowCondition]
   (schema/optional-key :lifecycles) [Lifecycle]
   (schema/optional-key :acker/percentage) schema/Int
   (schema/optional-key :acker/exempt-input-tasks?) schema/Bool
   (schema/optional-key :acker/exempt-output-tasks?) schema/Bool
   (schema/optional-key :acker/exempt-tasks) [schema/Keyword]})

(def ClusterId
  (schema/either schema/Uuid schema/Str))

(def EnvConfig
  {:zookeeper/address schema/Str
   :onyx/id ClusterId
   (schema/optional-key :zookeeper/server?) schema/Bool
   (schema/optional-key :zookeeper.server/port) schema/Int
   schema/Keyword schema/Any})


(def ^{:private true} PortRange
  [(schema/one schema/Int "port-range-start") 
   (schema/one schema/Int "port-range-end")])

(def AeronIdleStrategy
  (schema/enum :busy-spin :low-restart-latency :high-restart-latency))

(def JobScheduler
  schema/Keyword)

(def Messaging
  (schema/enum :aeron :netty :core.async :dummy-messenger))

(def PeerConfig
  {:zookeeper/address schema/Str
   :onyx/id ClusterId
   :onyx.peer/job-scheduler JobScheduler
   :onyx.messaging/impl Messaging
   :onyx.messaging/bind-addr schema/Str
   (schema/optional-key :onyx.messaging/peer-port-range) PortRange
   (schema/optional-key :onyx.messaging/peer-ports) [schema/Int]
   (schema/optional-key :onyx.messaging/external-addr) schema/Str
   (schema/optional-key :onyx.peer/inbox-capacity) schema/Int
   (schema/optional-key :onyx.peer/outbox-capacity) schema/Int
   (schema/optional-key :onyx.peer/retry-start-interval) schema/Int
   (schema/optional-key :onyx.peer/join-failure-back-off) schema/Int
   (schema/optional-key :onyx.peer/drained-back-off) schema/Int
   (schema/optional-key :onyx.peer/peer-not-ready-back-off) schema/Int
   (schema/optional-key :onyx.peer/job-not-ready-back-off) schema/Int
   (schema/optional-key :onyx.peer/fn-params) schema/Any
   (schema/optional-key :onyx.peer/backpressure-check-interval) schema/Int
   (schema/optional-key :onyx.peer/backpressure-low-water-pct) schema/Int
   (schema/optional-key :onyx.peer/backpressure-high-water-pct) schema/Int
   (schema/optional-key :onyx.zookeeper/backoff-base-sleep-time-ms) schema/Int
   (schema/optional-key :onyx.zookeeper/backoff-max-sleep-time-ms) schema/Int
   (schema/optional-key :onyx.zookeeper/backoff-max-retries) schema/Int
   (schema/optional-key :onyx.messaging/inbound-buffer-size) schema/Int
   (schema/optional-key :onyx.messaging/completion-buffer-size) schema/Int
   (schema/optional-key :onyx.messaging/release-ch-buffer-size) schema/Int
   (schema/optional-key :onyx.messaging/retry-ch-buffer-size) schema/Int
   (schema/optional-key :onyx.messaging/max-downstream-links) schema/Int
   (schema/optional-key :onyx.messaging/max-acker-links) schema/Int
   (schema/optional-key :onyx.messaging/peer-link-gc-interval) schema/Int
   (schema/optional-key :onyx.messaging/peer-link-idle-timeout) schema/Int
   (schema/optional-key :onyx.messaging/ack-daemon-timeout) schema/Int
   (schema/optional-key :onyx.messaging/ack-daemon-clear-interval) schema/Int
   (schema/optional-key :onyx.messaging/decompress-fn) Function
   (schema/optional-key :onyx.messaging/compress-fn) Function
   (schema/optional-key :onyx.messaging/allow-short-circuit?) schema/Bool
   (schema/optional-key :onyx.messaging.aeron/embedded-driver?) schema/Bool
   (schema/optional-key :onyx.messaging.aeron/subscriber-count) schema/Int
   (schema/optional-key :onyx.messaging.aeron/poll-idle-strategy) AeronIdleStrategy 
   (schema/optional-key :onyx.messaging.aeron/offer-idle-strategy) AeronIdleStrategy
   schema/Keyword schema/Any})

(def PeerId
  (schema/either schema/Uuid schema/Keyword))

(def PeerState
  (schema/enum :idle :backpressure :active))

(def PeerSite 
  {schema/Any schema/Any})

(def JobId
  (schema/either schema/Uuid schema/Keyword))

(def TaskId
  (schema/either schema/Uuid schema/Keyword))

(def TaskScheduler 
  schema/Keyword)

(def Replica
  {:job-scheduler JobScheduler
   :messaging {:onyx.messaging/impl Messaging
               schema/Keyword schema/Any}
   (schema/optional-key :peers) [PeerId]
   (schema/optional-key :peer-state) {PeerId PeerState}
   (schema/optional-key :peer-sites) {PeerId PeerSite}
   (schema/optional-key :prepared) {PeerId PeerId}
   (schema/optional-key :accepted) {PeerId PeerId}
   (schema/optional-key :pairs) {PeerId PeerId}
   (schema/optional-key :jobs) [JobId]
   (schema/optional-key :task-schedulers) {JobId TaskScheduler}
   (schema/optional-key :tasks) {JobId [TaskId]}
   (schema/optional-key :allocations) {JobId {TaskId [PeerId]}}
   (schema/optional-key :saturation) {JobId schema/Num}
   (schema/optional-key :task-saturation) {JobId {TaskId schema/Num}}
   (schema/optional-key :flux-policies) {JobId {TaskId schema/Any}}
   (schema/optional-key :min-required-peers) {JobId {TaskId schema/Num}}
   (schema/optional-key :input-tasks) {JobId [TaskId]}
   (schema/optional-key :output-tasks) {JobId [TaskId]}
   (schema/optional-key :exempt-tasks)  {JobId [TaskId]}
   (schema/optional-key :sealed-outputs) {JobId [TaskId]}
   (schema/optional-key :ackers) {JobId [PeerId]} 
   (schema/optional-key :acker-percentage) {JobId schema/Int}
   (schema/optional-key :acker-exclude-inputs) {TaskId schema/Bool}
   (schema/optional-key :acker-exclude-outputs) {TaskId schema/Bool}
   (schema/optional-key :completed-jobs) [JobId] 
   (schema/optional-key :killed-jobs) [JobId] 
   (schema/optional-key :exhausted-inputs) {JobId #{TaskId}}})

(def LogEntry
  {:fn schema/Keyword
   :args {schema/Any schema/Any}
   (schema/optional-key :immediate?) schema/Bool
   (schema/optional-key :message-id) schema/Int
   (schema/optional-key :created-at) schema/Int})

(def Reactions 
  (schema/maybe [LogEntry]))
