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

(def ^{:private true} partial-grouping-task
  {(schema/optional-key :onyx/group-by-key) schema/Any
   (schema/optional-key :onyx/group-by-fn) schema/Keyword
   :onyx/min-peers schema/Int
   :onyx/flux-policy (schema/enum :continue :kill)})

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

(def Job
  {:catalog [TaskMap]
   :workflow Workflow
   :task-scheduler schema/Keyword
   (schema/optional-key :percentage) schema/Int
   (schema/optional-key :flow-conditions) schema/Any
   (schema/optional-key :lifecycles) schema/Any
   (schema/optional-key :acker/percentage) schema/Int
   (schema/optional-key :acker/exempt-input-tasks?) schema/Bool
   (schema/optional-key :acker/exempt-output-tasks?) schema/Bool
   (schema/optional-key :acker/exempt-tasks) [schema/Keyword]})

(def Lifecycle
  {:lifecycle/task schema/Keyword
   :lifecycle/calls NamespacedKeyword
   (schema/optional-key :lifecycle/doc) String
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

(def DeploymentId
  (schema/either schema/Uuid schema/Str))

(def EnvConfig
  {:zookeeper/address schema/Str
   :onyx/id DeploymentId
   (schema/optional-key :zookeeper/server?) schema/Bool
   (schema/optional-key :zookeeper.server/port) schema/Int
   schema/Keyword schema/Any})

(def PeerConfig
  {:zookeeper/address schema/Str
   :onyx/id DeploymentId
   :onyx.peer/job-scheduler schema/Keyword
   :onyx.messaging/impl (schema/enum :aeron :netty :core.async :dummy-messenger)
   :onyx.messaging/bind-addr schema/Str
   (schema/optional-key :onyx.messaging/peer-port-range) [schema/Int]
   (schema/optional-key :onyx.messaging/peer-ports) [schema/Int]
   (schema/optional-key :onyx.messaging/external-addr) schema/Str
   (schema/optional-key :onyx.messaging/backpressure-strategy) schema/Keyword
   schema/Keyword schema/Any})

(def Flow
  {:flow/from schema/Keyword
   :flow/to (schema/either schema/Keyword [schema/Keyword])
   (schema/optional-key :flow/short-circuit?) schema/Bool
   (schema/optional-key :flow/exclude-keys) [schema/Keyword]
   (schema/optional-key :flow/doc) schema/Str
   (schema/optional-key :flow/params) [schema/Keyword]
   :flow/predicate (schema/either schema/Keyword [schema/Any])
   schema/Keyword schema/Any})
