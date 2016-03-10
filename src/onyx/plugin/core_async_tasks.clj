(ns onyx.plugin.core-async-tasks
  (:require [clojure.core.async :refer [chan]]
            [onyx.schema :as os]
            [onyx.plugin.core-async]
            [schema.core :as s]))

(def default-channel-size 1000)

(s/defn input-task
  ([task-name :- s/Keyword opts]
   (input-task task-name opts default-channel-size))
  ([task-name :- s/Keyword opts chan-size]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.core-async/input
                             :onyx/type :input
                             :onyx/medium :core.async
                             :onyx/max-peers 1
                             :onyx/doc "Reads segments from a core.async channel"}
                            opts)
           :lifecycles [{:lifecycle/task task-name
                         :core.async/id (java.util.UUID/randomUUID)
                         :core.async/size chan-size
                         :lifecycle/calls :onyx.plugin.core-async/in-calls}
                        {:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.core-async/reader-calls}]}
    :schema {:task-map os/TaskMap
             :lifecycles [os/Lifecycle]}}))

(s/defn output-task
  ([task-name :- s/Keyword opts]
   (output-task task-name opts default-channel-size))
  ([task-name :- s/Keyword opts chan-size]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.core-async/output
                             :onyx/type :output
                             :onyx/medium :core.async
                             :onyx/max-peers 1
                             :onyx/doc "Writes segments to a core.async channel"}
                            opts)
           :lifecycles [{:lifecycle/task task-name
                         :core.async/id (java.util.UUID/randomUUID)
                         :core.async/size (inc chan-size)
                         :lifecycle/calls :onyx.plugin.core-async/out-calls}
                        {:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.core-async/writer-calls}]}
    :schema {:task-map os/TaskMap
             :lifecycles [os/Lifecycle]}}))
