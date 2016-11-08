(ns onyx.tasks.null
  (:require [onyx.schema :as os]
            [onyx.plugin.null]
            [schema.core :as s]))

(s/defn output
  [task-name :- s/Keyword opts]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.null/output
                             :onyx/type :output
                             :onyx/medium :null
                             :onyx/doc "Writes segments to nowhere"}
                            opts)
           :lifecycles [{:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.null/in-calls}]}})
