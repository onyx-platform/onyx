(ns onyx.tasks.null
  (:require [onyx.schema :as os]
            [schema.core :as s]))

(s/defn output
  "Creates a no-op output plugin, requires the inclusion of
   the onyx.test-helper namespace at startup"
  [task-name :- s/Keyword opts]
  {:task {:task-map (merge {:onyx/name task-name
                            :onyx/plugin :onyx.test-helper/dummy-output
                            :onyx/type :output
                            :onyx/medium :null
                            :onyx/doc "Writes segments to nowhere"} opts)
          :lifecycles []}})
