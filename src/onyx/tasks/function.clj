(ns onyx.tasks.function
  (:require [onyx.schema :as os]
            [schema.core :as s]))

(s/defn function
  [task-name :- s/Keyword opts]
  {:task {:task-map (merge {:onyx/name task-name
                            :onyx/type :function}
                           opts)}})
