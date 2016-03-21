(ns onyx.job
  (:require [schema.core :as s]
            [onyx.schema :as os]))

(def base-schemas
  {:task-map os/TaskMap
   :lifecycles [os/Lifecycle]
   :triggers [os/Trigger]
   :windows [os/Window]
   :flow-conditions [os/FlowCondition]})

(s/defn ^:always-validate add-task :- os/Job
  "Adds a task's task-definition to a job"
  [{:keys [task-map lifecycles triggers windows flow-conditions] :as job}
   {:keys [task schema] :as task-definition}]
  (merge-with s/validate schema task)
  (merge-with s/validate base-schemas task)
  (cond-> job
    task-map (update :catalog conj (:task-map task))
    lifecycles (update :lifecycles into (:lifecycles task))
    triggers (update :triggers into (:triggers task))
    windows (update :windows into (:windows task))
    flow-conditions (update :flow-conditions into (:flow-conditions task))))
