(ns onyx.job
  (:require [schema.core :as s]
            [onyx.schema :as os]))

(def base-schemas
  {:task-map os/TaskMap
   :lifecycles [os/Lifecycle]
   :triggers [os/Trigger]
   :windows [os/Window]
   :flow-conditions [os/FlowCondition]
   :resume-point os/ResumePoint})

(defn vschema-merge [schema base-schema]
  [(os/combine-restricted-ns (merge (first schema) (or (:schema (first base-schema))
                                                       (first base-schema))))])

(defn compose-schemas [{:keys [task schema]} base-schema]
  (let [{:keys [task-map lifecycles
                triggers windows
                flow-conditions]} schema]
    (-> schema
        (update :task-map os/UniqueTaskMap)
        (update :lifecycles vschema-merge (:lifecycles base-schema))
        (update :triggers vschema-merge (:triggers base-schema))
        (update :windows vschema-merge (:windows base-schema))
        (update :flow-conditions vschema-merge (:flow-conditions base-schema))
        (update :job-metadata vschema-merge (:job-metadata base-schema))
        (update :resume-point vschema-merge (:resume-point base-schema))
        (select-keys (keys task)))))

(s/defn ^:always-validate add-task :- os/PartialJob
  "Adds a task's task-definition to a job"
  ([job task-definition & behaviors]
   (add-task job (reduce (fn [acc f] (f acc)) task-definition behaviors)))
  ([job task-bundle]
   (let [{:keys [task schema]} task-bundle
         {:keys [task-map lifecycles triggers windows flow-conditions]} task
         composed-schema (compose-schemas task-bundle base-schemas)]
     (s/validate composed-schema task)
     (cond-> job
       task-map (update :catalog conj task-map)
       lifecycles (update :lifecycles into lifecycles)
       triggers (update :triggers into triggers)
       windows (update :windows into windows)
       flow-conditions (update :flow-conditions into flow-conditions)))))

(defmulti register-job (fn [job-name config] job-name))
