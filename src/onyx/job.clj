(ns onyx.job
  (:require [schema.core :as s]
            [onyx.schema :as os]))

(def base-schemas
  {:task-map os/TaskMap
   :lifecycles [os/Lifecycle]
   :triggers [os/Trigger]
   :windows [os/Window]
   :flow-conditions [os/FlowCondition]})

(defn vector-map-merge [base-schema schema]
  (println (keys (merge (first schema) (first base-schema))))
  [(merge (first schema) (first base-schema))])

(defn compose-schemas [{:keys [task schema]} base-schema]
  (let [{:keys [task-map lifecycles
                triggers windows
                flow-conditions]} schema]
    (-> schema
        (update :task-map os/UniqueTaskMap)
        (update :lifecycles vector-map-merge      (:lifecycles base-schema))
        (update :triggers vector-map-merge        (:triggers base-schema))
        (update :windows vector-map-merge         (:windows base-schema))
        (update :flow-conditions vector-map-merge (:flow-conditions base-schema))
        (select-keys (keys task)))))

(s/defn ^:always-validate add-task :- os/Job
  "Adds a task's task-definition to a job"
  ([job task-definition & behaviors]
   (add-task job (reduce (fn [acc f] (f acc))task-definition behaviors)))
  ([{:keys [lifecycles triggers windows flow-conditions] :as job}
    {:keys [task schema] :as task-definition}]
   (let [composed-schema (compose-schemas task-definition base-schemas)]
     (s/validate composed-schema task)
     (cond-> job
       true (update :catalog conj (:task-map task))
       lifecycles (update :lifecycles into (:lifecycles task))
       triggers (update :triggers into (:triggers task))
       windows (update :windows into (:windows task))
       flow-conditions (update :flow-conditions into (:flow-conditions task))))))
