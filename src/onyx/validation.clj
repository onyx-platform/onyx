(ns onyx.validation
  (:require [schema.core :as schema]
            [clojure.data.fressian :as fressian]))

(def base-catalog-entry-validator
  {:onyx/name schema/Keyword
   :onyx/type (schema/enum :input :output :function)
   :onyx/consumption (schema/enum :sequential :concurrent)
   :onyx/batch-size (schema/pred pos? 'pos?)
   schema/Keyword schema/Any})

(defn edge-two-nodes? [edge]
  (= (count edge) 2))

(def edge-validator
  (schema/->Both [(schema/pred vector? 'vector?) 
                  (schema/pred edge-two-nodes? 'edge-two-nodes?)
                  [schema/Keyword]]))

(def workflow-validator 
  (schema/->Both [(schema/pred vector? 'vector?)
                  [edge-validator]]))

(def catalog-entry-validator
  (schema/conditional #(or (= (:onyx/type %) :input) (= (:onyx/type %) :output))
                      (merge base-catalog-entry-validator {:onyx/medium schema/Keyword})
                      :else
                      (merge base-catalog-entry-validator {:onyx/fn schema/Keyword})))

(defn serializable? [x]
  (try
    (do (fressian/read (.array (fressian/write x)))
        true)
    (catch Exception e 
      false)))

(defn task-dispatch-validator [task]
  (when (= (:onyx/name task)
           (:onyx/type task))
    (throw (Exception. (str "Task " (:onyx/name task) 
                            " cannot use the same value for :onyx/name as :onyx/type.")))))

(defn validate-catalog
  [catalog]
  (when-not (serializable? catalog)
    (throw (Exception. "Catalog must be serializable.")))
  (doseq [entry catalog]
    (task-dispatch-validator entry)
    (schema/validate catalog-entry-validator entry)))

(defn validate-workflow-names [{:keys [workflow catalog]}]
  (when-let [missing-names (->> workflow
                                (mapcat identity)
                                (remove (set (map :onyx/name catalog)))
                                seq)]
    (throw (Exception. (str "Catalog is missing :onyx/name values "
                            "for the following workflow keywords: "
                            (apply str (interpose ", " missing-names)))))))


(defn input-tasks-output [{:keys [workflow catalog]}]
  (let [input-tasks (set (map :onyx/name 
                              (filter (fn [task] 
                                        (= :input (:onyx/type task)))
                                      catalog)))]
    (some input-tasks (map second workflow))))

(defn output-tasks-input [{:keys [workflow catalog]}]
  (let [output-tasks (set (map :onyx/name 
                               (filter (fn [task] 
                                        (= :output (:onyx/type task)))
                                      catalog)))]
    (some output-tasks (map first workflow))))

(defn validate-workflow-inputs [job]
  (when-let [task (input-tasks-output job)]
    (throw (Exception. (str "Input task " task " used as output.")))))

(defn validate-workflow-outputs [job]
  (when-let [task (output-tasks-input job)]
    (throw (Exception. (str "Output task " task " used as input.")))))

(def job-validator
  {:catalog [(schema/pred map? 'map?)]
   :workflow workflow-validator})

(defn validate-job
  [job]
  (schema/validate job-validator job)
  (validate-catalog (:catalog job))
  (validate-workflow-names job)
  (validate-workflow-inputs job)
  (validate-workflow-outputs job))
