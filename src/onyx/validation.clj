(ns onyx.validation
  (:require [clojure.data.fressian :as fressian]
            [com.stuartsierra.dependency :as dep]
            [onyx.planning :as planning]
            [schema.core :as schema]))

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
    (do (fressian/write x)
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
  (doseq [entry catalog]
    (schema/validate catalog-entry-validator entry)))

(defn validate-workflow-names [{:keys [workflow catalog]}]
  (when-let [missing-names (->> workflow
                                (mapcat identity)
                                (remove (set (map :onyx/name catalog)))
                                seq)]
    (throw (Exception. (str "Catalog is missing :onyx/name values "
                            "for the following workflow keywords: "
                            (apply str (interpose ", " missing-names)))))))



(defn validate-workflow-names [{:keys [workflow catalog]}]
  (when-let [missing-names (->> workflow
                                (mapcat identity)
                                (remove (set (map :onyx/name catalog)))
                                seq)]
    (throw (Exception. (str "Catalog is missing :onyx/name values "
                            "for the following workflow keywords: "
                            (apply str (interpose ", " missing-names)))))))

(defn catalog->type-task-names [catalog type-pred]
  (set (map :onyx/name
            (filter (fn [task]
                      (type-pred (:onyx/type task)))
                    catalog))))

(defn validate-workflow-inputs [g input-tasks]
  (when-let [invalid (ffirst (filter (comp seq second)
                                     (map (juxt identity
                                                (partial dep/immediate-dependencies g))
                                          input-tasks)))]
    (throw (Exception. (str "Input task " invalid " has incoming edge.")))))

(defn validate-workflow-outputs [g output-tasks]
  (when-let [invalid (ffirst (filter (comp seq second)
                                     (map (juxt identity
                                                (partial dep/immediate-dependents g))
                                          output-tasks)))]
    (throw (Exception. (str "Output task " invalid " has outgoing edge.")))))

(defn validate-workflow-intermediates [g intermediate-tasks]
  (let [invalid-intermediate? (fn [[_ dependencies dependents]]
                                (let [dependencies? (empty? dependencies)
                                      dependents? (empty? dependents)]
                                  (or (and dependencies? (not dependents?))
                                      (and (not dependencies?) dependents?))))]
    (when-let [invalid (ffirst (filter invalid-intermediate?
                                       (map (juxt identity
                                                  (partial dep/immediate-dependencies g)
                                                  (partial dep/immediate-dependents g))
                                            intermediate-tasks)))]
      (throw (Exception. (str "Intermediate task " invalid " requires both incoming and outgoing edges."))))))

(defn validate-workflow-graph [{:keys [catalog workflow]}]
  (let [g (planning/to-dependency-graph workflow)]
    (validate-workflow-intermediates g (catalog->type-task-names catalog #{:function}))
    (validate-workflow-inputs g (catalog->type-task-names catalog #{:input}))
    (validate-workflow-outputs g (catalog->type-task-names catalog #{:output}))))

(defn validate-workflow [job]
  (validate-workflow-graph job)
  (validate-workflow-names job))

(def job-validator
  {:catalog [(schema/pred map? 'map?)]
   :workflow workflow-validator
   :task-scheduler schema/Keyword
   (schema/optional-key :percentage) schema/Int})

(defn validate-job
  [job]
  (schema/validate job-validator job)
  (validate-catalog (:catalog job))
  (validate-workflow job))

