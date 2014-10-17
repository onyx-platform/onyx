(ns onyx.validation
  (:require [schema.core :as schema]))

(def base-catalog-entry-validator
  {:onyx/name schema/Keyword
   :onyx/type (schema/enum :input :output :transformer :grouper :aggregator)
   :onyx/consumption (schema/enum :sequential :concurrent)
   :onyx/batch-size (schema/pred pos? 'pos?)
   schema/Keyword schema/Any})

(def catalog-entry-validator
  (schema/conditional #(or (= (:onyx/type %) :input) (= (:onyx/type %) :output))
                      (merge base-catalog-entry-validator {:onyx/medium schema/Keyword})
                      :else
                      (merge base-catalog-entry-validator {:onyx/fn schema/Keyword})))

(defn validate-catalog
  [catalog]
  (doseq [entry catalog]
    (schema/validate catalog-entry-validator entry)))

(defn flatten-workflow [workflow]
  (concat (keys workflow)
          (mapcat (fn [v]
                    (if (map? v)
                      (flatten-workflow v)
                      (list v)))
            (vals workflow))))

(defn validate-workflow-names [{:keys [workflow catalog]}]
  (when-let [missing-names (->> workflow
                                flatten-workflow
                                (remove (set (map :onyx/name catalog)))
                                seq)]
    (throw (Exception. (str "Catalog is missing :onyx/name values "
                            "for the following workflow keywords: "
                            (apply str (interpose ", " missing-names)))))))

(def job-validator
  {:catalog [(schema/pred map? 'map?)]
   :workflow (schema/pred map? 'map?)})

(defn validate-job
  [job]
  (schema/validate job-validator job)
  (validate-catalog (:catalog job))
  (validate-workflow-names job))
