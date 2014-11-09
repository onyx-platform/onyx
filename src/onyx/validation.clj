(ns onyx.validation
  (:require [schema.core :as schema]
            [clojure.data.fressian :as fressian]))

(def base-catalog-entry-validator
  {:onyx/name schema/Keyword
   :onyx/type (schema/enum :input :output :function)
   :onyx/consumption (schema/enum :sequential :concurrent)
   :onyx/batch-size (schema/pred pos? 'pos?)
   schema/Keyword schema/Any})

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

(defn validate-catalog
  [catalog]
  (when-not (serializable? catalog)
    (throw (Exception. "Catalog must be serializable.")))
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

(def job-validator
  {:catalog [(schema/pred map? 'map?)]
   :workflow (schema/pred vector? 'vector?)})

(defn validate-job
  [job]
  (schema/validate job-validator job)
  (validate-catalog (:catalog job))
  (validate-workflow-names job))

