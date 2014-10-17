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
  (schema/validate [(schema/pred map? 'map?)] catalog)
  (doseq [entry catalog] 
    (schema/validate catalog-entry-validator entry)))


