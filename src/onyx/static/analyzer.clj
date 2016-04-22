(ns ^{:no-doc true} onyx.static.analyzer
  (:require [clojure.walk :refer [prewalk]]
            [onyx.schema :as os]
            [onyx.static.path-seq :refer [path-seq]]
            [schema.core :as s]))

(defmulti classify-schema
  (fn [path schema]
    (type schema)))

(defmethod classify-schema schema.core.Predicate
  [path ve] (:pred-name ve))

(defmethod classify-schema schema.core.ConditionalSchema
  [path ve] (:error-symbol ve))

(defmulti classify-error
  (fn [path ve] (type (.schema ve))))

(defmethod classify-error schema.core.EnumSchema
  [path ve] {:error-type :value-choice-error})

(def pred-types
  {'integer? java.lang.Integer
   'keyword? clojure.lang.Keyword})

(defmethod classify-error schema.core.Predicate
  [path ve]
  (let [p (classify-schema path (.schema ve))]
    (if-let [t (pred-types p)]
      {:error-type :type-error
       :expected-type t
       :found-type (type (.value ve))
       :error-key (last path)
       :error-value (.value ve)}
      {:error-type :value-predicate-error
       :error-key (last path)
       :error-value (.value ve)
       :predicate p})))

(defmethod classify-error onyx.schema.RestrictedKwNamespace
  [path ve]
  {:error-type :invalid-key})

(defmethod classify-error schema.spec.variant.VariantSpec
  [path ve]
  {:error-type :conditional-failed
   :error-key (if (seq path) (last path) (first (keys (.value ve))))
   :predicates (map (partial classify-schema path)
                    (map :schema (:options (.schema ve))))})

(defmethod classify-error schema.core.Constrained
  [path ve]
  {:error-type :constraint-violated
   :predicate (:post-name (.schema ve))})

(defmethod classify-error clojure.lang.PersistentArrayMap
  [path ve]
  {:error-type :type-error
   :expected-type clojure.lang.PersistentArrayMap
   :found-type (type (.value ve))
   :error-key (last path)
   :error-value (.value ve)})

(defmethod classify-error clojure.lang.PersistentVector
  [path ve]
  {:error-type :type-error
   :expected-type clojure.lang.PersistentVector
   :found-type (type (.value ve))
   :error-key (last path)
   :error-value (.value ve)})

(defmethod classify-error java.lang.Class
  [path ve]
  {:error-type :type-error
   :expected-type (.schema ve)
   :found-type (type (.value ve))
   :error-key (last path)
   :error-value (.value ve)})

(defmethod classify-error :default
  [path ve]
  {:error-type :unknown})

(defn wrap-key [m x]
  (if-let [v (first x)]
    (assoc m :error-key v)
    m))

(defn analyze-error [t]
  (let [failures (->> (path-seq (:error (ex-data t)))
                      (filter :form))]
    (first
     (vals
      (reduce
       (fn [result {:keys [path form]}]
         (cond (= (type form) schema.utils.ValidationError)
               (assoc result path (classify-error path form))

               (= form 'missing-required-key)
               (assoc result path {:error-type :missing-required-key
                                   :missing-key (last path)})))
       {}
       failures)))))
