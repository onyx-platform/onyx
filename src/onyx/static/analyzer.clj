(ns ^{:no-doc true} onyx.static.analyzer
  (:require [clojure.walk :refer [prewalk]]
            [onyx.schema :as os]
            [schema.core :as s]))

(defmulti classify-schema
  (fn [schema]
    (type schema)))

(defmethod classify-schema schema.core.Predicate
  [ve] (:pred-name ve))

(defmethod classify-schema schema.core.ConditionalSchema
  [ve] (:error-symbol ve))

(defmulti classify-error
  (fn [ve] (type (.schema ve))))

(defmethod classify-error schema.core.EnumSchema
  [ve] {:error-type :value-choice-error})

(def pred-types
  {'integer? java.lang.Integer
   'keyword? clojure.lang.Keyword})

(defmethod classify-error schema.core.Predicate
  [ve]
  (let [p (classify-schema (.schema ve))]
    (if-let [t (pred-types p)]
      {:error-type :type-error
       :expected-type t
       :found-type (type (.value ve))
       :error-value (.value ve)}
      {:error-type :value-predicate-error
       :error-value (.value ve)
       :predicate p})))

(defmethod classify-error onyx.schema.RestrictedKwNamespace
  [ve]
  {:error-type :invalid-key})

(defmethod classify-error schema.spec.variant.VariantSpec
  [ve]
  {:error-type :conditional-failed
   :predicates (map classify-schema (map :schema (:options (.schema ve))))})

(defmethod classify-error schema.core.Constrained
  [ve]
  {:error-type :constraint-violated
   :predicate (:post-name (.schema ve))})

(defmethod classify-error clojure.lang.PersistentArrayMap
  [ve]
  {:error-type :type-error
   :expected-type (type (.schema ve))
   :found-type (type (.value ve))})

(defmethod classify-error clojure.lang.PersistentVector
  [ve]
  {:error-type :type-error
   :expected-type clojure.lang.PersistentVector
   :found-type (type (.value ve))
   :error-value (.value ve)})

(defmethod classify-error java.lang.Class
  [ve]
  {:error-type :type-error
   :expected-type (.schema ve)
   :found-type (type (.value ve))
   :error-value (.value ve)})

(defmethod classify-error :default
  [ve]
  {:error-type :unknown})

(defn wrap-key [m x]
  (if-let [v (first x)]
    (assoc m :error-key v)
    m))

(defn analyze-error [t]
  (let [fails (atom [])]
    (prewalk
     (fn [x]
       (cond (= (type x) schema.utils.ValidationError)
             (swap! fails conj (classify-error x))

             (and (vector? x) (= (type (second x)) schema.utils.ValidationError))
             (swap! fails conj (wrap-key (classify-error (second x)) x))

             (and (vector? x) (= (second x) 'missing-required-key))
             (swap! fails conj {:error-type :missing-required-key
                                :missing-key (first x)}))
       x)
     (:error (ex-data t)))
    (first @fails)))

;; (defn describe-job-schema-error [t]
;;   (let [error (:error (ex-data t))]
;;     (assert (map? error))
;;     (cond (:workflow error)
;;           (let [ve-or-seq (first (:workflow error))]
;;             (if (coll? ve-or-seq)
;;               (let [[left-err right-err] ve-or-seq
;;                     choice (or left-err right-err)]
;;                 {:error (classify-schema-error choice)
;;                  :error-value (.value choice)})
;;               (do (prn (type (first (.schema ve-or-seq))))
;;                   {:error (classify-schema-error ve-or-seq)
;;                    :error-value (.value ve-or-seq)}))))))

;; (defn describe-schema-error [t]
;;   (let [error-type (type (:error (ex-data t)))]
;;     (cond (map? (:error (ex-data t)))

;;           (let [[k v] (first (:error (ex-data t)))
;;                 x (if (instance? schema.utils.ValidationError k) k v)]
;;             {:error (classify-schema-error x)
;;              :key k
;;              :value v
;;              :error-value (if (instance? schema.utils.ValidationError x) (.value x) k)})

;;           (instance? schema.utils.ValidationError (:error (ex-data t)))
;;           (let [error (:error (ex-data t))]
;;             {:error (classify-schema-error error)})

;;           :else (throw t))))


