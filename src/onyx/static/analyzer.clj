(ns ^{:no-doc true} onyx.static.analyzer
  (:require [clojure.walk :refer [prewalk]]
            [onyx.schema :as os]
            [onyx.static.path-seq :refer [path-seq]]
            [schema.core :as s]))

(def pred-types
  {'integer? java.lang.Integer
   'keyword? clojure.lang.Keyword})

(defn wrap-key [m x]
  (if-let [v (first x)]
    (assoc m :error-key v)
    m))

(defmulti constraint->error
  (fn [error-data]
    (:predicate error-data)))

(defmethod constraint->error 'range-defined-for-fixed-and-sliding?
  [error-data]
  {:error-type :multi-key-semantic-error
   :error-keys [:window/type :window/range :window/slide]
   :error-key :window/type
   :semantic-error :sliding-window-needs-range-and-slide
   :path (conj (:path error-data) :window/type)})

(defmethod constraint->error :default
  [error-data] error-data)

(defmulti classify-schema
  (fn [path schema]
    (type schema)))

(defmethod classify-schema schema.core.Predicate
  [path ve] (:pred-name ve))

(defmethod classify-schema schema.core.ConditionalSchema
  [path ve] (:error-symbol ve))

(defmethod classify-schema schema.core.EnumSchema
  [path ve] (:vs ve))

(defmethod classify-schema schema.core.AnythingSchema
  [path ve] 'anything?)

(defmethod classify-schema clojure.lang.PersistentVector
  [path ve]
  (map (partial classify-schema path) ve))

(defmulti classify-error
  (fn [job path ve] (type (.schema ve))))

(defmethod classify-error schema.core.EnumSchema
  [job path ve]
  {:error-type :value-choice-error
   :error-key (last path)
   :error-value (.value ve)
   :path path})

(defmethod classify-error schema.core.Predicate
  [job path ve]
  (let [p (classify-schema path (.schema ve))]
    (if-let [t (pred-types p)]
      (if (map? (get-in job (butlast path)))
        {:error-type :type-error
         :expected-type t
         :found-type (type (.value ve))
         :error-key (last path)
         :error-value (.value ve)
         :path path}
        {:error-type :type-error
         :expected-type t
         :found-type (type (.value ve))
         :error-key (last (butlast path))
         :error-value (.value ve)
         :path path})
      {:error-type :value-predicate-error
       :error-key (last path)
       :error-value (.value ve)
       :predicate p
       :path path})))

(defmethod classify-error onyx.schema.RestrictedKwNamespace
  [job path ve]
  {:error-type :invalid-key
   :path path})

(defmethod classify-error schema.spec.variant.VariantSpec
  [job path ve]
  {:error-type :conditional-failed
   :error-key (if (seq path) (last path) (first (keys (.value ve))))
   :error-value (.value ve)
   :predicates (map (partial classify-schema path)
                    (map :schema (:options (.schema ve))))
   :path path})

(defmethod classify-error schema.core.Constrained
  [job path ve]
  (constraint->error
   {:error-type :constraint-violated
    :predicate (:post-name (.schema ve))
    :path path}))

(defmethod classify-error clojure.lang.PersistentArrayMap
  [job path ve]
  {:error-type :type-error
   :expected-type clojure.lang.PersistentArrayMap
   :found-type (type (.value ve))
   :error-key (last path)
   :error-value (.value ve)
   :path path})

(defmethod classify-error clojure.lang.PersistentVector
  [job path ve]
  {:error-type :type-error
   :expected-type clojure.lang.PersistentVector
   :found-type (type (.value ve))
   :error-key (last path)
   :error-value (.value ve)
   :path path})

(defmethod classify-error java.lang.Class
  [job path ve]
  {:error-type :type-error
   :expected-type (.schema ve)
   :found-type (type (.value ve))
   :error-key (last path)
   :error-value (.value ve)
   :path path})

(defmethod classify-error :default
  [job path ve]
  {:error-type :unknown
   :path path})

(defn analyze-error [job t]
  (let [failures (->> (path-seq (:error (ex-data t)))
                      (filter :form))]
    (vals
     (reduce
      (fn [result {:keys [path form]}]
        (cond (= (type form) schema.utils.ValidationError)
              (assoc result path (classify-error job path form))

              (= (type form) schema.utils.NamedError)
              (assoc result path (classify-error job path (.-error form)))

              (= form 'missing-required-key)
              (assoc result path {:error-type :missing-required-key
                                  :path path
                                  :missing-key (last path)})

              (= form 'invalid-key)
              (assoc result path {:error-type :invalid-key
                                  :path path
                                  :error-key (.value (last path))})))
      {}
      failures))))
