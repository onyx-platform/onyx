(ns ^{:no-doc true} onyx.static.analyzer
  (:require [clojure.walk :refer [prewalk]]
            [onyx.schema :as os]
            [onyx.static.path-seq :refer [path-seq]]
            [schema.core :as s]))

(def pred-types
  {'integer? #?(:clj java.lang.Integer
                :cljs js/Number)
   'keyword? #?(:clj clojure.lang.Keyword
                :cljs cljs.core/Keyword)})

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
  [path ^schema.utils.ValidationError ve] (:pred-name ve))

(defmethod classify-schema schema.core.ConditionalSchema
  [path ^schema.utils.ValidationError ve] (:error-symbol ve))

(defmethod classify-schema schema.core.EnumSchema
  [path ^schema.utils.ValidationError ve] (:vs ve))

(defmethod classify-schema schema.core.AnythingSchema
  [path ^schema.utils.ValidationError ve] 'anything?)

(defmethod classify-schema schema.core.CondPre
  [path ^schema.utils.ValidationError ^schema.utils.ValidationError ve]
  (map (partial classify-schema path) (:schemas ve)))

(defmethod classify-schema #?(:clj clojure.lang.PersistentVector
                              :cljs cljs.core/PersistentVector)
  [path ^schema.utils.ValidationError ve]
  (map (partial classify-schema path) ve))

(defmethod classify-schema #?(:clj clojure.lang.PersistentHashMap
                              :cljs cljs.core/PersistentArrayMap)
  [path ^schema.utils.ValidationError ve]
  (map (partial classify-schema path) (vals ve)))

(defmethod classify-schema schema.core.Constrained
  [path ^schema.utils.ValidationError ve] (:post-name ve))

(defmethod classify-schema #?(:clj java.lang.Class
                              :cljs js/Object)
  [path ^schema.utils.ValidationError ve] 'type-error)

(defmethod classify-schema :default
  [path ^schema.utils.ValidationError ve]
  (throw (ex-info "Unhandled schema classification case" {:validation-error ve})))

(defmulti classify-error
  (fn [job path ^schema.utils.ValidationError ve] (type (.schema ve))))

(defmethod classify-error schema.core.EnumSchema
  [job path ^schema.utils.ValidationError ve]
  {:error-type :value-choice-error
   :error-key (last path)
   :error-value (.value ve)
   :choices (:vs (.schema ve))
   :path path})

(defmethod classify-error schema.core.Predicate
  [job path ^schema.utils.ValidationError ve]
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
  [job path ^schema.utils.ValidationError ve]
  {:error-type :invalid-key
   :path path})

(defn determine-predicates [path ^schema.utils.ValidationError ve]
  (let [x (first @(.-expectation-delay ve))]
    (if (= x 'matches-some-precondition?)
      (map (partial classify-schema path)
           (map :schema (:options (.schema ve))))
      [x])))

(defmethod classify-error schema.spec.variant.VariantSpec
  [job path ^schema.utils.ValidationError ve]
  {:error-type :conditional-failed
   :error-key (if (seq path) (last path) (first (keys (.value ve))))
   :error-value (.value ve)
   :predicates (determine-predicates path ve)
   :path path})

(defmethod classify-error schema.core.Constrained
  [job path ^schema.utils.ValidationError ve]
  (constraint->error
   {:error-type :constraint-violated
    :predicate (:post-name (.schema ve))
    :path path}))

(defmethod classify-error #?(:clj java.util.Map
                             :cljs cljs.core/PersistentArrayMap)
  [job path ^schema.utils.ValidationError ve]
  {:error-type :type-error
   :expected-type #?(:clj java.util.Map
                     :cljs cljs.core/PersistentArrayMap)
   :found-type (type (.value ve))
   :error-key (last path)
   :error-value (.value ve)
   :path path})

(defmethod classify-error #?(:clj java.util.List
                             :cljs cljs.core/PersistentVector)
  [job path ^schema.utils.ValidationError ve]
  {:error-type :type-error
   :expected-type #?(:clj java.util.List
                     :cljs cljs.core/PersistentVector)
   :found-type (type (.value ve))
   :error-key (last path)
   :error-value (.value ve)
   :path path})

(defmethod classify-error #?(:clj java.lang.Object
                             :cljs js/Object)
  [job path ^schema.utils.ValidationError ve]
  {:error-type :type-error
   :expected-type (.schema ve)
   :found-type (type (.value ve))
   :error-key (last path)
   :error-value (.value ve)
   :path path})

(defmethod classify-error :default
  [job path ^schema.utils.ValidationError ve]
  {:error-type :unknown
   :path path})

(defn analyze-error [job t]
  (let [failures (->> (path-seq (:error (ex-data t)))
                      (filter :form))]
    (first
     (vals
      (reduce
       (fn [result {:keys [path form]}]
         (cond (= (type form) schema.utils.ValidationError)
               (assoc result path (classify-error job path form))

               (= (type form) schema.utils.NamedError)
               (assoc result path (classify-error job path (.-error ^schema.utils.NamedError form)))

               (= form 'missing-required-key)
               (assoc result path {:error-type :missing-required-key
                                   :path path
                                   :missing-key (last path)})

               (= form 'disallowed-key)
               (assoc result path {:error-type :disallowed-key
                                   :path path
                                   :disallowed-key (last path)})

               (= form 'invalid-key)
               (assoc result path {:error-type :invalid-key
                                   :path path
                                   :error-key (.value ^schema.utils.ValidationError (last path))})

               :else
               (throw (ex-info "Unhandled error analyzer case" {:form form}))))
       {}
       failures)))))
