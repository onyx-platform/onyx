(ns onyx.static.validation
  (:require [clojure.walk :refer [prewalk]]
            [com.stuartsierra.dependency :as dep]
            [onyx.static.planning :as planning]
            [onyx.compression.nippy :refer [compress decompress]]
            [schema.core :as schema]))

(def base-catalog-entry-validator
  {:onyx/name schema/Keyword
   :onyx/type (schema/enum :input :output :function)
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
    (do (compress x)
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
    (validate-workflow-inputs g (catalog->type-task-names catalog #{:input}))))

(defn validate-workflow [job]
  (validate-workflow-graph job)
  (validate-workflow-names job))

(def job-validator
  {:catalog [(schema/pred map? 'map?)]
   :workflow workflow-validator
   :task-scheduler schema/Keyword
   (schema/optional-key :percentage) schema/Int
   (schema/optional-key :flow-conditions) schema/Any
   (schema/optional-key :acker/percentage) schema/Int
   (schema/optional-key :acker/exempt-input-tasks?) schema/Bool
   (schema/optional-key :acker/exempt-output-tasks?) schema/Bool
   (schema/optional-key :acker/exempt-tasks) [schema/Keyword]})

(defn validate-job
  [job]
  (schema/validate job-validator job)
  (validate-catalog (:catalog job))
  (validate-workflow job))

(defn validate-flow-structure [flow-schema]
  (doseq [entry flow-schema]
    (let [entry (select-keys entry
                             [:flow/from :flow/to :flow/short-circuit?
                              :flow/exclude-keys :flow/doc :flow/params
                              :flow/predicate])]
      (schema/validate
       {:flow/from schema/Keyword
        :flow/to (schema/either schema/Keyword [schema/Keyword])
        (schema/optional-key :flow/short-circuit?) schema/Bool
        (schema/optional-key :flow/exclude-keys) [schema/Keyword]
        (schema/optional-key :flow/doc) schema/Str
        (schema/optional-key :flow/params) [schema/Keyword]
        :flow/predicate (schema/either schema/Keyword [schema/Any])}
       entry))))

(defn validate-flow-connections [flow-schema workflow]
  (let [all (into #{} (concat (map first workflow) (map second workflow)))]
    (doseq [entry flow-schema]
      (let [from (:flow/from entry)]
        (when-not (some #{from} all)
          (throw (ex-info ":flow/from value doesn't name a node in the workflow"
                          {:entry entry}))))

      (let [to (:flow/to entry)]
        (when-not (or (= :all to)
                      (= :none to)
                      (clojure.set/subset? to all))
          (throw (ex-info ":flow/to value doesn't name a node in the workflow, :all, or :none"
                          {:entry entry})))))))

(defn validate-flow-pred-all-kws [flow-schema]
  (prewalk
   (fn [x]
     (when-not (or (keyword? x) (coll? x) (nil? x))
       (throw (ex-info "Token in :flow/predicate was not a keyword or collection" {:token x})))
     x)
   (:flow/predicate (last flow-schema))))

(defn validate-all-position [flow-schema]
  (let [flow-nodes (into #{} (map :flow/from flow-schema))]
    (doseq [node flow-nodes]
      (doseq [entry (rest (filter #(= node (:flow/from %)) flow-schema))]
        (when (= :all (:flow/to entry))
          (throw (ex-info ":flow/to mapped to :all value must appear first flow ordering" {:entry entry})))))))

(defn using-all-clause? [flow-schema]
  (seq (filter #(= :all (:flow/to %)) flow-schema)))

(defn validate-none-position [flow-schema]
  (let [flow-nodes (into #{} (map :flow/from flow-schema))]
    (doseq [node flow-nodes]
      (let [entries (filter #(= node (:flow/from %)) flow-schema)]
        (let [entries (if (using-all-clause? entries)
                        (rest (rest entries))
                        (rest entries))]
          (doseq [entry entries]
            (when (= :none (:flow/to entry))
              (throw (ex-info ":flow/to mapped to :none value must exactly proceed :all entry" {:entry entry})))))))))

(defn validate-short-circuit [flow-schema]
  (let [flow-nodes (into #{} (map :flow/from flow-schema))]
    (doseq [node flow-nodes]
      (let [entries (filter #(= node (:flow/from %)) flow-schema)
            chunks (partition-by true? (map :flow/short-circuit? entries))]
        (when (or (> (count chunks) 2)
                  (seq (filter identity (apply concat (rest chunks)))))
          (throw (ex-info ":flow/short-circuit entries must proceed all entries that aren't :flow/short-circuit"
                          {:entry entries})))))))

(defn validate-auto-short-circuit [flow-schema]
  (doseq [entry flow-schema]
    (when (and (or (= (:flow/to entry) :all)
                   (= (:flow/to entry) :none))
               (not (:flow/short-circuit? entry)))
      (throw (ex-info ":flow/to :all and :none require :flow/short-circuit? to be true"
                      {:entry entry})))))

(defn validate-flow-conditions [flow-conditions-schema workflow]
  (validate-flow-structure flow-conditions-schema)
  (validate-flow-connections flow-conditions-schema workflow)
  (validate-flow-pred-all-kws flow-conditions-schema)
  (validate-all-position flow-conditions-schema)
  (validate-none-position flow-conditions-schema)
  (validate-short-circuit flow-conditions-schema)
  (validate-auto-short-circuit flow-conditions-schema))

