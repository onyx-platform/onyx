(ns onyx.static.validation
  (:require [clojure.walk :refer [prewalk]]
            [com.stuartsierra.dependency :as dep]
            [onyx.static.planning :as planning]
            [schema.core :as schema]
            [onyx.schema :refer [TaskMap Catalog Workflow Job LifecycleCall
                                 Lifecycle EnvConfig PeerConfig FlowCondition]]))

(defn task-dispatch-validator [task]
  (when (= (:onyx/name task)
           (:onyx/type task))
    (throw (Exception. (str "Task " (:onyx/name task)
                            " cannot use the same value for :onyx/name as :onyx/type.")))))

(defn name-and-type-not-equal [entry]
  (when (= (:onyx/name entry) (:onyx/type entry))
    (throw (ex-info "Task's :onyx/name and :onyx/type cannot be equal" {:task entry}))))

(defn no-duplicate-entries [catalog]
  (let [tasks (map :onyx/name catalog)]
    (when-not (= (distinct tasks) tasks)
      (throw (ex-info "Multiple catalog entries found with the same :onyx/name." {:catalog catalog})))))

(defn min-and-max-peers-sane [entry]
  (when (and (:onyx/min-peers entry)
             (:onyx/max-peers entry))
    (when-not (<= (:onyx/min-peers entry)
                  (:onyx/max-peers entry))
      (throw (ex-info ":onyx/min-peers must be <= :onyx/max-peers" {:entry entry})))))

(defn min-max-n-peers-mutually-exclusive [entry]
  (when (or (and (:onyx/min-peers entry) (:onyx/n-peers entry))
            (and (:onyx/max-peers entry) (:onyx/n-peers entry)))
    (throw (ex-info ":onyx/n-peers cannot be used with :onyx/min-peers or :onyx/max-peers" {:entry entry}))))

(defn validate-catalog
  [catalog]
  (no-duplicate-entries catalog)
  (schema/validate Catalog catalog)
  (doseq [entry catalog]
    (name-and-type-not-equal entry)
    (min-and-max-peers-sane entry)
    (min-max-n-peers-mutually-exclusive entry)))

(defn validate-workflow-names [{:keys [workflow catalog]}]
  (when-let [missing-names (->> workflow
                                (mapcat identity)
                                (remove (set (map :onyx/name catalog)))
                                seq)]
    (throw (Exception. (str "Catalog is missing :onyx/name values "
                            "for the following workflow keywords: "
                            (apply str (interpose ", " missing-names)))))))

(defn validate-workflow-no-dupes [{:keys [workflow]}]
  (when-not (= (count workflow)
               (count (set workflow)))
    (throw (ex-info "Workflows entries cannot contain duplicates"
                    {:workflow workflow}))))

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
  (validate-workflow-names job)
  (validate-workflow-no-dupes job))

(defn validate-lifecycles [lifecycles catalog]
  (doseq [lifecycle lifecycles]
    (when-not (or (= (:lifecycle/task lifecycle) :all)
                  (some #{(:lifecycle/task lifecycle)} (map :onyx/name catalog)))
      (throw (ex-info (str ":lifecycle/task must name a task in the catalog. It was: " (:lifecycle/task lifecycle))
                      {:lifecycle lifecycle :catalog catalog})))
    (schema/validate Lifecycle lifecycle)))

(defn validate-lifecycle-calls [m]
  (schema/validate LifecycleCall m))

(defn validate-env-config [env-config]
  (schema/validate EnvConfig env-config))

(defn validate-flow-structure [flow-conditions]
  (doseq [entry flow-conditions]
    (schema/validate FlowCondition entry)))

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

(defn validate-peer-config [peer-config]
  (schema/validate PeerConfig peer-config))

(defn validate-job
  [job]
  (schema/validate Job job)
  (validate-catalog (:catalog job))
  (validate-workflow job))

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

(defn validate-flow-conditions [flow-conditions workflow]
  (validate-flow-structure flow-conditions)
  (validate-flow-connections flow-conditions workflow)
  (validate-flow-pred-all-kws flow-conditions)
  (validate-all-position flow-conditions)
  (validate-none-position flow-conditions)
  (validate-short-circuit flow-conditions)
  (validate-auto-short-circuit flow-conditions))

(defn window-names-a-task [tasks w]
  (when-not (some #{(:window/task w)} tasks)
    (throw (ex-info ":window/task must name a task in the catalog" {:window w :tasks tasks}))))

(defn window-ids-unique [windows]
  (let [ids (map :window/id windows)]
    (when-not (= (count ids) (count (into #{} ids)))
      (throw (ex-info ":window/id must be unique across windows, found" {:ids ids})))))

(defn validate-windows [windows catalog]
  (let [task-names (map :onyx/name catalog)]
    (doseq [w windows]
      (window-names-a-task task-names w))
    (window-ids-unique windows)))

(defn coerce-uuid [uuid]
  (if (instance? java.util.UUID uuid)
    uuid
    (java.util.UUID/fromString uuid)))
