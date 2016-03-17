(ns onyx.static.validation
  (:require [clojure.walk :refer [prewalk]]
            [com.stuartsierra.dependency :as dep]
            [schema.core :as schema]
            [onyx.static.planning :as planning]
            [onyx.windowing.units :as u]
            [onyx.information-model :refer [model]]
            [onyx.schema :refer [TaskMap Catalog Workflow Job LifecycleCall StateAggregationCall
                                 RefinementCall TriggerCall Lifecycle EnvConfig PeerConfig PeerClientConfig FlowCondition]]))

(defn validate-java-version []
  (let [version (System/getProperty "java.runtime.version")] 
    (when-not (pos? (.compareTo version "1.8.0"))
      (throw (ex-info "Onyx is only supported when running on Java 8 or later." 
                      {:version version})))))

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

(defn describe-cause [k]
  (if (= schema.utils.ValidationError (type k))
    (cond (= (os/restricted-ns :onyx) (.schema k))
          (if-let [doc (dissoc (get-in model [:catalog-entry :model (.value k)]) :doc)]
            {:cause "Unsupported combination of task-map keys."
             :key (.value k)
             :documentation doc}
            {:cause "Unsupported onyx task-map key."
             :key (.value k)})
          :else
          k)
    k))

(defn describe-value [k v]
  (if (= schema.utils.ValidationError (type v))
    (let [entry (get-in model [:catalog-entry :model k])]
      (if-let [deprecation-doc (:deprecation-doc entry)]
        {:cause deprecation-doc
         :data {k (.value v)}
         :deprecated-version (:deprecated-version entry)}
        (if-let [doc (dissoc entry :doc)]
          {:cause "Unsupported value"
           :data {k (.value v)}
           :documentation doc}
          {:cause "Unsupported value"
           :data {k (.value v)}
           :value (.value v)})))
    v)) 

(defn improve-issue [m]
  (into {}
        (mapv (fn [[k v]]
                [(describe-cause k)
                 (describe-value k v)]) 
              m)))

(defn task-map-schema-exception->help [e]
  (let [{:keys [type schema value error] :as exd} (ex-data e)
        schema-data (:data exd)]
    (case type
      :schema.core/error (improve-issue error)
      e)))

(defn validate-catalog
  [catalog]
  (no-duplicate-entries catalog)
  (doseq [entry catalog]
    (name-and-type-not-equal entry)
    (min-and-max-peers-sane entry)
    (min-max-n-peers-mutually-exclusive entry)
    (try 
      (schema/validate TaskMap entry)
      (catch Exception e
        (let [friendly-exception (try (task-map-schema-exception->help e)
                                      (catch Exception fe 
                                        ;; Throw original exception. We have obviously messed up providing a friendlier one
                                        (throw e)))]
          (throw (ex-info (format "Task %s failed validation. Error: %s" (:onyx/name entry) friendly-exception)
                          {:explanation friendly-exception})))))))

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

(defn validate-state-aggregation-calls [m]
  (schema/validate StateAggregationCall m))

(defn validate-refinement-calls [m]
  (schema/validate RefinementCall m))

(defn validate-trigger-calls [m]
  (schema/validate TriggerCall m))

(defn validate-env-config [env-config]
  (schema/validate EnvConfig env-config))

(defn validate-flow-structure [flow-conditions]
  (doseq [entry flow-conditions]
    (schema/validate FlowCondition entry)))

(defn validate-flow-connections [flow-schema workflow]
  (let [task->egress-edges (reduce (fn [m [from to]]
                                     (update m from (fn [v]
                                                      (conj (set v) to))))
                                   {}
                                   workflow)
        all-tasks (into (set (map first workflow)) (map second workflow))]
    (doseq [{:keys [flow/from flow/to] :as entry} flow-schema]
      (when-not (or (all-tasks from) (= from :all))
        (throw (ex-info ":flow/from value doesn't name a node in the workflow"
                        {:entry entry})))

      (when-not (or (= :all to)
                    (= :none to)
                    (and (= from :all)
                         (empty? (remove all-tasks to)))
                    (and (coll? to) 
                         (every? (fn [t] ((task->egress-edges from) t)) to)))
        (throw (ex-info ":flow/to value doesn't name a valid connected task in the workflow, :all, or :none"
                        {:entry entry}))))))

(defn validate-peer-config [peer-config]
  (schema/validate PeerConfig peer-config))

(defn validate-peer-client-config [peer-client-config]
  (schema/validate PeerClientConfig peer-client-config))

(defn validate-job
  [job]
  (validate-catalog (:catalog job))
  (validate-workflow job)
  (schema/validate Job job))

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

(defn range-and-slide-units-compatible [w]
  (when (and (:window/range w) (:window/slide w))
    (when-not (= (u/standard-units-for (second (:window/range w)))
                 (u/standard-units-for (second (:window/slide w))))
      (throw (ex-info "Incompatible units for :window/range and :window/slide" {:window w})))))

(defn sliding-windows-define-range-and-slide [w]
  (when (= (:window/type w) :sliding)
    (when (or (not (:window/range w)) (not (:window/slide w)))
      (throw (ex-info ":sliding windows must define both :window/range and :window/slide" {:window w})))))

(defn fixed-windows-dont-define-slide [w]
  (when (and (= (:window/type w) :fixed) (:window/slide w))
    (throw (ex-info ":fixed windows do not define a :window/slide value" {:window w}))))

(defn global-windows-dont-define-range-or-slide [w]
  (when (and (= (:window/type w) :global) (:window/range w))
    (throw (ex-info ":global windows do not define a :window/range value" {:window w})))

  (when (and (= (:window/type w) :global) (:window/slide w))
    (throw (ex-info ":global windows do not define a :window/slide value" {:window w}))))

(defn session-windows-dont-define-range-or-slide [w]
  (when (and (= (:window/type w) :session) (:window/range w))
    (throw (ex-info ":session windows do not define a :window/range value" {:window w})))

  (when (and (= (:window/type w) :session) (:window/slide w))
    (throw (ex-info ":session windows do not define a :window/slide value" {:window w}))))

(defn session-windows-define-a-timeout [w]
  (when (and (= (:window/type w) :session) (not (:window/timeout-gap w)))
    (throw (ex-info ":session windows must define a :window/timeout-gap value" {:window w}))))

(defn window-key-where-required [w]
  (let [t (:window/type w)]
    (when (and (some #{t} #{:fixed :sliding :session})
               (not (:window/window-key w)))
      (throw (ex-info (format "Window type %s requires a :window/window-key" t) {:window w})))))

(defn task-has-uniqueness-key [w catalog]
  (let [t (planning/find-task catalog (:window/task w))
        deduplicate? (and (:onyx/uniqueness-key t)
                          (or (true? (:onyx/deduplicate? t))
                              (nil? (:onyx/deduplicate? t))))
        no-deduplicate? (and (nil? (:onyx/uniqueness-key t))
                            (false? (:onyx/deduplicate? t)))
        valid-combo? (not (and deduplicate? no-deduplicate?))]
    (when-not valid-combo?
      (throw (ex-info
               (format "Task %s is windowed, and therefore define :onyx/uniqueness-key, or do not define :onyx/uniqueness-key and use :onyx/deduplicate? false."
                       (:onyx/name t))
               {:task t})))))

(defn validate-windows [windows catalog]
  (let [task-names (map :onyx/name catalog)]
    (window-ids-unique windows)
    (doseq [w windows]
      (window-names-a-task task-names w)
      (range-and-slide-units-compatible w)
      (sliding-windows-define-range-and-slide w)
      (fixed-windows-dont-define-slide w)
      (global-windows-dont-define-range-or-slide w)
      (session-windows-dont-define-range-or-slide w)
      (session-windows-define-a-timeout w)
      (window-key-where-required w)
      (task-has-uniqueness-key w catalog))))

(defn trigger-names-a-window [window-ids t]
  (when-not (some #{(:trigger/window-id t)} window-ids)
    (throw (ex-info "Trigger must name a window ID" {:trigger t :window-ids window-ids}))))

(defn validate-triggers [triggers windows]
  (let [window-names (map :window/id windows)]
    (doseq [t triggers]
      (trigger-names-a-window window-names t))))

(defn coerce-uuid [uuid]
  (try 
    (if (instance? java.util.UUID uuid)
        uuid
        (java.util.UUID/fromString uuid))
    (catch Throwable t
      (throw (ex-info (format "Argument must be a UUID or string UUID. Type was %s" (type uuid))
                      {:type (type uuid)
                       :value uuid})))))
