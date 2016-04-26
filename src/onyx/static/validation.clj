(ns onyx.static.validation
  (:require [clojure.walk :refer [prewalk]]
            [com.stuartsierra.dependency :as dep]
            [schema.core :as schema]
            [onyx.static.planning :as planning]
            [onyx.windowing.units :as u]
            [onyx.information-model :refer [model]]
            [onyx.static.helpful-job-errors :as hje]
            [onyx.static.analyzer :as a]
            [onyx.schema :refer [TaskMap Catalog Workflow Job LifecycleCall StateAggregationCall
                                 RefinementCall TriggerCall Lifecycle EnvConfig PeerConfig PeerClientConfig FlowCondition] :as os]))

(defn find-dupes [coll]
  (map key (remove (comp #{1} val) (frequencies coll))))

(defn print-schema-errors! [job t]
  (doseq [{:keys [error-type error-value path] :as data} (a/analyze-error job t)]
    (hje/print-helpful-job-error job data (get-in job (butlast path)) (first path))
    (println)))

(defn validate-java-version []
  (let [version (System/getProperty "java.runtime.version")]
    (when-not (pos? (.compareTo version "1.8.0"))
      (throw (ex-info "Onyx is only supported when running on Java 8 or later."
                      {:version version})))))

(defn no-duplicate-entries [{:keys [catalog :as job]}]
  (let [tasks (map :onyx/name catalog)
        duplicates (find-dupes tasks)]
    (when (seq duplicates)
      (let [data {:error-type :duplicate-entry-error
                  :error-key :onyx/name
                  :error-value (first duplicates)
                  :path [:catalog]}]
        (hje/print-helpful-job-error job data catalog :catalog))
      (throw (ex-info "Multiple catalog entries found with the same :onyx/name." {:catalog catalog})))))

(defn min-and-max-peers-sane [job entry]
  (when (and (:onyx/min-peers entry)
             (:onyx/max-peers entry))
    (when-not (<= (:onyx/min-peers entry)
                  (:onyx/max-peers entry))
      (let [data {:error-type :multi-key-semantic-error
                  :error-keys [:onyx/min-peers :onyx/max-peers]
                  :error-key :onyx/min-peers
                  :error-value [(:onyx/min-peers entry) (:onyx/max-peers entry)]
                  :semantic-error :min-peers-gt-max-peers
                  :path [:catalog]}]
        (hje/print-helpful-job-error job data entry :catalog))
      (throw (ex-info ":onyx/min-peers must be <= :onyx/max-peers" {:entry entry})))))

(defn min-max-n-peers-mutually-exclusive [job entry]
  (when (or (and (:onyx/min-peers entry) (:onyx/n-peers entry))
            (and (:onyx/max-peers entry) (:onyx/n-peers entry)))
    (let [data {:error-type :multi-key-semantic-error
                :error-keys [:onyx/min-peers :onyx/max-peers :onyx/n-peers]
                :error-key :onyx/n-peers
                :error-value [(:onyx/min-peers entry) (:onyx/max-peers entry) (:onyx/n-peers entry)]
                :semantic-error :n-peers-with-min-or-max
                :path [:catalog]}]
      (hje/print-helpful-job-error job data entry :catalog))
    (throw (ex-info ":onyx/n-peers cannot be used with :onyx/min-peers or :onyx/max-peers" {:entry entry}))))

(defn validate-catalog
  [{:keys [catalog] :as job}]
  (no-duplicate-entries job)

  (doseq [entry catalog]
    (min-and-max-peers-sane job entry)
    (min-max-n-peers-mutually-exclusive job entry)
    (try
      (schema/validate TaskMap entry)
      (catch Throwable t
        (print-schema-errors! job t)
        (throw t)))))

(defn validate-workflow-names [{:keys [workflow catalog]}]
  (when-let [missing-names (->> workflow
                                (mapcat identity)
                                (remove (set (map :onyx/name catalog)))
                                seq)]
    (hje/print-workflow-element-error
     workflow
     (first missing-names)
     (fn [faulty-key]
       (str "Task " (pr-str faulty-key) " wasn't found in the catalog.")))
    (throw (Exception. (str "Catalog is missing :onyx/name values "
                            "for the following workflow keywords: "
                            (apply str (interpose ", " missing-names)))))))

(defn validate-workflow-no-dupes [{:keys [workflow]}]
  (let [dupes (find-dupes workflow)]
    (when (seq dupes)
      (hje/print-workflow-edge-error workflow (first dupes)
                                     (constantly "Workflow entries cannot contain duplicates"))
      (throw (ex-info "Workflow entries cannot contain duplicates"
                      {:workflow workflow})))))

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

(defn validate-workflow-intermediates [workflow g intermediate-tasks]
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
      (hje/print-workflow-element-error
       workflow
       invalid
       (fn [faulty-key]
         (str "Intermediate task " (pr-str faulty-key) " requires both incoming and outgoing edges.")))
      (throw (Exception. (str "Intermediate task " invalid " requires both incoming and outgoing edges."))))))

(defn validate-workflow-graph [{:keys [catalog workflow]}]
  (let [g (planning/to-dependency-graph workflow)]
    (validate-workflow-intermediates workflow g (catalog->type-task-names catalog #{:function}))
    (validate-workflow-inputs g (catalog->type-task-names catalog #{:input}))))

(defn validate-workflow [job]
  (validate-workflow-names job)
  (validate-workflow-graph job)
  (validate-workflow-no-dupes job))

(defn validate-lifecycles [{:keys [lifecycles catalog] :as job}]
  (doseq [lifecycle lifecycles]
    (try
      (schema/validate Lifecycle lifecycle)
      (catch Throwable t
        (print-schema-errors! job t)
        (throw t)))

    (when-not (or (= (:lifecycle/task lifecycle) :all)
                  (some #{(:lifecycle/task lifecycle)} (map :onyx/name catalog)))
      (hje/print-invalid-task-name-error
       lifecycle :lifecycle/task
       (:lifecycle/task lifecycle)
       :lifecycles
       (map :onyx/name catalog))
      (throw (ex-info (str ":lifecycle/task must name a task in the catalog. It was: " (:lifecycle/task lifecycle))
                      {:lifecycle lifecycle :catalog catalog})))))

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

(defn validate-flow-structure [{:keys [flow-conditions] :as job}]
  (try
    (doseq [entry flow-conditions]
      (schema/validate FlowCondition entry))
    (catch Throwable t
      (print-schema-errors! job t))))

(defn validate-flow-connections [{:keys [workflow flow-conditions] :as job}]
  (let [task->egress-edges (reduce (fn [m [from to]]
                                     (update m from (fn [v]
                                                      (conj (set v) to))))
                                   {}
                                   workflow)

        all-tasks (into (set (map first workflow)) (map second workflow))]

    (doseq [{:keys [flow/from flow/to] :as entry} flow-conditions]
      (when-not (or (all-tasks from) (= from :all))
        (hje/print-invalid-task-name-error
         entry :flow/from (:flow/from entry) :flow-conditions all-tasks)
        (throw (ex-info ":flow/from value doesn't name a node in the workflow"
                        {:entry entry})))
      (when-not (or (= :all to)
                    (= :none to)
                    (and (= from :all)
                         (empty? (remove all-tasks to)))
                    (and (coll? to)
                         (every? (fn [t]
                                   (try ((task->egress-edges from) t)
                                        (catch NullPointerException e nil))) to)))
        (hje/print-invalid-task-name-error
         entry :flow/to (:flow/to entry) :flow-conditions all-tasks)
        (throw (ex-info ":flow/to value doesn't name a valid connected task in the workflow, :all, or :none"
                        {:entry entry}))))))

(defn validate-peer-config [peer-config]
  (schema/validate PeerConfig peer-config))

(defn validate-peer-client-config [peer-client-config]
  (schema/validate PeerClientConfig peer-client-config))

(defn validate-job-schema [job]
  (try
    (schema/validate Job job)
    (catch Throwable t
      (print-schema-errors! job t)
      (throw t))))

(defn validate-job
  [job]
  (validate-job-schema job)
  (validate-catalog job)
  (validate-lifecycles job)
  (validate-workflow job))

(defn validate-flow-pred-all-kws [{:keys [flow-conditions] :as job}]
  (doseq [entry flow-conditions]
    (prewalk
     (fn [x]
       (when-not (or (keyword? x) (coll? x) (nil? x))
         (let [error-data {:error-key :flow/predicate
                           :path [:flow-conditions]
                           :context entry}
               msg "All tokens in predicate must be either a keyword or vector."]
           (hje/malformed-value-error* job error-data :flow-conditions msg))
         (throw (ex-info "Token in :flow/predicate was not a keyword or collection" {:token x})))
       x)
     (:flow/predicate entry))))

(defn validate-all-position [{:keys [flow-conditions]}]
  (let [flow-nodes (into #{} (map :flow/from flow-conditions))]
    (doseq [node flow-nodes]
      (doseq [entry (rest (filter #(= node (:flow/from %)) flow-conditions))]
        (when (= :all (:flow/to entry))
          (let [error-data {:error-key :flow/to
                            :error-value :all
                            :path [:flow-conditions]}
                msg ":flow/to mapped to :all value must appear first flow ordering"]
            (hje/entry-ordering-error* flow-conditions error-data :flow-conditions msg))
          (throw (ex-info ":flow/to mapped to :all value must appear first flow ordering" {:entry entry})))))))

(defn using-all-clause? [flow-conditions]
  (seq (filter #(= :all (:flow/to %)) flow-conditions)))

(defn validate-none-position [{:keys [flow-conditions]}]
  (let [flow-nodes (into #{} (map :flow/from flow-conditions))]
    (doseq [node flow-nodes]
      (let [entries (filter #(= node (:flow/from %)) flow-conditions)]
        (let [entries (if (using-all-clause? entries)
                        (rest (rest entries))
                        (rest entries))]
          (doseq [entry entries]
            (when (= :none (:flow/to entry))
              (let [error-data {:error-key :flow/to
                                :error-value :none
                                :path [:flow-conditions]}
                    msg ":flow/to mapped to :none value must exactly proceed :all entry"]
                (hje/entry-ordering-error* flow-conditions error-data :flow-conditions msg))
              (throw (ex-info ":flow/to mapped to :none value must exactly proceed :all entry" {:entry entry})))))))))

(defn validate-short-circuit [{:keys [flow-conditions]}]
  (let [flow-nodes (into #{} (map :flow/from flow-conditions))]
    (doseq [node flow-nodes]
      (let [entries (filter #(= node (:flow/from %)) flow-conditions)
            chunks (partition-by true? (map :flow/short-circuit? entries))]
        (when (or (> (count chunks) 2)
                  (seq (filter identity (apply concat (rest chunks)))))
          (let [error-data {:error-key :flow/short-circuit?
                            :error-value true
                            :path [:flow-conditions]}
                msg ":flow/short-circuit? entries must proceed all entries that aren't :flow/short-circuit?"]
            (hje/entry-ordering-error* flow-conditions error-data :flow-conditions msg))
          (throw (ex-info ":flow/short-circuit entries must proceed all entries that aren't :flow/short-circuit"
                          {:entry entries})))))))

(defn validate-auto-short-circuit [{:keys [flow-conditions] :as job}]
  (doseq [entry flow-conditions]
    (when (and (or (= (:flow/to entry) :all)
                   (= (:flow/to entry) :none))
               (not (:flow/short-circuit? entry)))
      (let [data {:error-type :multi-key-semantic-error
                  :error-keys [:flow/to :flow/short-circuit?]
                  :error-key :flow/to
                  :semantic-error :auto-short-circuit
                  :path [:flow-conditions]}]
        (hje/print-helpful-job-error job data entry :flow-conditions))
      (throw (ex-info ":flow/to :all and :none require :flow/short-circuit? to be true"
                      {:entry entry})))))

(defn validate-flow-conditions [{:keys [flow-conditions workflow] :as job}]
  (validate-flow-structure job)
  (validate-flow-connections job)
  (validate-flow-pred-all-kws job)
  (validate-all-position job)
  (validate-none-position job)
  (validate-short-circuit job)
  (validate-auto-short-circuit job))

(defn window-names-a-task [tasks {:keys [window/task] :as w}]
  (when-not (some #{task} tasks)
    (hje/print-invalid-task-name-error w :window/task task :windows tasks)
    (throw (ex-info ":window/task must name a task in the catalog" {:window w :tasks tasks}))))

(defn window-ids-unique [{:keys [windows] :as job}]
  (let [ids (map :window/id windows)
        dupes (find-dupes ids)]
    (when (seq dupes)
      (let [data {:error-type :duplicate-entry-error
                  :error-key :window/id
                  :error-value (first dupes)
                  :path [:windows]}]
        (hje/print-helpful-job-error job data windows :windows))
      (throw (ex-info ":window/id must be unique across windows, found" {:ids ids})))))

(defn range-and-slide-units-compatible [job w]
  (when (and (:window/range w) (:window/slide w))
    (when-not (= (u/standard-units-for (second (:window/range w)))
                 (u/standard-units-for (second (:window/slide w))))
      (let [data {:error-type :multi-key-semantic-error
                  :error-keys [:window/range :window/slide]
                  :error-key :window/range
                  :semantic-error :range-and-slide-incompatible
                  :path [:windows]}]
        (hje/print-helpful-job-error job data w :windows))
      (throw (ex-info "Incompatible units for :window/range and :window/slide" {:window w})))))

(defn sliding-windows-define-range-and-slide [job w]
  (when (= (:window/type w) :sliding)
    (when (or (not (:window/range w)) (not (:window/slide w)))
      (let [data (a/constraint->error
                  {:predicate 'range-defined-for-fixed-and-sliding?
                   :path [:windows]})]
        (hje/print-helpful-job-error job data w :windows))
      (throw (ex-info ":sliding windows must define both :window/range and :window/slide" {:window w})))))

(defn fixed-windows-dont-define-slide [job w]
  (when (and (= (:window/type w) :fixed) (:window/slide w))
    (let [data {:error-type :multi-key-semantic-error
                :error-keys [:window/type :window/slide]
                :error-key :window/type
                :semantic-error :fixed-windows-dont-define-slide
                :path [:windows]}]
      (hje/print-helpful-job-error job data w :windows))
    (throw (ex-info ":fixed windows do not define a :window/slide value" {:window w}))))

(defn global-windows-dont-define-range-or-slide [job w]
  (when (and (= (:window/type w) :global) (or (:window/range w) (:window/slide w)))
    (let [data {:error-type :multi-key-semantic-error
                :error-keys [:window/type :window/range :window/slide]
                :error-key :window/type
                :semantic-error :global-windows-dont-define-range-or-slide
                :path [:windows]}]
      (hje/print-helpful-job-error job data w :windows))
    (throw (ex-info ":global windows do not define a :window/range or :window/slide value" {:window w}))))

(defn session-windows-dont-define-range-or-slide [job w]
  (when (and (= (:window/type w) :session) (or (:window/range w) (:window/slide w)))
    (let [data {:error-type :multi-key-semantic-error
                :error-keys [:window/type :window/range :window/slide]
                :error-key :window/type
                :semantic-error :session-windows-dont-define-range-or-slide
                :path [:windows]}]
      (hje/print-helpful-job-error job data w :windows))
    (throw (ex-info ":session windows do not define a :window/range or :window/slide value" {:window w}))))

(defn session-windows-define-a-timeout [job w]
  (when (and (= (:window/type w) :session) (not (:window/timeout-gap w)))
    (let [data {:error-type :contextual-missing-key-error
                :present-key :window/type
                :absent-key :window/timeout-gap
                :semantic-error :session-windows-define-a-timeout
                :path [:windows]}]
      (hje/print-helpful-job-error job data w :windows))
    (throw (ex-info ":session windows must define a :window/timeout-gap value" {:window w}))))

(defn window-key-where-required [job w]
  (let [t (:window/type w)]
    (when (and (some #{t} #{:fixed :sliding :session})
               (not (:window/window-key w)))
      (let [data {:error-type :contextual-missing-key-error
                  :present-key :window/type
                  :absent-key :window/window-key
                  :semantic-error :window-key-required
                  :path [:windows]}]
        (hje/print-helpful-job-error job data w :windows))
      (throw (ex-info (format "Window type %s requires a :window/window-key" t) {:window w})))))

(defn task-has-uniqueness-key [job w catalog]
  (let [t (planning/find-task catalog (:window/task w))
        deduplicate? (and (:onyx/uniqueness-key t)
                          (or (true? (:onyx/deduplicate? t))
                              (nil? (:onyx/deduplicate? t))))
        no-deduplicate? (and (nil? (:onyx/uniqueness-key t))
                            (false? (:onyx/deduplicate? t)))
        valid-combo? (or (and (not deduplicate?) no-deduplicate?)
                         (and deduplicate? (not no-deduplicate?)))]
    (when-not valid-combo?
      (let [data {:error-type :mutually-exclusive-error
                  :error-keys [:onyx/uniqueness-key :onyx/deduplicate?]
                  :error-key :onyx/uniqueness-key
                  :semantic-error :task-uniqueness-key
                  :path [:catalog]}]
        (hje/print-helpful-job-error job data t :catalog))
      (throw (ex-info
              (format "Task %s is windowed, and therefore define :onyx/uniqueness-key, or do not define :onyx/uniqueness-key and use :onyx/deduplicate? false."
                      (:onyx/name t))
              {:task t})))))

(defn validate-windows [{:keys [windows catalog] :as job}]
  (let [task-names (map :onyx/name catalog)]
    (window-ids-unique job)
    (doseq [w windows]
      (window-names-a-task task-names w)
      (range-and-slide-units-compatible job w)
      (sliding-windows-define-range-and-slide job w)
      (fixed-windows-dont-define-slide job w)
      (global-windows-dont-define-range-or-slide job w)
      (session-windows-dont-define-range-or-slide job w)
      (session-windows-define-a-timeout job w)
      (window-key-where-required job w)
      (task-has-uniqueness-key job w catalog))))

(defn trigger-names-a-window [window-ids t]
  (when-not (some #{(:trigger/window-id t)} window-ids)
    (hje/print-invalid-task-name-error t :trigger/window-id (:trigger/window-id t) :triggers window-ids)
    (throw (ex-info "Trigger must name a window ID" {:trigger t :window-ids window-ids}))))

(defn validate-triggers [{:keys [windows triggers] :as job}]
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
