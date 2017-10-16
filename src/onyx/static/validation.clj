(ns onyx.static.validation
  (:require [clojure.walk :refer [prewalk]]
            [com.stuartsierra.dependency :as dep]
            [schema.core :as schema]
            [onyx.static.planning :as planning]
            [onyx.windowing.units :as u]
            [onyx.information-model :refer [model]]
            [onyx.static.helpful-job-errors :as hje]
            [onyx.static.analyzer :as a]
            [onyx.static.util :refer [index-of]]
            [onyx.schema :refer [UniqueTaskMap TaskMap Catalog Workflow Job
                                 LifecycleCall StateAggregationCall
                                 RefinementCall TriggerCall Lifecycle
                                 EnvConfig PeerConfig PeerClientConfig
                                 FlowCondition] :as os]))

(defn find-dupes [coll]
  (map key (remove (comp #{1} val) (frequencies coll))))

(defn print-schema-errors! [job t]
  (let [{:keys [error-type error-value path] :as data} (a/analyze-error job t)
        data (assoc data :e t)]
    (hje/print-helpful-job-error job data (get-in job (butlast path)) (first path))
    (println)))

(defn helpful-validate [schema value job]
  (try
    (schema/validate schema value)
    (catch Throwable t
      (let [res (try
                  (print-schema-errors! job t)
                  {:helpful? true}
                  (catch Throwable failure-t
                    {:helpful? false
                     :e failure-t}))]
        (if (:helpful? res)
          (throw (ex-info "Using helpful error, back propagating original error." {:e t}))
          ;; Something went wrong trying to come up with a better
          ;; error. Throw back the original exception.
          (throw t))))))

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
        (hje/print-helpful-job-error-and-throw job data catalog :catalog)))))

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
        (hje/print-helpful-job-error-and-throw job data entry :catalog)))))

(defn min-max-n-peers-mutually-exclusive [job entry]
  (when (and ;; allow conflicting n-peers / max-peers if they are equivalent
             (not (= 1 (:onyx/max-peers entry) (:onyx/n-peers entry)))
             (or (and (:onyx/min-peers entry) (:onyx/n-peers entry))
                 (and (:onyx/max-peers entry) (:onyx/n-peers entry))))
    (let [data {:error-type :multi-key-semantic-error
                :error-keys [:onyx/min-peers :onyx/max-peers :onyx/n-peers]
                :error-key :onyx/n-peers
                :error-value [(:onyx/min-peers entry) (:onyx/max-peers entry) (:onyx/n-peers entry)]
                :semantic-error :n-peers-with-min-or-max
                :path [:catalog]}]
      (hje/print-helpful-job-error-and-throw job data entry :catalog))))

(defn validate-catalog
  [{:keys [catalog] :as job}]
  (no-duplicate-entries job)

  (doseq [entry catalog]
    (min-and-max-peers-sane job entry)
    (min-max-n-peers-mutually-exclusive job entry)
    (helpful-validate TaskMap entry job)))

(defn validate-workflow-names [{:keys [workflow catalog]}]
  (when-let [missing-names (->> workflow
                                (mapcat identity)
                                (remove (set (map :onyx/name catalog)))
                                seq)]
    (hje/print-workflow-element-error
     workflow
     (first missing-names)
     (fn [faulty-key]
       (str "Task " (pr-str faulty-key) " wasn't found in the catalog.")))))

(defn validate-workflow-no-dupes [{:keys [workflow] :as job}]
  (let [dupes (find-dupes workflow)]
    (when (seq dupes)
      (hje/print-workflow-edge-error workflow (first dupes)
                                     (constantly "Workflow entries cannot contain duplicates")))))

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
         (str "Intermediate task " (pr-str faulty-key) " requires both incoming and outgoing edges."))))))

(defn validate-workflow-graph [{:keys [catalog workflow]}]
  (let [g (planning/to-dependency-graph workflow)]
    (validate-workflow-intermediates workflow g (catalog->type-task-names catalog #{:function}))
    (validate-workflow-inputs g (catalog->type-task-names catalog #{:input}))))

(defn validate-workflow [job]
  (validate-workflow-names job)
  (validate-workflow-graph job)
  (validate-workflow-no-dupes job))

(defn validate-resume-point [{:keys [workflow catalog windows resume-point] :as job}]
  (let [tasks (reduce into #{} workflow)
        task->task-map (into {} (map (juxt :onyx/name identity) catalog))
        task->windows (group-by :window/task windows)]
    (when resume-point
      (run! (fn [[task-id task-map]]
              ;; Improve mode map errors
              (doseq [t [:input :windows]]
                (let [resume (get-in resume-point [task-id t])]
                  (when (and (= :initialize (:mode resume))
                             (not= resume {:mode :initialize}))
                    (throw (ex-info (format "No other keys are allowed in an initialize resume point (task: %s, type: %s). Please use {:mode :initialize} with no other keys." task-id t) 
                                    {:task task-id
                                     :type t
                                     :resume-point resume})))))

              (when (and (= :input (:onyx/type task-map))
                         (not (get-in resume-point [task-id :input])))
                (throw (ex-info (format "Missing input resume-point for task %s." task-id) 
                                {:task task-id
                                 :resume-point resume-point})))

              (when (and (:output (:onyx/type task-map))
                         (not (get-in resume-point [task-id :output])))
                (throw (ex-info (format "Missing output resume-point for task %s." task-id) 
                                {:task task-id
                                 :resume-point resume-point})))

              (when-let [windows (get task->windows task-id)]
                (let [window-ids (set (map :window/id windows))
                      resume-point-windows (set (keys (get-in resume-point [task-id :windows])))] 
                  (when (not= window-ids resume-point-windows)
                    (let [missing-resume-points (clojure.set/difference window-ids resume-point-windows)
                          missing-windows (clojure.set/difference resume-point-windows window-ids)] 
                      (throw (ex-info (format "Incorrect window resume-points for task %s. Missing resume points for windows %s. Additional resume points for %s." 
                                              task-id missing-resume-points missing-windows)  
                                      {:task task-id
                                       :missing-resume-points missing-resume-points
                                       :resume-points-with-missing-windows missing-windows})))))))
            task->task-map))))

(defn validate-lifecycles [{:keys [lifecycles catalog] :as job}]
  (doseq [lifecycle lifecycles]
    (helpful-validate Lifecycle lifecycle job)

    (when-not (or (= (:lifecycle/task lifecycle) :all)
                  (some #{(:lifecycle/task lifecycle)} (map :onyx/name catalog)))
      (hje/print-invalid-task-name-error
       lifecycle :lifecycle/task
       (:lifecycle/task lifecycle)
       :lifecycles
       (map :onyx/name catalog)))))

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
  (doseq [entry flow-conditions]
    (helpful-validate FlowCondition entry job)))

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
         entry :flow/from (:flow/from entry) :flow-conditions all-tasks))
      (when-not (or (= :all to)
                    (= :none to)
                    (and (= from :all)
                         (empty? (remove all-tasks to)))
                    (and (coll? to)
                         (every? (fn [t]
                                   (get (task->egress-edges from) t)) 
                                 to)))
        (if (coll? to)
          (run! (fn [t] (when-not (get (task->egress-edges from) t)
                          (hje/print-invalid-task-name-error entry :flow/to t :flow-conditions all-tasks))) 
                to)
          (hje/print-invalid-flow-to-type entry :flow/to (:flow/to entry) :flow-conditions all-tasks))))))

(defn validate-peer-config [peer-config]
  (schema/validate PeerConfig peer-config))

(defn validate-peer-client-config [peer-client-config]
  (schema/validate PeerClientConfig peer-client-config))

(defn validate-job-schema [job]
  (helpful-validate Job job job))

(defn validate-flow-pred-all-kws [{:keys [flow-conditions] :as job}]
  (doseq [entry flow-conditions]
    (prewalk
     (fn [x]
       (when-not (or (keyword? x) (coll? x) (nil? x))
         (let [error-data {:error-key :flow/predicate
                           :path [:flow-conditions]
                           :context entry}
               msg "All tokens in predicate must be either a keyword or vector."]
           (hje/malformed-value-error* job error-data :flow-conditions msg)))
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
            (hje/entry-ordering-error* flow-conditions error-data :flow-conditions msg)))))))

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
                (hje/entry-ordering-error* flow-conditions error-data :flow-conditions msg)))))))))

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
            (hje/entry-ordering-error* flow-conditions error-data :flow-conditions msg)))))))

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
        (hje/print-helpful-job-error-and-throw job data entry :flow-conditions)))))

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
    (hje/print-invalid-task-name-error w :window/task task :windows tasks)))

(defn window-ids-unique [{:keys [windows] :as job}]
  (let [ids (map :window/id windows)
        dupes (find-dupes ids)]
    (when (seq dupes)
      (let [data {:error-type :duplicate-entry-error
                  :error-key :window/id
                  :error-value (first dupes)
                  :path [:windows]}]
        (hje/print-helpful-job-error-and-throw job data windows :windows)))))

(defn range-and-slide-units-compatible [job w]
  (when (and (:window/range w) (:window/slide w))
    (when-not (= (u/standard-units-for (second (:window/range w)))
                 (u/standard-units-for (second (:window/slide w))))
      (let [data {:error-type :multi-key-semantic-error
                  :error-keys [:window/range :window/slide]
                  :error-key :window/range
                  :semantic-error :range-and-slide-incompatible
                  :path [:windows]}]
        (hje/print-helpful-job-error-and-throw job data w :windows)))))

(defn sliding-windows-define-range-and-slide [job w]
  (when (= (:window/type w) :sliding)
    (when (or (not (:window/range w)) (not (:window/slide w)))
      (let [data (a/constraint->error
                  {:predicate 'range-defined-for-fixed-and-sliding?
                   :path [:windows]})]
        (hje/print-helpful-job-error-and-throw job data w :windows)))))

(defn fixed-windows-dont-define-slide [job w]
  (when (and (= (:window/type w) :fixed) (:window/slide w))
    (let [data {:error-type :multi-key-semantic-error
                :error-keys [:window/type :window/slide]
                :error-key :window/type
                :semantic-error :fixed-windows-dont-define-slide
                :path [:windows]}]
      (hje/print-helpful-job-error-and-throw job data w :windows))))

(defn global-windows-dont-define-range-or-slide [job w]
  (when (and (= (:window/type w) :global) (or (:window/range w) (:window/slide w)))
    (let [data {:error-type :multi-key-semantic-error
                :error-keys [:window/type :window/range :window/slide]
                :error-key :window/type
                :semantic-error :global-windows-dont-define-range-or-slide
                :path [:windows]}]
      (hje/print-helpful-job-error-and-throw job data w :windows))))

(defn session-windows-dont-define-range-or-slide [job w]
  (when (and (= (:window/type w) :session) (or (:window/range w) (:window/slide w)))
    (let [data {:error-type :multi-key-semantic-error
                :error-keys [:window/type :window/range :window/slide]
                :error-key :window/type
                :semantic-error :session-windows-dont-define-range-or-slide
                :path [:windows]}]
      (hje/print-helpful-job-error-and-throw job data w :windows))))

(defn session-windows-must-store-extents [job {:keys [window/storage-strategy] :as w}]
  (when (and (= (:window/type w) :session)
             (and (not (nil? storage-strategy))
                  (not (some #{:incremental :extents} storage-strategy))))
    (let [data {:error-type :multi-key-semantic-error
                :error-keys [:window/storage-strategy :window/type]
                :error-key :window/type
                :semantic-error :session-windows-must-store-extents
                :path [:windows]}]
      (hje/print-helpful-job-error-and-throw job data w :windows))))

(defn window-incremental-extents-incompatible [job {:keys [window/storage-strategy] :as w}]
  (when (and (some #{:extents} storage-strategy)
             (some #{:incremental} storage-strategy))
    (let [data {:error-type :multi-key-semantic-error
                :error-keys [:window/storage-strategy :window/type]
                :error-key :window/type
                :semantic-error :extents-and-incremental-mutually-exclusive
                :path [:windows]}]
      (hje/print-helpful-job-error-and-throw job data w :windows))))

(defn session-windows-define-a-timeout [job w]
  (when (and (= (:window/type w) :session) (not (:window/timeout-gap w)))
    (let [data {:error-type :contextual-missing-key-error
                :present-key :window/type
                :absent-key :window/timeout-gap
                :semantic-error :session-windows-define-a-timeout
                :path [:windows]}]
      (hje/print-helpful-job-error-and-throw job data w :windows))))

(defn window-key-where-required [job w]
  (let [t (:window/type w)]
    (when (and (some #{t} #{:fixed :sliding :session})
               (not (:window/window-key w)))
      (let [data {:error-type :contextual-missing-key-error
                  :present-key :window/type
                  :absent-key :window/window-key
                  :semantic-error :window-key-required
                  :path [:windows]}]
        (hje/print-helpful-job-error-and-throw job data w :windows)))))

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
      (session-windows-must-store-extents job w)
      (window-incremental-extents-incompatible job w)
      (session-windows-define-a-timeout job w)
      (window-key-where-required job w))))

(defn trigger-names-a-window [window-ids t]
  (when-not (some #{(:trigger/window-id t)} window-ids)
    (hje/print-invalid-task-name-error t :trigger/window-id (:trigger/window-id t) :triggers window-ids)))

(defn trigger-id-unique-per-window [job triggers]
  (when-let [invalid-triggers (->> triggers
                                   (group-by (juxt :trigger/window-id :trigger/id))
                                   (filter (fn [[k v]]
                                             (> (count v) 1)))
                                   (vals)
                                   (first))]
    (let [data {:error-type :duplicate-entry-error
                :semantic-error :conflicting-trigger-ids
                :error-key :trigger/id
                :path [:triggers]}] 
      (hje/print-helpful-job-error-and-throw job data invalid-triggers :triggers))))

(defn validate-triggers [{:keys [windows triggers catalog] :as job}]
  (let [window-names (map :window/id windows)]
    (trigger-id-unique-per-window job triggers)
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

(defn validate-job
  [job]
  (binding [*out* *err*]
    (validate-job-schema job)
    (validate-catalog job)
    (validate-lifecycles job)
    (validate-workflow job)
    (validate-flow-conditions job)
    (validate-windows job)
    (validate-triggers job)
    (validate-resume-point job)))
