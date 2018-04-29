(ns onyx.information-model)

(def peer-scheduler-event-types
  [:peer-reallocated :peer-left :job-killed :job-completed :recovered])

(def trigger-event-types
  [:timer-tick :new-segment :job-completed :recovered :watermark :checkpointed])

(def model
  {:job {:summary "An Onyx job is defined in data and submitted to a a cluster for execution. It takes a map with keys :catalog, :workflow, :flow-conditions, :windows, :triggers, :metadata, and :task-scheduler. Returns a map of :job-id and :task-ids, which map to a UUID and vector of maps respectively. :metadata is a map of values that must serialize to EDN. :metadata will be logged with all task output, and is useful for identifying a particular task based on something other than its name or ID."
         :model {:catalog {:doc "All inputs, outputs, and functions in a workflow must be described via a catalog. A catalog is a vector of maps. Configuration and docstrings are described in the catalog."
                           :type :map
                           :choices :any
                           :tags [:task]
                           :examples [{:doc "Simple Catalog Example"
                                       :example [{:onyx/name :in
                                                  :onyx/plugin :onyx.plugin.core-async/input
                                                  :onyx/type :input
                                                  :onyx/medium :core.async
                                                  :onyx/batch-size 20
                                                  :onyx/max-peers 1
                                                  :onyx/doc "Reads segments from a core.async channel"}

                                                 {:onyx/name :inc
                                                  :onyx/fn :onyx.peer.min-peers-test/my-inc
                                                  :onyx/type :function
                                                  :onyx/batch-size 20}

                                                 {:onyx/name :out
                                                  :onyx/plugin :onyx.plugin.core-async/output
                                                  :onyx/type :output
                                                  :onyx/medium :core.async
                                                  :onyx/batch-size 20
                                                  :onyx/max-peers 1
                                                  :onyx/doc "Writes segments to a core.async channel"}]}]
                           :doc-url "http://www.onyxplatform.org/docs/user-guide/latest/#_catalog"
                           :cheat-sheet-url "http://www.onyxplatform.org/docs/cheat-sheet/latest/#/catalog-entry"
                           :optional? false
                           :added "0.1.0"}
                 :job-name {:doc "Job name that can be used to reverse lookup a current job-id that corresponds to that name. Only one job for a given job-name should be running at a time. Please see onyx.api/job-ids."
                            :type [:keyword :string]
                            :parameters "#/job-name"
                            :tags [:metadata]
                            :optional? true
                            :added "0.12.0"}
                 :workflow {:doc "A workflow is the structural specification of an Onyx program. Its purpose is to articulate the paths that data flows through the cluster at runtime. It is specified via a directed, acyclic graph. A workflow comprises a vector of two element vectors, each containing two tasks name keywords."
                            :type :vector
                            :examples [{:doc "Simple workflow example, showing :in task, flowing to two :intermediate tasks, each flowing to the same output task."
                                        :example [[:in :intermediate1]
                                                  [:in :intermediate2]
                                                  [:intermediate1 :out1]
                                                  [:intemediate2 :out2]]}]
                            :doc-url "http://www.onyxplatform.org/docs/user-guide/latest/#_workflow"
                            :choices :any
                            :tags [:task]
                            :optional? false
                            :added "0.1.0"}
                 :task-scheduler {:doc "Task scheduler setting"
                                  :type :keyword
                                  :choices [:onyx.task-scheduler/balanced
                                            :onyx.task-scheduler/percentage
                                            :onyx.task-scheduler/colocated]
                                  :tags [:task]
                                  :optional? false
                                  :added "0.1.0"}
                 :resume-point {:doc "Resume points allow job state to be resumed by new jobs. See the documentation for more information."
                                :type :map
                                :parameters "#/resume-point"
                                :doc-url "http://www.onyxplatform.org/docs/user-guide/latest/#resume-point"
                                :tags [:task]
                                :optional? true
                                :added "0.10.0"}
                 :percentage {:doc "For use with percentage job scheduler. Defines the percentage of the peers in the cluster that the job should receive."
                              :type :double
                              :tags [:task]
                              :optional? true
                              :added "0.1.0"}
                 :flow-conditions {:doc "Flow conditions are used for isolating logic about whether or not segments should pass through different tasks in a workflow, and support a rich degree of composition with runtime parameterization."
                                   :type :vector
                                   :parameters "#/flow-conditions-entry"
                                   :doc-url "http://www.onyxplatform.org/docs/user-guide/latest/#flow-conditions"
                                   :examples [{:doc "Exmaple flow conditions (note, this is an incomplete job)."
                                               :example [{:workflow [[:input-stream :process-children]
                                                                     [:input-stream :process-adults]
                                                                     [:input-stream :process-female-athletes]
                                                                     [:input-stream :process-everyone]]
                                                          :flow-conditions [{:flow/from :input-stream
                                                                             :flow/to [:process-children]
                                                                             :my/max-child-age 17
                                                                             :flow/predicate [:my.ns/child? :my/max-child-age]
                                                                             :flow/doc "Emits segment if this segment is a child."}

                                                                            {:flow/from :input-stream
                                                                             :flow/to [:process-adults]
                                                                             :flow/predicate :my.ns/adult?
                                                                             :flow/doc "Emits segment if this segment is an adult."}

                                                                            {:flow/from :input-stream
                                                                             :flow/to [:process-female-athletes]
                                                                             :flow/predicate [:and :my.ns/female? :my.ns/athlete?]
                                                                             :flow/doc "Emits segment if this segment is a female athlete."}

                                                                            {:flow/from :input-stream
                                                                             :flow/to [:process-everyone]
                                                                             :flow/predicate :my.ns/constantly-true
                                                                             :flow/doc "Always emit this segment"}]}]}]
                                   :tags [:task]
                                   :optional? true
                                   :added "0.5.0"}
                 :windows {:doc "Windows allow you to group and accrue data into possibly overlapping buckets. Windows are intimately related to the Triggers feature."
                           :type :vector
                           :tags [:task :windows :triggers :state]
                           :doc-url "http://www.onyxplatform.org/docs/user-guide/latest/#windowing-and-aggregation"
                           :parameters "#/window-entry"
                           :optional? true
                           :added "0.8.0"}
:triggers {:doc "Triggers are a feature that interact with windows. Windows capture and bucket data over time. Triggers let you release the captured data over a variety stimuli."
           :doc-url "http://www.onyxplatform.org/docs/user-guide/latest/#triggers"
           :parameters "#/trigger-entry"
           :type :vector
           :tags [:task :windows :state]
           :optional? true
           :added "0.8.0"}
:lifecycles {:doc "Lifecycles are a feature that allow you to control code that executes at particular points during task execution on each peer. Lifecycles are data driven and composable."
             :doc-url "http://www.onyxplatform.org/docs/user-guide/latest/#lifecycles"
             :parameters "#/lifecycle-entry"
             :type :vector
             :tags [:task]
             :optional? true
             :added "0.1.0"}
:metadata {:doc "Map of metadata to be associated with the job. Supports the supply of `:job-id` as a UUID, which will allow idempotent job submission. Metadata can be accessed from tasks via `:onyx.core/metadata` in the event map."
           :doc-url "http://www.onyxplatform.org/docs/user-guide/latest/#submit-job"
           :type :map
           :tags [:task]
           :optional? true
           :added "0.9.0"}

:job-config {:doc "Parameters specific to the job being submitted. In some cases these options may override peer-config entries."}}}


         :catalog-entry
         {:summary "All inputs, outputs, and functions in a workflow must be described via a catalog. A catalog is a vector of maps, strikingly similar to Datomic’s schema. Configuration and docstrings are described in the catalog."
          :doc-url "http://www.onyxplatform.org/docs/user-guide/latest/#_catalog"
          :model {:onyx/name
                  {:doc "The name of the task that represents this catalog entry. Must correspond to a keyword in the workflow associated with this catalog."
                   :type :keyword
                   :choices :any
                   :tags [:task]
                   :restrictions ["Must be unique across all catalog entries."
                                  "Value cannot be `:none`."
                                  "Value cannot be `:all`."]
                   :optional? false
                   :added "0.8.0"}

                  :onyx/type
                  {:doc "The role that this task performs. `:input` reads from a data source, and must be a source node. `:function` applies a transformation and must be an intermediate or terminal node. `:reduce` is a task that must contain windows, may be either a terminal or intermediate node, and does not pass transformed segments downstream. When `:reduce` task is an intermediate node, a trigger with `:trigger/emit` must be set on the task. `:output` writes data as the terminal node in a workflow."
                   :type :keyword
                   :tags [:task]
                   :choices [:input :function :output :reduce]
                   :optional? false
                   :added "0.8.0"}

                  :onyx/batch-size
                  {:doc "The number of segments a peer will wait to read before processing them all in a batch for this task. Segments will be processed when either `:onyx/batch-size` segments have been received at this peer, or `:onyx/batch-timeout` milliseconds have passed - whichever comes first. This is a knob that is used to tune throughput and latency, and it goes hand-in-hand with `:onyx/batch-timeout`."
                   :type :integer
                   :tags [:latency :throughput]
                   :restrictions ["Value must be greater than 0."]
                   :optional? false
                   :added "0.8.0"}

                  :onyx/max-segments-per-barrier
                  {:doc "The number of segments a peer is allowed to read before a peer before emitting a checkpointed barrier. This can be used as a form of backpressure, especially for high fan out jobs, though should generally be avoided in favor of other forms of backpressure."
                   :type :integer
                   :tags [:latency :throughput :experimental]
                   :optionally-allowed-when ["`:onyx/type` is set to `:input`"]
                   :optional? true
                   :added "0.12.0"}

                  :onyx/batch-timeout
                  {:doc "The number of milliseconds a peer will wait to read more segments before processing them all in a batch for this task. Segments will be processed when either `:onyx/batch-timeout` milliseconds passed, or `:onyx/batch-size` segments have been read - whichever comes first. This is a knob that is used to tune throughput and latency, and it goes hand-in-hand with `:onyx/batch-size`."
                   :type :integer
                   :unit :milliseconds
                   :tags [:latency :throughput]
                   :restrictions ["Value must be greater than 0."]
                   :default 50
                   :optional? true
                   :added "0.8.0"}

                  :onyx/doc
                  {:doc "A docstring for this catalog entry."
                   :type :string
                   :tags [:documentation]
                   :optional? true
                   :added "0.8.0"}

                  :onyx/max-peers
                  {:doc "The maximum number of peers that will ever be assigned to this task concurrently."
                   :type :integer
                   :tags [:aggregation :grouping]
                   :restrictions ["Value must be greater than 0."]
                   :optional? true
                   :added "0.8.0"}

                  :onyx/min-peers
                  {:doc "The minimum number of peers that will be concurrently assigned to execute this task before it begins. If the number of peers working on this task falls below its initial count due to failure or planned departure, the choice of `:onyx/flux-policy` defines the strategy for what to do."
                   :type :integer
                   :tags [:aggregation :grouping]
                   :restrictions ["Value must be greater than 0."]
                   :optional? true
                   :added "0.8.0"}

                  :onyx/n-peers
                  {:doc "A convenience parameter which expands to `:onyx/min-peers` and `:onyx/max-peers` set to the same value. This is useful if you want to specify exactly how many peers should concurrently execute this task - no more, and no less."
                   :type :integer
                   :tags [:aggregation :grouping]
                   :restrictions ["Value must be greater than 0."
                                  "`:onyx/min-peers` cannot also be defined for this catalog entry."
                                  "`:onyx/max-peers` cannot also be defined for this catalog entry."]
                   :optional? true
                   :added "0.8.0"}

                  :onyx/language
                  {:doc "Designates the language that the function denoted by `:onyx/fn` is implemented in."
                   :type :keyword
                   :tags [:interoperability]
                   :choices [:clojure :java]
                   :default :clojure
                   :optional? true
                   :added "0.8.0"}

                  :onyx/restart-pred-fn
                  {:doc "A fully-qualified namespaced keyword pointing to function which takes an exception as a parameter, returning a boolean indicating whether the peer that threw this exception should restart its task."
                   :type :keyword
                   :choices :any
                   :tags [:fault-tolerance]
                   :restrictions ["Must resolve to a function on the classpath at runtime."]
                   :optional? true
                   :added "0.8.0"
                   :deprecated-version "0.8.9"
                   :deprecation-doc ":onyx/restart-pred-fn has been removed from Onyx. A more general and powerful feature has been added instead, named Lifecycle Exceptions. See the docs for :lifecycle/handle-exception to switch over."}

                  :onyx/params
                  {:doc "A vector of keys to obtain from the task map, and inject into the initial parameters of the function defined in :onyx/fn. The segment will be injected as the final parameter to the onyx/fn."
                   :type :vector
                   :tags [:function]
                   :optional? true
                   :added "0.8.0"}

                  :onyx/medium
                  {:doc "Denotes the kind of input or output communication or storage that is being read from or written to (e.g. `:kafka` or `:web-socket`). This is currently does not affect any functionality, and is reserved for the future."
                   :type :keyword
                   :tags [:plugin]
                   :choices :any
                   :required-when ["`:onyx/type` is set to `:input`"
                                   "`:onyx/type` is set to `:output`"]
                   :added "0.8.0"}

                  :onyx/plugin
                  {:doc "When `:onyx/language` is set to `:clojure`, this is a fully qualified, namespaced keyword pointing to a function that takes the Event map and returns a Record implementing the Plugin interfaces. When `:onyx/language` is set to `:java`, this is a keyword pointing to a Java class that is constructed with the Event map. This class must implement the interoperability interfaces."
                   :type :keyword
                   :tags [:plugin]
                   :choices :any
                   :restrictions ["Namespaced keyword required unless :onyx/language :java is set, in which case a non-namespaced keyword is required."]
                   :optionally-allowed-when ["`:onyx/type` is set to `:reduce`"]
                   :required-when ["`:onyx/type` is set to `:input`"
                                   "`:onyx/type` is set to `:output`"]
                   :added "0.8.0"}

                  :onyx/pending-timeout
                  {:doc "The duration of time, in milliseconds, that a segment that enters an input task has to be fully acknowledged and processed. That is, this segment, and any subsequent segments that it creates in downstream tasks, must be fully processed before this timeout occurs. If the segment is not fully processed, it will automatically be retried."
                   :type :integer
                   :default 60000
                   :tags [:input :plugin :latency :fault-tolerance]
                   :units :milliseconds
                   :deprecated-version "0.10.0"
                   :deprecation-doc "`:onyx/pending-timeout` has been deprecated as 0.10.0's Asynchronous Barrier Snapshotting fault tolerance technique does not depend on retrying individual segments on a timeout."
                   :optionally-allowed-when ["`:onyx/type` is set to `:input`"
                                             "Value must be greater than 0."]
                   :added "0.8.0"}

                  :onyx/input-retry-timeout
                  {:doc "The duration of time, in milliseconds, that the input task goes dormant between checking which segments should expire from its internal pending pool. When segments expire, they are automatically retried."
                   :type :integer
                   :default 1000
                   :tags [:input :plugin :latency :fault-tolerance]
                   :units :milliseconds
                   :optionally-allowed-when ["`:onyx/type` is set to `:input`"
                                             "Value must be greater than 0."]
                   :deprecated-version "0.10.0"
                   :deprecation-doc "`:onyx/input-retry-timeout` has been deprecated as 0.10.0's Asynchronous Barrier Snapshotting fault tolerance technique does not depend on retrying individual segments on a timeout."
                   :added "0.8.0"}

                  :onyx/max-pending
                  {:doc "The maximum number of segments that a peer executing an input task will allow in its internal pending message pool. If this pool is filled to capacity, it will not accept new segments - exhibiting backpressure to upstream message producers."
                   :type :integer
                   :default 10000
                   :tags [:input :plugin :latency :backpressure :fault-tolerance]
                   :units :segments
                   :optionally-allowed-when ["`:onyx/type` is set to `:input`"
                                             "Value must be greater than 0."]
                   :deprecated-version "0.10.0"
                   :deprecation-doc "`:onyx/max-pending` was removed as Asynchronous Barrier Snapshotting performs backpressure via barriers, rather than individual segments."
                   :added "0.8.0"}

                  :onyx/fn
                  {:doc "A function to transform a segment into another segment. A fully qualified, namespaced keyword that points to a function on the classpath. This function takes at least one argument - an incoming segment, and returns either a segment or a vector of segments. This function may not return `nil`. This function can be parameterized further through a variety of techniques. The function is called to transform the segments before being processed by windows, being sent downstream, or being written to an output medium."
                   :type :keyword
                   :tags [:function]
                   :required-when ["`:onyx/type` is set to `:function`"]
                   :optionally-allowed-when ["`:onyx/type` is set to `:input`"
                                             "`:onyx/type` is set to `:reduce`"
                                             "`:onyx/type` is set to `:output`"]
                   :added "0.8.0"}

                  :onyx/assign-watermark-fn
                  {:doc "A function to assign a watermark to a datasource by inspecting a segment read from that datasource. Should return the numbers of milliseconds since epoch. Missing watermarks will be ignored. A fully qualified, namespaced keyword that points to a function on the classpath. "
                   :type :keyword
                   :tags [:input]
                   :optionally-allowed-when ["`:onyx/type` is set to `:input`"]
                   :added "0.11.1"}

                  :onyx/group-by-key
                  {:doc "The key, or vector of keys, to group incoming segments by. Keys that hash to the same value will always be sent to the same virtual peer."
                   :type [:any [:any]]
                   :tags [:aggregation :grouping :windows]
                   :optionally-allowed-when ["`:onyx/type` is set to `:function`, `:output`, or `:reduce`."]
                   :restrictions ["Cannot be defined when `:onyx/group-by-fn` is defined."
                                  "`:onyx/flux-policy` must also be defined in this catalog entry."]
                   :added "0.8.0"}

                  :onyx/group-by-fn
                  {:doc "A fully qualified, namespaced keyword that points to a function on the classpath. This function takes a single argument, a segment, as a parameter. The value that the function returns will be hashed. Values that hash to the same value will always be sent to the same virtual peer."
                   :type :keyword
                   :tags [:aggregation :grouping :windows :function]
                   :optionally-allowed-when ["`:onyx/type` is set to `:function`, `:output`, or `:reduce`"]
                   :restrictions ["Cannot be defined when `:onyx/group-by-key` is defined."
                                  "`:onyx/flux-policy` must also be defined in this catalog entry."]
                   :added "0.8.0"}

                  :onyx/bulk?
                  {:doc "Boolean value indicating whether the function in this catalog entry denoted by `:onyx/fn` should take a single segment, or the entire batch of segments that were read as a parameter. When set to `true`, this task's `:onyx/fn` return value is ignored. The segments are identically propagated to the downstream tasks. The primary use of `:onyx/bulk?` is for side-effecting functions."
                   :type :boolean
                   :default false
                   :tags [:function]
                   :deprecated-version "0.9.11"
                   :deprecation-doc "`:onyx/bulk?` has been deprecated in favor of [`:onyx/batch-fn?`](http://www.onyxplatform.org/docs/cheat-sheet/latest/#catalog-entry/:onyx/batch-fn-QMARK). If you require the previous behavior, ensure your `:onyx/fn` returns the same segments that were passed into it."
                   :optionally-allowed-when ["`:onyx/type` is set to `:function`"]
                   :added "0.8.0"}

                  :onyx/batch-fn?
                  {:doc "Boolean value indicating whether the function in this catalog entry denoted by `:onyx/fn` should take a single segment, or the entire batch of segments that were read as a parameter. When `true`, the `:onyx/fn` must return a sequence of the same length as its input match. Each element of the return value represents the children segments that will succeed the corresponding parent segment. Hence, the arguments match positionally. Children values may either be a single segment, or a vector of segments, as normal. This feature is useful for batching requests to services, waiting for whole batches of asynchronous requests to be made, dedepulicating calculations, etc. Libraries such as [claro](https://github.com/xsc/claro), [muse](https://github.com/kachayev/muse), and [urania](https://funcool.github.io/urania/latest/) may be useful for use in these `:onyx/fn`s."
                   :type :boolean
                   :default false
                   :tags [:function :input :output :reduce]
                   :added "0.9.11"}

                  :onyx/flux-policy
                  {:doc "The policy that should be used when a task with grouping enabled loses a peer. Losing a peer means that the consistent hashing used to pin the same hashed values to the same peers will be altered. Using the `:kill` flux policy will kill the job. This is useful for jobs that cannot tolerate an altered hashing strategy. Using `:continue` will allow the job to continue running. With `:kill` and `:continue`, new peers will never be added to this job. The final policy is `:recover`, which is like `:continue`, but will allow peers to be added back to this job to meet the `:onyx/min-peers` number of peers working on this task concurrently."
                   :type :keyword
                   :choices [:kill :continue :recover]
                   :tags [:aggregation :grouping :windows]
                   :restrictions ["If `:kill` is used `:onyx/min-peers` or `:onyx/n-peers` must be defined for this catalog entry."
                                  "If `:recover` is used, then `:onyx/max-peers` must be equal to `:onyx/min-peers`. "]
                   :optionally-allowed-when ["`:onyx/type` is set to `:function`, `:output`, or `:reduce`."
                                             "`:onyx/group-by-key` or `:onyx/group-by-fn` is set."]
                   :added "0.8.0"}

                  :onyx/uniqueness-key
                  {:doc "The key of incoming segments that indicates global uniqueness. This is used by the Windowing feature to detect duplicated processing of segments. An example of this would be an `:id` key for segments representing users, assuming `:id` is globally unique in your system. An example of a bad uniqueness-key would be `:first-name` as two or more users may have their first names in common."
                   :type :any
                   :tags [:aggregation :windows]
                   :required-when ["A Window is defined on this task."]
                   :deprecated-version "0.10.0"
                   :deprecation-doc "Uniqueness keys and deduplication have been deprecated as the Asynchronous Barrier Snapshotting method supports exactly once data processing. If you have duplicates in your input source, you should roll your own filtering mechanism using windowing."
                   :added "0.8.0"}

                  :onyx/deduplicate?
                  {:doc "Does not deduplicate segments using the `:onyx/uniqueness-key`, which is otherwise required when using windowed tasks. Often useful if your segments do not have a unique key that you can use to filter incoming replayed or duplicated segments."
                   :type :boolean
                   :default true
                   :tags [:aggregation :windows]
                   :deprecated-version "0.10.0"
                   :deprecation-doc "Uniqueness keys and deduplication have been deprecated as the Asynchronous Barrier Snapshotting method supports exactly once data processing. If you have duplicates in your input source, you should roll your own filtering mechanism using windowing."
                   :optionally-allowed-when ["A window is defined on this task."]
                   :required-when ["A Window is defined on this task and there is no possible :onyx/uniqueness-key to on the segment to deduplicate with."]
                   :added "0.8.0"}

                  :onyx/required-tags
                  {:doc "When set, only allows peers which have *all* tags listed in this key in their :onyx.peer/tags configuration. This is used for preventing peers without certain user defined capabilities from executing particular tasks. A concrete use case would be only allowing peers with a database license key to execute a specific task."
                   :type [:keyword]
                   :default []
                   :optional? true
                   :added "0.8.9"}}}

   :flow-conditions-entry
   {:summary "Flow conditions are used for isolating logic about whether or not segments should pass through different tasks in a workflow, and support a rich degree of composition with runtime parameterization."
    :doc-url "http://www.onyxplatform.org/docs/user-guide/latest/#flow-conditions"
    :model {:flow/from
            {:doc "The source task from which segments are being sent."
             :type :keyword
             :optional? false
             :restrictions ["Must name a task in the workflow."]
             :added "0.8.0"}

            :flow/to
            {:doc "The destination task where segments will arrive. There must be an edge between all `:flow/from` tasks to the corresponding `:flow/to` task in the `:workflow` DAG. If set to `:all`, all downstream tasks will receive this segment. If set to `:none`, no downstream tasks will receive this segment. Otherwise it must name a vector of keywords indicating downstream tasks. The order of keywords is irrelevant."
             :type [:keyword [:keyword]]
             :choices [[:any] :all :none]
             :optional? false
             :restrictions ["When the value is a vector of keyword, every keyword must name a task in the workflow."]
             :added "0.8.0"}

            :flow/predicate
            {:doc "When denoted as a keyword, this must be a fully qualified, namespaced keyword pointing to a function on the classpath at runtime. This function takes at least 4 arguments - the Event map, the old segment before `:onyx/fn` was applied, the new segment after `:onyx/fn` was applied, and the sequence of new segments generated by the old segment. If the old segment generated exactly one segment, and not a sequence of segments, the value of the last parameter will be a collection with only the new segment in it.

                  When denoted as a vector of keywords, the first value in the vector  may either be the keyword `:and`, `:or`, or `:not`, or be a keyword as described above. In the latter case, any subsequent values must be keywords that resolve to keys in the flow condition entries map. The values of these keys are resolved and passed as additional parameters to the function. In the former case, the result of the function (which may again be wrapped with a vector to nest logical operators or parameters), is applied with the designated logical operator. This yields predicate composition."
             :type [:keyword [:keyword]]
             :optional? false
             :added "0.8.0"}

            :flow/exclude-keys
            {:doc "If any of the keys are present in the segment, they will be `dissoc`ed from the segment before it is sent downstream. This is useful when values in the segment are present purely for the purpose of making a decision about which downstream tasks it should be sent to."
             :type [[:keyword]]
             :optional? true
             :added "0.8.0"}

            :flow/short-circuit?
            {:doc "When multiple flow condition entry predicates evaluated to true, the tasks in `:flow/to` are set unioned. If this behavior is undesirable, and you want exactly the tasks in this flow condition's `:flow/to` key to be used, plus any previously matched flow conditions `:flow/to` values. Setting `:flow/short-circuit?` to `true` will force the matcher to stop executing and immediately return with the values that it matched."
             :type :boolean
             :optional? true
             :default false
             :restrictions ["Any entry that has :flow/short-circuit? set to true must come before any entries for an task that have it set to false or nil."]
             :added "0.8.0"}

            :flow/thrown-exception?
            {:doc "If an exception is thrown from an Onyx transformation function, you can capture it from within your flow conditions by setting this value to `true`. If an exception is thrown, only flow conditions with `:flow/thrown-exception?` set to `true` will be evaluated. The value that is normally the segment which is sent to the predicate will be the exception object that was thrown. Note that exceptions don't serialize. This feature is meant to be used in conjunction with post-transformations and Actions for sending exception values to downstream tasks. Tasks which are `:flow/to` with `:flow/thrown-exception?` set will not receive non-exceptional messages."
             :type :boolean
             :optional? true
             :default false
             :restrictions ["Exception flow conditions must have `:flow/short-circuit?` set to `true`"]
             :added "0.8.0"}

            :flow/post-transform
            {:doc "A fully qualified, namespaced keyword that points to a function on the classpath at runtime. This function is invoked when an exception is thrown processing a segment in `:onyx/fn` and this flow condition's predicate evaluates to `true`. The function takes 3 parameters - the Event map, the segment that causes the exception to be thrown, and the exception object. The return value of this function is sent to the downstream tasks instead of trying to serialize the exception. The return value must be a segment or sequence of segments, and must serialize."
             :type :keyword
             :optional? true
             :default nil
             :restrictions ["`:flow/thrown-exception?` must be set to `true`."]
             :added "0.8.0"}

            :flow/predicate-errors-to
            {:doc "A set of tasks to route a segment to when this flow condition's predicate throws an exception. Must be used in conjunction with `:flow/post-transform` to turn exceptions into serializable segments. If set to `:all`, all downstream tasks will receive this segment. If set to `:none`, no downstream tasks will receive this segment. Otherwise it must name a vector of keywords indicating downstream tasks. The order of keywords is irrelevant."
             :type [:keyword [:keyword]]
             :choices [[:any] :all :none]
             :optional? false
             :restrictions ["When the value is a vector of keyword, every keyword must name a task in the workflow."]
             :added "0.10.0"}

            :flow/action
            {:doc "Names a side effect to perform in response to processing this segment. If set to `:retry`, this segment will be immediately, forcibly retried from the root input task from which it emanated. This segment will not be sent to any downstream tasks."
             :type :keyword
             :choices [:retry]
             :optional? true
             :default nil
             :restrictions ["Any flow condition clauses with `:flow/action` set to `:retry` must also have `:flow/short-circuit?` set to `true`, and `:flow/to` set to `:none`."]
             :added "0.8.0"}

            :flow/doc
            {:doc "A docstring for this flow condition."
             :type :string
             :optional? true
             :added "0.8.0"}}}

   :window-entry
   {:summary "Windows allow you to group and accrue data into possibly overlapping buckets. Windows are intimately related to the Triggers feature."
    :doc-url "http://www.onyxplatform.org/docs/user-guide/latest/#windowing-and-aggregation"
    :model {:window/id
            {:doc "A unique identifier for this window."
             :type [:keyword :uuid]
             :optional? false
             :restrictions ["Must be unique across all Window entries."]
             :added "0.8.0"}

            :window/task
            {:doc "The task that this window will be applied to."
             :type :keyword
             :optional? false
             :restrictions ["Must name a task in the workflow."]
             :added "0.8.0"}

            :window/type
            {:doc "The type of Window to use. See the User Guide for what each type means."
             :type :keyword
             :choices [:fixed :sliding :global :session]
             :optional? false
             :added "0.8.0"}

            :window/storage-strategy
            {:doc "The way that window state is materialized/computed and stored. `:ordered-log`, accumulates aggregation state machine log entries, ordered by event time, in the state store. `:incremental` computes the window incrementally, and only stores the final result. If desired, both combinations may be selected, allowing for the incremental results to be queried via onyx-peer-http-query, while using the `:ordered-log` for trigger invocations. `:extents` stores the window boundaries, but not the materialized values. This is intended to be used in conjunction with `:ordered-log` so that the existing windows are known and can be materialized at any time, and is required when using session windows, if `:incremental` is not used. Please note that each choice will have a performance and space impact, with `:ordered-log` having a greater DB size impact as it must maintain all state machine updates."
             :type [:keyword]
             :choices [:ordered-log :incremental :extents]
             :default [:incremental]
             :optional? true
             :added "0.11.0"}

            :window/aggregation
            {:doc "If this value is a keyword, it is a fully qualified, namespaced keyword pointing to a symbol on the classpath at runtime. This symbol must be a map with keys as further specified by the information model. Onyx comes with a handful of aggregations built in, such as `:onyx.windowing.aggregation/min`. See the User Guide for the full list. Users can also implement their own aggregations.

                  If this value is a vector, it contain two values: a keyword as described above, and another keyword which represents the key to aggregate over."
             :type [:keyword [:keyword]]
             :optional? false
             :added "0.8.0"}

            :window/window-key
            {:doc "The key of the incoming segments to window over. This key can represent any totally ordered domain, for example `:event-time`."
             :type :any
             :optional-when ["`:window/type` is set to `:global`, and `:window/storage-strategy` includes `:ordered-log`."]
             :required-when ["`:window/type` is set to `:fixed`"
                             "`:window/type` is set to `:sliding`"
                             "`:window/type` is set to `:session`"]
             :added "0.8.0"}

            :window/min-value
            {:doc "A globally minimum value that values of `:window/window-key` will never be less than. This is used for calculating materialized aggregates for windows in a space efficient manner."
             :type :integer
             :optional? true
             :default 0
             :added "0.8.0"}

            :window/session-key
            {:doc "The key of the incoming segments to calculate a session window over."
             :type :any
             :deprecated-version "0.10.0"
             :deprecation-doc ":window/session-key has been deprecated. Please use `:onyx/group-by-key` or `:onyx/group-by-fn` to window sessions over the session-key."
             :optional? true
             :added "0.8.0"}

            :window/range
            {:doc "The span of time, or other totally ordered domain, that this window will capture data within."
             :type [:unit]
             :optional? false
             :required-when ["The `:window/type` is `:fixed` or `:sliding`."]
             :added "0.8.0"}

            :window/slide
            {:doc "To offset of time, or other totally ordered domain, to wait before starting a new window after the previous window."
             :type [:unit]
             :required-when ["The `:window/type` is `:sliding`."]
             :added "0.8.0"}

            :window/init
            {:doc "The initial value to be used for the aggregate, if required. Some aggregates require this, such as the Minimum aggregate. Others, such as the Conj aggregate, do not, as empty vector makes a suitable initial value."
             :type :any
             :required-when ["The `:window/aggregation` has no predefined initial value."]
             :added "0.8.0"}

            :window/timeout-gap
            {:doc "The duration of dormant activity that constitutes a session window being closed."
             :type :unit
             :required-when ["The `window/type` is `:session`."]
             :added "0.8.0"}

            :window/doc
            {:doc "A docstring for this window."
             :type :string
             :optional? true
             :added "0.8.0"}}}

   :state-aggregation
   {:summary "Onyx provides the ability to perform stateful updates for segments calculated over windows. For example, a grouping task may accumulate incoming values for a number of keys over windows of 5 minutes."
    :doc-url "http://www.onyxplatform.org/docs/user-guide/latest/#_aggregation"
    :model {:aggregation/init {:doc "Fn (window) to initialize the state."
                               :type :function
                               :optional? true
                               :added "0.8.0"}
            :aggregation/init-locals {:doc "Fn (window) to initialise local vars for use in other phases of the aggregation. Function should return a map that will be merged into the window map."
                                      :type :function
                                      :optional? false
                                      :added "0.11.0"}
            :aggregation/create-state-update {:doc "Fn (window, segment) to generate a serializable state machine update."
                                              :type :function
                                              :optional? false
                                              :added "0.8.0"}
            :aggregation/apply-state-update {:doc "Fn (window, state, entry) to apply state machine update entry to a state."
                                             :type :function
                                             :optional? false
                                             :added "0.8.0"}
            :aggregation/super-aggregation-fn {:doc "Fn (window, state-1, state-2) to combine two states in the case of two windows being merged, e.g. session windows."
                                               :type :function
                                               :optional? true
                                               :added "0.8.0"}}}

   :state-refinement
   {:summary "Onyx provides the ability to perform state refinements after triggers fired."
    :doc-url "http://www.onyxplatform.org/docs/user-guide/latest/#_refinement_modes"
    :model {:refinement/create-state-update {:doc "Fn (trigger, state, state-event) to generate a serializable state machine update."
                                             :type :function
                                             :optional? false
                                             :added "0.9.0"}
            :refinement/apply-state-update {:doc "Fn (trigger, state, entry) to apply the refinement state machine update entry to a state."
                                            :type :function
                                            :optional? false
                                            :added "0.9.0"}}}
   :trigger
   {:summary "Implement different trigger behaviours e.g. timers, segments, etc."
    :doc-url nil
    :model {:trigger/init-state {:doc "Fn (trigger) to initialise the state of the trigger."
                                 :type :function
                                 :optional? false
                                 :added "0.9.0"}
            :trigger/init-locals {:doc "Fn (trigger) to initialise local vars for use in other phases of the trigger. Function should return a map that will be merged into the trigger map."
                                  :type :function
                                  :optional? false
                                  :added "0.9.0"}
            :trigger/next-state {:doc "Fn (trigger, state-event) updates the trigger state in response to a state-event"
                                 :type :function
                                 :optional? false
                                 :added "0.9.0"}
            :trigger/trigger-fire? {:doc "Fn (trigger, trigger-state, state-event) returns a boolean that defines whether the trigger's sync function will be called."
                                    :type :function
                                    :optional? false
                                    :added "0.9.0"}}}
   :trigger-entry
   {:summary "Triggers are a feature that interact with Windows. Windows capture and bucket data over time. Triggers let you release the captured data over a variety of stimuli."
    :model {:trigger/window-id
            {:doc "The name of a `:window/id` window to fire the trigger against."
             :type :keyword
             :optional? false
             :restrictions ["Must name a `:window/id` in the window entries."]
             :added "0.8.0"}

            :trigger/refinement
            {:doc "A way to refine the window state after a trigger is fired. A fully qualified, namespaced keyword pointing to a symbol on the classpath at runtime. This symbol must be a map with keys as further specified by the refinement information model. As of 0.11.0, refinements are used purely to update state. Please look into `:trigger/pre-evictor` and `:trigger/post-evictor` for other methods of flushing window state."
             :type :keyword
             :optional? true
             :added "0.8.0"}

            :trigger/post-evictor
            {:doc "A way to evict window state after a trigger is fired. Currently only `[:all]`, evicting all window contents, and `[:none]`, leaving all contents, are supported."
             :example [:all]
             :type [:keyword]
             :default [:none]
             :restrictions [":all and :none are mutually exclusive."]
             :optional? true
             :added "0.11.0"}

            :trigger/state-context
            {:doc "Triggers can be used with different levels of statefulness. `:trigger/state-context` defines the context that the trigger is run in. When `:trigger-state` is used, a trigger state machine will be used, with the intermediate state results being stored in the state store. When `:window-state` is used, the current state of the window will be supplied to the trigger-fire? function, so that a trigger can be fired based on the contents of the window. Any combination of state contexts may be supplied."
             :example [:trigger-state]
             :type [:keyword]
             :default [:trigger-state]
             :optional? true
             :added "0.11.0"}

            :trigger/on
            {:doc "The event to trigger in reaction to, such as a segment with a special feature, or on a timer. See the User Guide for the full list of prepackaged Triggers. Takes a fully qualified, namespaced keyword resolving to the trigger definition. The following triggers are included with onyx: :onyx.triggers/segment, :onyx.triggers/timer, :onyx.triggers/punctuation, :onyx.triggers/watermark, :onyx.triggers/percentile-watermark"
             :type :keyword
             :optional? false
             :added "0.8.0"}

            :trigger/sync
            {:doc "A fully qualified, namespaced keyword pointing to a function on the classpath at runtime. This function takes 5 arguments: the event map, the window map that this trigger is defined on, the trigger map, a state-event map, and the window state as an immutable value. Its return value is ignored.

                  This function is invoked when the trigger fires, and is used to do any arbitrary action with the window contents, such as sync them to a database. It is called once for each trigger.

                  You can use lifecycles to supply any stateful connections necessary to sync your data. Supplied values from lifecycles will be available through the first parameter - the event map."
             :type :keyword
             :optional? true
             :added "0.8.0"}

            :trigger/emit
            {:doc "A fully qualified, namespaced keyword pointing to a function on the classpath at runtime. This function takes 5 arguments: the event map, the window map that this trigger is defined on, the trigger map, a state-event map, and the window state as an immutable value. It must return a segment, or vector of segments, which will flow downstream. Please note, Onyx does not currently provide guarantees that trigger/emit'd segments will be flushed as a result aof a `:job-completed` trigger event."
             :type :keyword
             :optional? true
             :added "0.10.0"}

            :trigger/pred
            {:doc "Used with the trigger :onyx.triggers/punctuation. A fully qualified, namespaced keyword pointing to a function on the classpath at runtime. This function takes 5 arguments: the event map, this window-id, the lower bound of this window, the upper bound of this window, and the segment. This function should return true if the trigger should fire, and false otherwise."
             :type :keyword
             :optional? false}

            :trigger/watermark-percentage
            {:doc "Used with the trigger :onyx.triggers/percentile-watermark. A double between 0.0 and 1.0, both inclusive, representing a percentage greater than the lower bound of a window. If an segment is seen with a value for a windowing key greater than this percentage, the trigger fires."
             :type :double
             :optional? false}

            :trigger/period
            {:doc "Used with the trigger :onyx.triggers/timer. A timer trigger sleeps for a duration of `:trigger/period`. When it is done sleeping, the `:trigger/sync` and/or `:trigger/emit` function is invoked with its usual arguments. The trigger goes back to sleep and repeats itself."
             :type :keyword
             :required-when ["`:trigger/on` is `:timer`"]
             :choices [:milliseconds :millisecond :seconds :second :minutes :minute :hours :hour :days :day]
             :optional? true
             :added "0.8.0"}

            :trigger/delay
            {:doc "Used with the trigger `:onyx.triggers/watermark`. A watermark trigger applies after the watermark has passed the end of a window. `:trigger/delay` adds an additional delay to the watermark, such that a window will only be triggerd after the watermark + delay."
             :type :keyword
             :choices [:milliseconds :millisecond :seconds :second :minutes :minute :hours :hour :days :day]
             :optional? true
             :added "0.11.1"}

            :trigger/threshold
            {:doc "Used with the trigger :onyx.triggers/segment. A segment trigger will fire every threshold of segments."
             :required-when ["`:trigger/on` is `:segment`"]
             :type [:integer :elements]
             :example [5 :elements]
             :optional? true
             :added "0.8.0"}

            :trigger/fire-all-extents?
            {:doc "When set to `true`, if any particular extent fires in reaction to this trigger, all extents also fire."
             :type :boolean
             :optional? true
             :default false
             :added "0.8.0"}

            :trigger/doc
            {:doc "A docstring for this trigger."
             :type :string
             :optional? true
             :added "0.8.0"}

            :trigger/id
            {:doc "An id for the trigger that is unique over the window that it is placed on. As of 0.10.0 `:trigger/id`s are required."
             :type [:keyword :uuid]
             :optional? false
             :updated "0.10.0"
             :added "0.8.0"}}}

   :event-map {:summary "Onyx exposes an 'event context' through many of its APIs. This is a description of what you will find in this map and what each of its key/value pairs mean. More keys
may be added by the user as the context is associated to throughout the task pipeline."
               :schema :onyx.schema.Event
               :type :map
               :model {:onyx.core/id {:type :uuid
                                      :doc "The unique ID of this peer's lifecycle"}
                       :onyx.core/job-name {:type :any
                                            :doc "The uniqued job name that maps to job IDs. Must be a String, Keyword, or UUID."}
                       :onyx.core/lifecycle-id {:type :uuid
                                                :optional? true
                                                :doc "The unique ID for this *execution* of the lifecycle"}
                       :onyx.core/tenancy-id {:type :any
                                              :doc "The ID for the cluster that the peers will coordinate through. Provides a means for strong, multi-tenant isolation of peers."}
                       :onyx.core/job-id {:type :uuid
                                          :doc "The Job ID of the task that this peer is executing"}
                       :onyx.core/task-id {:type :keyword
                                           :doc "The Task ID that this peer is executing"}
                       :onyx.core/slot-id {:type :integer
                                           :doc "The Task Slot ID allocated to this peer."}
                       :onyx.core/task {:type :keyword
                                        :doc "The task name that this peer is executing"}
                       :onyx.core/fn {:type :function
                                      :doc "The :onyx/fn for this task."}
                       :onyx.core/catalog {:type [:catalog-entry]
                                           :doc "The full catalog for this job"}
                       :onyx.core/workflow {:type :workflow
                                            :doc "The workflow for this job"}
                       :onyx.core/flow-conditions {:type [:flow-conditions-entry]
                                                   :doc "The flow conditions for this job"}
                       :onyx.core/lifecycles {:type [:lifecycle-entry]
                                              :doc "The lifecycle entries for this job"}
                       :onyx.core/triggers {; type should not be :any however we end up with
                                            ; recursive schema check bugs. This will be fixed.
                                            :type :any
                                            :optional? true
                                            :doc "The trigger entries for this job"}
                       :onyx.core/windows {:type [:window-entry]
                                           :doc "The window entries for this job"}
                       :onyx.core/task-map {:type :catalog-entry
                                            :doc "The catalog entry for this task"}
                       :onyx.core/serialized-task {:type :serialized-task
                                                   :doc "The task that this peer is executing that has been serialized to ZooKeeper"}
                       :onyx.core/metadata {:type :job-metadata
                                            :doc "The job's metadata, supplied via the :metadata key when submitting the job"}
                       :onyx.core/log-prefix {:type :string
                                              :doc "Logging context including more information about the task, peer and job ids."}
                       :onyx.core/params {:type [:any]
                                          :doc "The parameter sequence to be applied to the function that this task uses"}
                       :onyx.core/task-information {:type :record
                                                    :doc "Task information for this task. Mostly consists of data already in the event map."}
                       :onyx.core/log {:type :record
                                       :doc "The log record component, used to write to ZooKeeper."}
                       :onyx.core/storage {:type :record
                                           :doc "The durable storage record component, used for checkpointing."}
                       :onyx.core/task-kill-flag {:type :channel
                                                  :doc "Signalling channel used to kill the task."}
                       :onyx.core/kill-flag {:type :channel
                                             :doc "Signalling channel used to kill the peer"}
                       :onyx.core/outbox-ch {:type :channel
                                             :doc "The core.async channel to deliver outgoing log entries on"}
                       :onyx.core/group-ch {:type :channel
                                            :doc "The core.async channel to deliver restart notifications to the peer"}
                       :onyx.core/peer-opts {:type :peer-config
                                             :doc "The options that this peer was started with"}
                       :onyx.core/job-config {:type :job-config
                                              :doc "The job specific configuration."}
                       :onyx.core/replica-atom {:type :replica-atom
                                                :doc "The replica that this peer has currently accrued."}
                       :onyx.core/resume-point {:type :any
                                                :optional? true
                                                :doc "Resume point provided as part of onyx job `:resume-point` key."}
                       :onyx.core/monitoring {:type :record
                                              :doc "Onyx monitoring component implementing the [IEmitEvent](https://github.com/onyx-platform/onyx/blob/master/src/onyx/extensions.clj) protocol"}
                       :onyx.core/input-plugin {:type :any
                                                :optional? false
                                                :doc "Instantiation of the input plugin for this task."}
                       :onyx.core/output-plugin {:type :any
                                                 :optional? false
                                                 :doc "Instantiation of the input plugin for this task."}
                       :onyx.core/batch {:type [:segment]
                                         :optional? true
                                         :doc "The sequence of segments read by this peer"}
                       :onyx.core/results {:type :results
                                           :optional? true
                                           :deprecated-version "0.12.0"
                                           :deprecation-doc "Please use `:onyx.core/write-batch` whenever you wish to inspect the segments to be sent downstream / wretten to the output."
                                           :doc "A map containing `:tree`: the mapping of segments to the newly created segments, `:segments`: the newly created segments, `:retries`: the segments that will be retried from the input source."}
                       :onyx.core/transformed {:type :results
                                               :optional? true
                                               :doc "A sequence of sequences containing the segments that `:onyx/fn` produced for each segment in `:onyx.core/batch`. For example, if `:onyx.core/batch` contains `[{:n 1}]`, `:onyx.core/transformed` may contain `[[{:n 2 :type :incremented}]]`."}
                       :onyx.core/triggered {:type [:segment]
                                             :optional? true
                                             :doc "A sequential containing segments emitted by `:trigger/emit`."}
                       :onyx.core/write-batch {:type :results
                                               :optional? true
                                               :doc "A sequence of segments containing the results of `:onyx.core/transformed` and `:onyx.core/triggered`."}
                       :onyx.core/since-barrier-count {:type :AtomicInteger
                                                       :optional? true
                                                       :doc "Counts the number of segments since the last barrier."}
                       :onyx.core/scheduler-event {:type :keyword
                                                   :choices peer-scheduler-event-types
                                                   :optional? true
                                                   :doc "The cause of a peer allocated to a task being stopped. This will be added to the event map before the `:lifecycle/after-task-stop` lifecycle function is called."}}}
   :state-event
   {:summary "A state event contains context about a state update, trigger call, or refinement update. It consists of a Clojure record, with some keys being nil, depending on the context of the call e.g. a trigger call may include context about the originating cause of the trigger."
    :schema :onyx.schema.StateEvent
    :type :record
    :model {:event-type
            {:doc "The event that precipitated the state update or trigger e.g. a new segment arrived"
             :type :keyword
             :choices trigger-event-types
             :optional? false
             :added "0.9.0"}
            :task-event
            {:doc "The full Event map defined in `:event-map` of the information model"
             :type :event-map
             :optional? false
             :added "0.9.0"}
            :segment
            {:doc "The segment that caused the state event to occur. Will only be present when :event-type is :new-segment."
             :type :segment
             :optional? false
             :added "0.9.0"}
            :grouped?
            {:doc "A boolean defining whether the window state is grouped by key. Only present when event-type is :new-segment."
             :type :boolean
             :optional? true
             :added "0.9.0"}
            :group-key
            {:doc "The grouping key for the window state. Set when `:onyx/group-by-key` or `:onyx/group-by-fn` is used."
             :type :any
             :optional? false
             :added "0.9.0"}
            :lower-bound
            {:doc "The lower most value of any window key for a segment that belongs to this window. Usually coerceable to a java Date. Available in refinements, but not trigger calls. This means that :trigger/on is global over all windows."
             :type :integer
             :optional? true
             :added "0.9.0"}
            :upper-bound
            {:doc "The uppermost value of any window key for a segment that belongs to this window. Usually coerceable to a java Date. Available in refinements, but not trigger calls. This means that :trigger/on is global over all windows."
             :type :integer
             :optional? true
             :added "0.9.0"}
            :watermarks
            {:doc "Job level watermark times, effective at this peer on this task. Map takes the form `{:input millis-since-epoch :coordinator millis-since-epoch}`."
             :type :map
             :optional? false
             :added "0.11.1"}
            :checkpointed
            {:doc "A map containing the `:replica-version` and `:epoch` of a consistent snapshot that was taken for the job. This value will be supplied when the event-type is `:checkpointed`."
             :type :map
             :optional? true
             :added "0.12.0"}
            :replica-version
            {:doc "The current allocation version for this job. This represents the last time the cluster reallocated the peer topology. When combined with the `:epoch`, this represents the current barrier being processed by the task."
             :type :integer
             :optional? false
             :added "0.12.0"}
            :epoch
            {:doc "The current barrier epoch for this job since the last cluster reallocation. When combined with the `:replica-version`, this represents the current barrier being processed by the task."
             :type :integer
             :optional? false
             :added "0.12.0"}
            :window
            {:doc "The window entry associated with this state event."
             :type :window-entry
             :optional? false
             :added "0.9.0"}
            :trigger-state
            {:doc "The current trigger state after the trigger-fire? function call returns true."
             :type :any
             :optional? true
             :added "0.12.0"}
            :next-state
            {:doc "The window state that will be set after the refinement update is applied."
             :type :any
             :optional? true
             :added "0.9.0"}}}
   :lifecycle-entry
   {:summary "Lifecycles are a feature that allow you to control code that executes at particular points during task execution on each peer. Lifecycles are data driven and composable."
    :link nil
    :model {:lifecycle/task
            {:doc "The task that this lifecycle applies to."
             :type :keyword
             :optional? false
             :restrictions ["Must be a task defined in the workflow."]
             :added "0.8.0"}

            :lifecycle/calls
            {:doc "A fully qualified, namespaced keyword pointing to a symbol on the classpath at runtime. This symbol must be a map with keys further specified by the information model. The keys in this map denote the concrete functions to invoke at execution time."
             :type :keyword
             :optional? false
             :added "0.8.0"}

            :lifecycle/doc
            {:doc "A docstring for this lifecycle."
             :type :string
             :optional? true
             :added "0.8.0"}}}

   :task-states
   {:summary "Task States describes the different phases and states that the task state machine can be in. The peer moves to `:recover` mode on any change in the job allocation, before continuously cycling through the processing modes :start-iteration, :barriers, :process-batch, and :heartbeat). Some states are blocking, in that some condition must be met before advancing to the next state. Note that not all states are applicable to all tasks. For example, non-windowed tasks will strip any states related to state management and windowing."
    :model {:recover [{:lifecycle :lifecycle/poll-recover
                       :type #{:source :intermediate :sink}
                       :doc "Poll the messenger for the first recovery barrier sent by the coordinator. Once it has received the first barrier, it advances to the next state."
                       :blocking? true}
                      {:lifecycle :lifecycle/offer-barriers
                       :doc "Offers the next barrier to downstream tasks. Once it succeeds in offering the barrier to all downstream tasks, it advances to the next state."
                       :type #{:source :intermediate}
                       :blocking? true}
                      {:lifecycle :lifecycle/offer-barrier-status
                       :type #{:source :intermediate :sink}
                       :doc "Offers the peer's current status up to upstream peers. Once it succeeds in offering the status to all upstream tasks, it advances to the next state."
                       :blocking? true}
                      {:lifecycle :lifecycle/recover-input
                       :doc "Reads the checkpoint from durable storage and then supplies the checkpoint to the input plugin recover! method. Advance to the next state."
                       :type #{:source}
                       :blocking? false}
                      {:lifecycle :lifecycle/recover-state
                       :doc "Reads the checkpoint from durable storage and then supplies the checkpoint to recover the window and trigger states. Advance to the next state."
                       :blocking? false
                       :type #{:windowed}}
                      {:lifecycle :lifecycle/recover-output
                       :type #{:sink}
                       :doc "Reads the checkpoint from durable storage and then supplies the checkpoint to the output plugin recover! method. Advance to the next state."
                       :blocking? false}
                      {:lifecycle :lifecycle/unblock-subscribers
                       :type #{:source :intermediate :sink}
                       :doc "Unblock the messenger subscriptions, allowing messages to be read by the task. Advance to the next state."
                       :blocking? false}]
            :start-iteration [{:lifecycle :lifecycle/next-iteration
                               :type #{:source :intermediate :sink}
                               :doc "Resets the event map to start a new interation in the processing phase. Advance to the next state."
                               :blocking? false}]
            :barriers [{:lifecycle :lifecycle/input-poll-barriers
                        :type #{:source}
                        :doc "Poll messenger subscriptions for new barriers. Advance to the next state."
                        :blocking? false}
                       {:lifecycle :lifecycle/check-publisher-heartbeats
                        :doc "Check whether upstream has timed out directly after subscriber poll. Evict if timeout has been met. Advance to the next state."
                        :type #{:source}
                        :blocking? false}
                       {:lifecycle :lifecycle/seal-barriers?
                        :type #{:source :intermediate}
                        :doc "Check whether barriers have been received from all upstream sources. If all barriers have been received, advance to checkpoint states, otherwise advance to :lifecycle/before-read-batch."
                        :blocking? false}
                       {:lifecycle :lifecycle/seal-barriers?
                        :type #{:sink}
                        :doc "Check whether barriers have been received from all upstream sources. If all barriers have been received, advance to checkpoint states, otherwise advance to :lifecycle/before-read-batch."
                        :blocking? false}
                       {:lifecycle :lifecycle/checkpoint-input
                        :type #{:source}
                        :doc "Start checkpoint of input state. Advance to the next state."
                        :blocking? true}
                       {:lifecycle :lifecycle/checkpoint-state
                        :type #{:windowed}
                        :doc "Start checkpoint of window and trigger states. Advance to the next state."
                        :blocking? true}
                       {:lifecycle :lifecycle/checkpoint-output
                        :doc "Start checkpoint of output state. Advance to the next state."
                        :type #{:sink}
                        :blocking? true}
                       {:lifecycle :lifecycle/offer-barriers
                        :type #{:source :intermediate}
                        :doc "Offers the next barrier to downstream tasks. Once it succeeds in offering the barrier to all downstream tasks, it advances to the next state."
                        :blocking? true}
                       {:lifecycle :lifecycle/offer-barrier-status
                        :type #{:source :intermediate :sink}
                        :doc "Offers the peer's current status up to upstream peers. Once it succeeds in offering the status to all upstream tasks, it advances to the next state."
                        :blocking? true}
                       {:lifecycle :lifecycle/unblock-subscribers
                        :type #{:source :intermediate :sink}
                        :doc "Unblock the messenger subscriptions, allowing messages to be read by the task. Advance to the next state."
                        :blocking? false}]
            :process-batch [{:lifecycle :lifecycle/before-batch
                             :type #{:source :intermediate :sink}
                             :doc "Call all `:lifecycle/before-batch` fns supplied via lifecycle calls maps. Advance to the next state."
                             :blocking? false}
                            {:lifecycle :lifecycle/read-batch
                             :type #{:source :intermediate :sink}
                             :doc "Poll input source (for `:input` task) or network subscription (for `:function` task and `:output` tasks) for messages, placing these messages in `:onyx.core/batch` in the event map. Advance to the next state."
                             :blocking? false}
                            {:lifecycle :lifecycle/check-publisher-heartbeats
                             :doc "Check whether upstream has timed out directly after subscriber poll. Evict if timeout has been met. Advance to the next state."
                             :type #{:intermediate :sink}
                             :blocking? false}
                            {:lifecycle :lifecycle/after-read-batch
                             :type #{:source :intermediate :sink}
                             :blocking? false
                             :doc "Call all `:lifecycle/after-read-batch` fns supplied via lifecycle calls maps. Advance to the next state."}
                            {:lifecycle :lifecycle/apply-fn
                             :type #{:source :intermediate :sink}
                             :doc "Call `:onyx/fn` supplied for this task on each segment in `:onyx.core/batch`, placing the results in `:onyx.core/results`. Advance to the next state."
                             :blocking? false}
                            {:lifecycle :lifecycle/after-apply-fn
                             :type #{:source :intermediate :sink}
                             :doc "Call all `:lifecycle/after-apply-fn` fns supplied via lifecycle calls maps. Advance to the next state."
                             :blocking? false}
                            {:lifecycle :lifecycle/assign-windows
                             :type #{:windowed}
                             :blocking? false
                             :doc "Update windowed aggregation states, and call any trigger functions. Advance to the next state."}
                            {:lifecycle :lifecycle/prepare-segments
                             :type #{:source :intermediate :sink}
                             :doc "Copies segments from `:onyx.core/transformed` and `:onyx.core/triggered` to `:onyx.core/write-batch` prior to segments being sent to the output (either plugin medium, or task downstream)."
                             :blocking? false}
                            {:lifecycle :lifecycle/prepare-batch
                             :type #{:source :intermediate :sink}
                             :doc "Prepare batch for emission to downstream tasks or output mediums. The prepare-batch method is called on any plugins. prepare-batch is useful when output mediums may reject offers of segments, where write-batch may have to retry writes multiple times. Advance if the plugin prepare-batch method returns true, otherwise idle and retry prepare-batch."
                             :blocking? true}
                            {:lifecycle :lifecycle/write-batch
                             :type #{:source :intermediate :sink}
                             :doc "Write :onyx.core/results to output medium or message :onyx.core/results to downstream peers. write-batch will be called on any plugins. Advance to the next state if write-batch returns true, otherwise idle and retry write-batch."
                             :blocking? true}
                            {:lifecycle :lifecycle/after-batch
                             :type #{:source :intermediate :sink}
                             :doc "Call all `:lifecycle/after-batch` fns supplied via lifecycle calls maps. Advance to the next state."
                             :blocking? false}]
            :heartbeat [{:lifecycle :lifecycle/offer-heartbeats
                         :type #{:source :intermediate :sink}
                         :doc "Offer heartbeat messages to peers if it has been `:onyx.peer/heartbeat-ms` milliseconds since the previous heartbeats were sent. Set state to :lifecycle/next-iteration to perform the next task-lifecycle iteration."
                         :blocking? false}]}}

   :lifecycle-calls
   {:summary "Lifecycle calls are related to lifecycles. They consist of a map of functions that are used when resolving lifecycle entries to their corresponding functions."
    :link nil
    :model {:lifecycle/doc {:doc "A docstring for these lifecycle calls."
                            :type :string
                            :optional? true
                            :added "0.8.0"}

            :lifecycle/start-task? {:doc "A function that takes two arguments - an event map, and the matching lifecycle map. Must return a boolean value indicating whether to start the task or not. If false, the process backs off for a preconfigured amount of time and calls this task again. Useful for lock acquisition. This function is called prior to any processes inside the task becoming active."
                                    :type :function
                                    :optional? true
                                    :added "0.8.0"}

            :lifecycle/before-task-start {:doc "A function that takes two arguments - an event map, and the matching lifecycle map. This lifecycle is especially useful for injecting non-serializable resources like connections into your `:onyx/fn` params. Must return a map that is merged back into the original event map. This function is called after processes in the task are launched, but before the peer listens for incoming segments from other peers."
                                          :type :function
                                          :optional? true
                                          :added "0.8.0"}

            :lifecycle/before-batch {:doc "A function that takes two arguments - an event map, and the matching lifecycle map. Must return a map that is merged back into the original event map. This function is called prior to receiving a batch of segments from the reading function."
                                     :type :function
                                     :optional? true
                                     :added "0.8.0"}

            :lifecycle/after-read-batch {:doc "A function that takes two arguments - an event map, and the matching lifecycle map. Must return a map that is merged back into the original event map. This function is called immediately after a batch of segments has been read by the peer. The segments are available in the event map by the key `:batch`."
                                         :type :function
                                         :optional? true
                                         :added "0.8.0"}

            :lifecycle/after-apply-fn {:doc "A function that takes two arguments - an event map, and the matching lifecycle map. Must return a map that is merged back into the original event map. This function is called immediately after the `:onyx/fn` is mapped over the batch of segments."
                                         :type :function
                                         :optional? true
                                         :added "0.9.15"}

            :lifecycle/after-batch {:doc "A function that takes two arguments - an event map, and the matching lifecycle map. Must return a map that is merged back into the original event map. This function is called immediately after a batch of segments has been processed by the peer, but before the batch is acked."
                                    :type :function
                                    :optional? true
                                    :added "0.8.0"}

            :lifecycle/after-task-stop {:doc "A function that takes two arguments - an event map, and the matching lifecycle map. Must return a map that is merged back into the original event map. This function is called before the peer relinquishes its task. No more segments will be received."
                                        :type :function
                                        :optional? true
                                        :added "0.8.0"}

            :lifecycle/after-ack-segment {:doc "A function that takes four arguments - an event map, a message id, the return of an input plugin ack-segment call, and the matching lifecycle map. May return a value of any type which will be discarded. This function is whenever a segment at the input task has been fully acked."
                                          :type :function
                                          :deprecated-version "0.10.0"
                                          :deprecation-doc ":lifecycle/after-ack-segment is not supported in Onyx 0.10 as the messaging model has changed."
                                          :optional? true
                                          :added "0.8.0"}

            :lifecycle/after-retry-segment {:doc "A function that takes four arguments - an event map, a message id, the return of an input plugin ack-segment call, and the matching lifecycle map. May return a value of any type which will be discarded. This function is whenever a segment at the input task has been pending for greater than pending-timeout time and will be retried."
                                            :type :function
                                            :deprecated-version "0.10.0"
                                            :deprecation-doc ":lifecycle/after-retry-segment is not supported in Onyx 0.10 as the messaging model has changed."
                                            :optional? true
                                            :added "0.8.0"}

            :lifecycle/handle-exception {:doc "If an exception is thrown during any lifecycle execution except `after-task-stop`, one or more lifecycle handlers may be defined. If present, the exception will be caught and passed to this function,  which takes 4 arguments - an event map, the matching lifecycle map, the keyword lifecycle name from which the exception was thrown, and the exception object. This function must return `:kill`, `:restart` or `:defer` indicating whether the job should be killed, the task restarted, or the decision deferred to the next lifecycle exception handler, if another is defined. If all handlers `:defer`, the default behavior is `:kill`."
                                         :type :function
                                         :optional? true
                                         :added "0.8.3"}}}

   :job-config
  {:summary "All options available to configure the job. Parameters may override peer-config options."
    :link nil
    :model {:onyx.peer/coordinator-barrier-period-ms
            {:doc "A coordinator will send another barrier if it has been `:onyx.peer/coordinator-barrier-period-ms` ms since it last sent a barrier. Will override the peer-config option of the same name."
             :type :integer
             :unit :millisecond
             :optional? true
             :added "0.12.1"}}}

   :peer-config
   {:summary "All options available to configure the virtual peers and development environment."
    :link nil
    :model {:onyx/id
            {:doc "The ID for the cluster that the peers will coordinate via. Provides a way to provide strong, multi-tenant isolation of peers."
             :type [:one-of [:string :uuid]]
             :optional? false
             :added "0.8.0"
             :deprecated-version "0.9.0"
             :deprecation-doc "`:onyx/id` has been renamed :onyx/tenancy-id for clarity. Update all :onyx/id keys accordingly."}

            :onyx/tenancy-id
            {:doc "The ID for the cluster that the peers will coordinate through. Provides a means for strong, multi-tenant isolation of peers."
             :type [:one-of [:string :uuid]]
             :optional? false
             :added "0.9.0"}

            :onyx.peer/heartbeat-ms
            {:doc "Number of ms an idle peer should wait before sending a heartbeat message, and checking whether other peers are alive. This should be smaller than `:onyx.peer/subscriber-liveness-timeout-ms` and `:onyx.peer/publisher-liveness-timeout-ms`."
             :type :integer
             :unit :millisecond
             :default 500
             :optional? true
             :added "0.10.0"}

            :onyx.peer/idle-min-sleep-ns
            {:doc "Number of nanoseconds an idle peer should sleep for when blocked in a particular lifecycle stage. Higher numbers will reduce CPU load when peer is relatively idle. Defaults to 0.05 milliseconds, or 50000 nanoseconds."
             :type :integer
             :unit :nanosecond
             :default 50000
             :optional? true
             :added "0.10.0"}

            :onyx.peer/idle-max-sleep-ns
            {:doc "Number of nanoseconds an idle peer should sleep for when blocked in a particular lifecycle stage. Higher numbers will reduce CPU load when peer is relatively idle. Defaults to 0.5 milliseconds, or 500000 nanoseconds."
             :type :integer
             :unit :nanosecond
             :default 500000
             :optional? true
             :added "0.10.0"}

            :onyx.peer/coordinator-barrier-period-ms
            {:doc "A coordinator will send another barrier if it has been `:onyx.peer/coordinator-barrier-period-ms` ms since it last sent a barrier."
             :type :integer
             :unit :millisecond
             :default 1000
             :optional? true
             :added "0.10.0"}

            :onyx.peer/coordinator-max-sleep-ms
            {:doc "The maximum amount of time that the coordinator will sleep when there are no actions to be taken. Should be less than `:onyx.peer/coordinator-barrier-period-ms` for optimal functioning of barrier emission."
             :type :integer
             :default 10
             :unit :millisecond
             :optional? true
             :added "0.10.0"}

            :onyx.peer/subscriber-liveness-timeout-ms
            {:doc "Number of ms between heartbeats before a subscriber is determined to be dead."
             :type :integer
             :unit :millisecond
             :default 60000
             :optional? true
             :added "0.10.0"}

            :onyx.peer/publisher-liveness-timeout-ms
            {:doc "Number of ms between heartbeats before a publisher is determined to be dead."
             :type :integer
             :unit :millisecond
             :default 60000
             :optional? true
             :added "0.10.0"}

            :onyx.peer/job-scheduler
            {:doc "Each running Onyx instance is configured with exactly one job scheduler. The purpose of the job scheduler is to coordinate which jobs peers are allowed to volunteer to execute."
             :type :keyword
             :choices [:onyx.job-scheduler/percentage :onyx.job-scheduler/balanced :onyx.job-scheduler/greedy]
             :optional? false
             :added "0.8.0"}

            :onyx.peer/storage.zk.insanely-allow-windowing?
            {:doc "Allows window contents to be checkpointed with ZooKeeper. This is highly unadvised for anything but testing, as ZooKeeper checkpoints are not written asynchronously, are not automatically garbage collected, and ZooKeeper does not support znodes greater than 1MB."
             :type :boolean
             :default false
             :optional? true
             :added "0.12.0"}

            :onyx.peer.metrics/lifecycles
            {:doc "Onyx can provide metrics for all lifecycle stages. Simply provide the lifecycle stages to monitor them. Note that tracking all lifecycles may cause a performance hit depending on your workload."
             :type [:keyword]
             :default #{:lifecycle/read-batch :lifecycle/write-batch
                        :lifecycle/apply-fn :lifecycle/unblock-subscribers
                        :lifecycle/assign-windows}
             :choices [:lifecycle/poll-recover :lifecycle/offer-barriers :lifecycle/offer-barrier-status :lifecycle/recover-input :lifecycle/recover-state :lifecycle/recover-output :lifecycle/unblock-subscribers :lifecycle/next-iteration :lifecycle/input-poll-barriers :lifecycle/check-publisher-heartbeats :lifecycle/seal-barriers? :lifecycle/seal-barriers? :lifecycle/checkpoint-input :lifecycle/checkpoint-state :lifecycle/checkpoint-output :lifecycle/offer-barriers :lifecycle/offer-barrier-status :lifecycle/unblock-subscribers :lifecycle/before-batch :lifecycle/read-batch :lifecycle/check-publisher-heartbeats :lifecycle/after-read-batch :lifecycle/apply-fn :lifecycle/after-apply-fn :lifecycle/assign-windows :lifecycle/prepare-batch :lifecycle/write-batch :lifecycle/after-batch :lifecycle/offer-heartbeats]

             :optional? true
             :added "0.10.0"}

            :onyx.monitoring/config
            {:doc "Monitoring configuration. Use this to supply functions that update metrics."
             :type :any
             :default {:monitoring :no-op}
             :optional? true
             :added "0.10.0"}

            :onyx.peer/storage
            {:doc "Storage type to use for checkpointing."
             :type :keyword
             :choices [:s3 :zookeeper]
             :default :zookeeper
             :optional? true
             :added "0.10.0"}

            :onyx.peer/storage.timeout
            {:doc "Peer will timeout checkpointing after storage.timeout ms has passed."
             :type :integer
             :default 120000
             :optional? true
             :added "0.10.0"}

            :onyx.peer/storage.s3.auth-type
            {:doc "Authentication method to use for authenticating with S3 for checkpointing. The default, :provider, will use the [AWS Credentials Provider Chain](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html). Use of :config will allow both `:onyx.peer/storage.s3.auth.access-key` and `:onyx.peer/storage.s3.auth.secret-key` to be provided via the peer-config."
             :type :string
             :optional? true
             :default :provider-chain
             :added "0.10.0"}

            :onyx.peer/storage.s3.auth.access-key
            {:doc "The S3 auth secret-key key for the checkpointing module."
             :type :string
             :required-when ["`:onyx.peer/storage.s3.auth-type` is :config."]
             :optional? true
             :added "0.10.0"}

            :onyx.peer/storage.s3.auth.secret-key
            {:doc "The S3 auth access key for the checkpointing module."
             :type :string
             :required-when ["`:onyx.peer/storage.s3.auth-type` is :config."]
             :optional? true
             :added "0.10.0"}

            :onyx.peer/storage.s3.bucket
            {:doc "S3 bucket to use for checkpointing when `:onyx.peer/storage` is `:s3`."
             :type :string
             :optional? true
             :added "0.10.0"}

            :onyx.peer/storage.s3.region
            {:doc "S3 region endpoint to use for checkpointing when `:onyx.peer/storage` is `:s3`. IMPORTANT: this will not set the region on the bucket, just the endpoint used. Ensure you have created your bucket in an appropriate region."
             :type :string
             :optional? true
             :added "0.10.0"}

            :onyx.peer/storage.s3.endpoint
            {:doc "Override the default s3 endpoint."
             :type :string
             :optional? true
             :added "0.10.0"}

            :onyx.peer/storage.s3.protocol
            {:doc "Override the default s3 protocol. Useful for testing S3 compatible APIs in staging environments not served over SSL."
             :type :keyword
             :choices [:http :https]
             :default :https
             :optional? true
             :added "0.12.6"}

            :onyx.peer/storage.s3.accelerate?
            {:doc "Boolean that sets whether to use [S3 transfer acceleration](http://docs.aws.amazon.com/AmazonS3/latest/dev/transfer-acceleration.html), for use when `:onyx.peer/storage` is set to `s3`."
             :type :boolean
             :default false
             :optional? true
             :added "0.10.0"}

            :onyx.peer/storage.s3.encryption
            {:doc "Enum for which s3 encryption type to use when `:onyx.peer/storage` is set to `s3`."
             :type :keyword
             :choices [:aes256 :none]
             :default :none
             :optional? true
             :added "0.10.0"}

            :onyx.peer/storage.s3.multipart-copy-part-size
            {:doc "Sets the minimum part size in bytes for each part in a multi-part copy request. This setting may be useful when tuning checkpointing, but please benchmark your use. Sets [TransferManager Configuration](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/transfer/TransferManagerConfiguration.html#setMultipartCopyPartSize-long-) internally."
             :type :integer
             :optional? true
             :added "0.10.0"}

            :onyx.peer/storage.s3.multipart-copy-threshold
            {:doc "Sets the size threshold in bytes for when to use multipart uploads. This setting may be useful when tuning checkpointing, but please benchmark your use. Sets [TransferManager Configuration](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/transfer/TransferManagerConfiguration.html#setMultipartUploadThreshold-long-) internally."
             :type :integer
             :optional? true
             :added "0.10.0"}

            :zookeeper/address
            {:doc "The addresses of the ZooKeeper servers to use for coordination e.g. 192.168.1.1:2181,192.168.1.2:2181"
             :type :string
             :optional? false
             :added "0.8.0"}

            :onyx.peer/inbox-capacity
            {:doc "Maximum number of messages to try to pre-fetch and store in the inbox, since reading from the log happens asynchronously."
             :type :integer
             :unit :messages
             :default 1000
             :optional? true
             :added "0.8.0"}

            :onyx.peer/outbox-capacity
            {:doc "Maximum number of messages to buffer in the outbox for writing, since writing to the log happens asynchronously."
             :type :integer
             :unit :messages
             :default 1000
             :optional? true
             :added "0.8.0"}

            :onyx.peer/retry-start-interval
            {:doc "Number of ms to wait before trying to reboot a virtual peer after failure."
             :type :integer
             :unit :milliseconds
             :default 2000
             :optional? true
             :deprecated-version "0.10.0"
             :added "0.8.0"}

            :onyx.peer/join-failure-back-off
            {:doc "Mean number of ms to wait before trying to rejoin the cluster after a previous join attempt has aborted."
             :type :integer
             :unit :milliseconds
             :default 200
             :optional? true
             :added "0.8.0"}

            :onyx.peer/drained-back-off
            {:doc "Number of ms to wait before trying to complete the job if all input tasks have been exhausted."
             :type :integer
             :unit :milliseconds
             :default 100
             :optional? true
             :added "0.8.0"}

            :onyx.peer/peer-not-ready-back-off
            {:doc "Number of ms to back off and wait before retrying the call to `start-task?` lifecycle hook if it returns false."
             :type :integer
             :unit :milliseconds
             :default 100
             :optional? true
             :added "0.8.0"}

            :onyx.peer/job-not-ready-back-off
            {:doc "Number of ms to back off and wait before trying to discover configuration needed to start the subscription after discovery failure."
             :type :integer
             :unit :milliseconds
             :optional? true
             :default 100
             :added "0.8.0"}

            :onyx.peer/fn-params
            {:doc "A map of keywords to vectors. Keywords represent task names, vectors represent the first parameters to apply to the function represented by the task. For example, `{:add [42]}` for task `:add` will call the function underlying `:add` with `(f 42 <segment>)` This will apply to any job with this task name."
             :type :map
             :optional? true
             :default {}
             :added "0.8.0"}

            :onyx.peer/stop-task-timeout-ms
            {:doc "Number of ms to wait on stopping a task before allowing a peer to be scheduled to a new task"
             :type :integer
             :unit :milliseconds
             :optional? true
             :default 20000
             :added "0.9.7"}

            :onyx.peer/tags
            {:doc "Tags which denote the capabilities of this peer in terms of user-defined functionality."
             :type [:keyword]
             :optional? true
             :default []
             :added "0.8.9"}

            :onyx.peer/trigger-timer-resolution
            {:doc "The resolution of the timer firing state-events that are not caused by segments arriving."
             :type :integer
             :optional? true
             :units :milliseconds
             :deprecated-version "0.10.0"
             :deprecation-doc "Timer resolution was deprecated in 0.10.0."
             :default 100
             :added "0.9.0"}

            :onyx.peer/initial-sync-backoff-ms
            {:doc "Backoff when waiting for all of the peers to signal readiness to each other."
             :optional? true
             :type :integer
             :default 50
             :added "0.10.0"}

            :onyx.windowing/min-value
            {:doc "A default strict minimum value that `:window/window-key` can ever be. Note, this is generally best configured individually via :window/min-value in the task map."
             :type :integer
             :optional? true
             :default 0
             :added "0.8.0"}

            :onyx.zookeeper/backoff-base-sleep-time-ms
            {:doc "Initial amount of time to wait between ZooKeeper connection retries"
             :unit :milliseconds
             :optional? true
             :type :integer
             :default 1000
             :added "0.8.0"}

            :onyx.zookeeper/backoff-max-sleep-time-ms
            {:doc "Maximum amount of time in ms to sleep on each retry"
             :unit :milliseconds
             :optional? true
             :type :integer
             :default 30000
             :added "0.8.0"}

            :onyx.zookeeper/backoff-max-retries
            {:doc "Maximum number of times to retry connecting to ZooKeeper"
             :optional? true
             :type :integer
             :default 5
             :added "0.8.0"}

            :onyx.zookeeper/prepare-failure-detection-interval
            {:doc "Number of ms to wait between checking if the peer that joins this peer via prepare has failed. This value is used within a loop to periodically detect a false-positive case where a ZooKeeper ephemeral node is still present even though the process has (recently died). This value is only used within the prepare phase of joining a peer, and is not used for the normal failure detection path when a peer has fully joined the cluster."
             :unit :milliseconds
             :optional? true
             :type :integer
             :default 1000
             :added "0.8.3"}

            :onyx.task-scheduler.colocated/only-send-local?
            {:doc "When this peer is running a task for a job with a co-located task scheduler and this value is true, this peer will only send messages to segments local to its machine. It is desirable to set this to false when you want tasks to be perfectly uniformly spread over the machines in your cluster, but do not want jobs to run entirely locally."
             :optional? true
             :type :boolean
             :default true
             :added "0.8.4"}

            :onyx.log/config
            {:doc "Timbre logging configuration for the peers. See [Logging](http://www.onyxplatform.org/docs/user-guide/latest/logging.html)."
             :optional? true
             :type :map
             :added "0.6.0"}

            :onyx.messaging/decompress-fn
            {:doc "The Clojure function to use for messaging decompression. Receives one argument - a byte array. Must return the decompressed value of the byte array."
             :optional? true
             :type :function
             :deprecated-version "0.10.0"
             :deprecation-doc "Custom serialization functions are currently deprecated, however they may return in the future if there is demand."
             :default 'onyx.compression.nippy/decompress
             :added "0.8.0"}

            :onyx.messaging/term-buffer-size.coordinator
            {:doc "Coordinator messenger buffer size. Used for messages between the coordinator and input peers. This option should nearly never need to be changed as it is used for sending small barrier messages."
             :optional? true
             :added "0.12.0"
             :restrictions ["Parameter must be a power of 2."]
             :default 524288}

            :onyx.messaging/term-buffer-size.heartbeat
            {:doc "Control heartbeat messenger buffer size. Used for heartbeating and backpressure signals This option should nearly never need to be changed as it is used for sending small messages."
             :optional? true
             :added "0.12.0"
             :restrictions ["Parameter must be a power of 2."]
             :default 1048576}

            :onyx.messaging/term-buffer-size.segment
            {:doc "Segment messenger buffer size. Used to send segments to downstream peers. Maximum segment size is dictated by this buffer size, where the maximum segment size = term-buffer-size.segment / 8. The default size will allow for segments of approximately 262,144 bytes after protocol message headers are accountered for. Adjust this parameter (carefully) if you wish to tune throughput vs memory usage. Note that each peer may have a term buffer to each downstream peer, and thus you should expect up to n^2 of these buffers, each of 3 * term-buffer-size.segment size in bytes."
             :optional? true
             :added "0.12.0"
             :restrictions ["Parameter must be a power of 2."]
             :default 2097152}

            :onyx.messaging/term-buffer-size.segment-short-circuit
            {:doc "Short circuiting segment messenger buffer size. Used to send segments to downstream peers located on the same node. Generally this parameter will not need to be tuned, as most of the messaging takes place via a local buffer, not the messenger buffer."
             :optional? true
             :added "0.12.0"
             :restrictions ["Parameter must be a power of 2."]
             :default 524288}

            :onyx.messaging/compress-fn
            {:doc "The Clojure function to use for messaging compression. Receives one argument - a sequence of segments. Must return a byte array representing the segment seq."
             :optional? true
             :type :function
             :deprecated-version "0.10.0"
             :deprecation-doc "Custom serialization functions are currently deprecated, however they may return in the future if there is demand."
             :default 'onyx.compression.nippy/compress
             :added "0.8.0"}

            :onyx.messaging/impl
            {:doc "The messaging protocol to use for peer-to-peer communication."
             :optional? false
             :type :keyword
             :choices [:aeron]
             :added "0.8.0"}

            :onyx.messaging/bind-addr
            {:doc "An IP address to bind the peer to for messaging. Defaults to `nil`. On AWS EC2, it's generally enough to configure this to the result of `(slurp http://169.254.169.254/latest/meta-data/local-ipv4)`"
             :optional? false
             :type :string
             :default nil
             :added "0.8.0"}

            :onyx.messaging/external-addr
            {:doc "An IP address to advertise to other peers. Useful in case of firewalling, port forwarding, etc, where the interface/IP that is bound is different to the address that other peers should connect to."
             :optional? true
             :type :string
             :default nil
             :added "0.8.0"}

            :onyx.messaging/peer-port
            {:doc "Port that peers should use to communicate."
             :optional? false
             :type :integer
             :default nil
             :added "0.8.0"}

            :onyx.messaging/allow-short-circuit?
            {:doc "A boolean denoting whether to allow virtual peers to short circuit networked messaging when co-located with the other virtual peer. Short circuiting allows for direct transfer of messages to a virtual peer's internal buffers, which improves performance where possible. This configuration option is primarily for use in performance testing, as peers will not generally be able to short circuit messaging after scaling to many nodes."
             :optional? true
             :type :boolean
             :default true
             :added "0.8.0"}

            :onyx.messaging/short-circuit-buffer-size
            {:doc "Maximum number of batches multiplied by consuming peer, per short circuit buffer. This affects memory consumption, and performance."
             :optional? true
             :default 50
             :type :integer
             :added "0.10.0"}

            :onyx.messaging.aeron/embedded-driver?
            {:doc "A boolean denoting whether an Aeron media driver should be started up with the environment. See [this example](https://github.com/onyx-platform/onyx/blob/026dce2ca5494999e0abe3deeb5e9d0fdc7ef09f/src/onyx/messaging/aeron_media_driver.clj) for an example for how to start the media driver externally."
             :optional? true
             :type :boolean
             :default true
             :added "0.8.0"}

            :onyx.messaging.aeron/embedded-media-driver-delete-dirs-on-start?
            {:doc "A boolean denoting whether an Aeron media driver should delete its directory on startup. Using this option runs the risk of multiple media drivers stepping on each other, and should be avoided unless you are sure this isn't occurring."
             :optional? true
             :type :boolean
             :default false
             :added "0.12.0"}

            :onyx.messaging.aeron/embedded-media-driver-threading
            {:doc "Threading mode to use with the embedded media driver."
             :optional? true
             :type :keyword
             :choices [:dedicated :shared :shared-network]
             :default :shared
             :added "0.9.0"}

            :onyx.query/server?
            {:doc "Bool to denote wether the peer-group should start a http server that can be queried for replica state and job information"
             :type :boolean
             :optional? true
             :added "0.9.10"}

            :onyx.query.server/ip
            {:doc "The IP the http query server should listen on."
             :type :string
             :optional? true
             :default "0.0.0.0"
             :added "0.9.10"}

            :onyx.query.server/port
            {:doc "The port the http query server should liston on"
             :type :integer
             :optional? true
             :default 8080}}}

   :env-config
   {:summary "All options available to configure the node environment."
    :link nil
    :model {:zookeeper/server?
            {:doc "Bool to denote whether to startup a local, in-memory ZooKeeper. **Important: for TEST purposes only.**"
             :type :boolean
             :optional? true
             :added "0.8.0"}

            :zookeeper.server/port
            {:doc "Port to use for the local in-memory ZooKeeper"
             :type :integer
             :required-when ["The `:zookeeper/server?` is `true`."]
             :added "0.8.0"}

            :onyx/tenancy-id
            {:doc "The ID for the cluster that the peers will coordinate via. Provides a way to provide strong, multi-tenant isolation of peers."
             :type [:one-of [:string :uuid]]
             :optional? false
             :added "0.9.0"}

            :onyx/id
            {:doc "The ID for the cluster that the peers will coordinate via. Provides a way to provide strong, multi-tenant isolation of peers."
             :type [:one-of [:string :uuid]]
             :required-when ["`:onyx.bookkeeper/server?` is `true`."]
             :optional? true
             :added "0.8.0"
             :deprecated-version "0.9.0"
             :deprecation-doc ":onyx/id has been renamed :onyx/tenancy-id for clarity. Update all :onyx/id keys accordingly."}

            :zookeeper/address
            {:doc "The addresses of the ZooKeeper servers to use for coordination e.g. 192.168.1.1:2181,192.168.1.2:2181"
             :type :string
             :optional? false
             :added "0.8.0"}

            :onyx.bookkeeper/server?
            {:doc "Bool to denote whether to startup a BookKeeper instance on this node, for use in persisting Onyx state information."
             :type :boolean
             :default false
             :optional? true
             :added "0.8.0"}

            :onyx.bookkeeper/delete-server-data?
            {:doc "Bool to denote whether to delete all BookKeeper server instance data on environment shutdown. Set to true when using BookKeeper for unit/integration test runs."
             :type :boolean
             :default false
             :optional? true
             :added "0.8.0"}

            :onyx.bookkeeper/local-quorum?
            {:doc "Bool to denote whether to startup a full quorum of BookKeeper instances on this node. **Important: for TEST purposes only.**"
             :default false
             :type :boolean
             :required-when ["The `:onyx.bookkeeper/server?` is `true` and `:onyx.bookkeeper/local-quorum?` is `true`"]
             :optional? true
             :added "0.8.0"}

            :onyx.bookkeeper/local-quorum-ports
            {:doc "Ports to use for the local BookKeeper quorum."
             :type :vector
             :default [3196 3197 3198]
             :required-when ["The `:onyx.bookkeeper/server?` is `true` and `:onyx.bookkeeper/local-quorum?` is `true`"]
             :optional? true
             :added "0.8.0"}

            :onyx.bookkeeper/port
            {:doc "Port to startup this node's BookKeeper instance on."
             :type :integer
             :default 3196
             :required-when ["The `:onyx.bookkeeper/server?` is `true` and `:onyx.bookkeeper/local-quorum?` is `false`"]
             :added "0.8.0"}

            :onyx.bookkeeper/base-journal-dir
            {:doc "Directory to store BookKeeper's journal in. It is recommended that this is altered to somewhere fast, preferably on a different disk to the BookKeeper ledger."
             :type :string
             :default "/tmp/bookkeeper_journal"
             :optional? true
             :added "0.8.0"}

            :onyx.bookkeeper/base-ledger-dir
            {:doc "Directory to store BookKeeper's ledger in. It is recommended that this is altered to somewhere fast, preferably on a different disk to the BookKeeper journal"
             :type :string
             :default "/tmp/bookkeeper_ledger"
             :optional? true
             :added "0.8.0"}

            :onyx.bookkeeper/disk-usage-threshold
            {:doc "Fraction of the total utilized usable disk space to declare the disk full. The value of this parameter represents a percentage."
             :optional? true
             :type :double
             :default 0.98
             :added "0.8.4"}

            :onyx.bookkeeper/disk-usage-warn-threshold
            {:doc "Fraction of the total utilized usable disk space to warn about disk usage. The value of this parameter represents a percentage. It needs to lower or equal than the :onyx.bookkeeper/disk-usage-threshold"
             :optional? true
             :type :double
             :default 0.95
             :added "0.8.4"}
            :onyx.bookkeeper/zk-ledgers-root-path
            {:doc "Root zookeeper path to store ledger metadata."
             :optional? true
             :type :string
             :default "/ledgers"
             :added "0.9.8"}}}})

(defn version-deprecations [version]
  (->> model
       (map (fn [[k m]]
              [k (mapv key (filter (fn [[option doc]]
                                     (= version (:deprecated-version doc)))
                                   (:model m)))]))

       (remove (comp empty? second))
       (into {})))

(def model-display-order
  {:job [:job-name :workflow :catalog :flow-conditions :windows
         :triggers :metadata :lifecycles :job-config
         :resume-point :task-scheduler :percentage]
   :catalog-entry
   [:onyx/name
    :onyx/type
    :onyx/batch-size
    :onyx/batch-timeout
    :onyx/doc
    :onyx/min-peers
    :onyx/max-peers
    :onyx/n-peers
    :onyx/language
    :onyx/params
    :onyx/medium
    :onyx/plugin
    :onyx/max-segments-per-barrier
    :onyx/fn
    :onyx/assign-watermark-fn
    :onyx/batch-fn?
    :onyx/group-by-key
    :onyx/group-by-fn
    :onyx/flux-policy
    :onyx/required-tags
    :onyx/bulk?
    :onyx/restart-pred-fn
    :onyx/uniqueness-key
    :onyx/deduplicate?
    :onyx/pending-timeout
    :onyx/input-retry-timeout
    :onyx/max-pending]
   :flow-conditions-entry
   [:flow/from :flow/to :flow/predicate :flow/predicate-errors-to :flow/exclude-keys :flow/short-circuit?
    :flow/thrown-exception?  :flow/post-transform :flow/action :flow/doc]
   :window-entry
   [:window/id :window/task :window/type :window/aggregation :window/window-key
    :window/min-value :window/session-key :window/range :window/slide :window/storage-strategy
    :window/init :window/timeout-gap :window/doc]
   :state-aggregation
   [:aggregation/init
    :aggregation/init-locals
    :aggregation/create-state-update
    :aggregation/apply-state-update
    :aggregation/super-aggregation-fn]
   :trigger-entry
   [:trigger/window-id :trigger/refinement :trigger/on :trigger/sync :trigger/emit :trigger/id
    :trigger/period :trigger/threshold :trigger/pred :trigger/watermark-percentage :trigger/fire-all-extents?
    :trigger/state-context :trigger/post-evictor :trigger/doc :trigger/delay]
   :lifecycle-entry
   [:lifecycle/task :lifecycle/calls :lifecycle/doc]
   :lifecycle-calls
   [:lifecycle/doc
    :lifecycle/start-task?
    :lifecycle/before-task-start
    :lifecycle/before-batch
    :lifecycle/after-read-batch
    :lifecycle/after-apply-fn
    :lifecycle/after-batch
    :lifecycle/after-task-stop
    :lifecycle/after-ack-segment
    :lifecycle/after-retry-segment
    :lifecycle/handle-exception]
   :job-config [:onyx.peer/coordinator-barrier-period-ms]
   :peer-config
   [:onyx/tenancy-id
    :zookeeper/address
    :onyx.log/config
    :onyx.monitoring/config
    :onyx.peer.metrics/lifecycles
    :onyx.peer/job-scheduler
    :onyx.peer/publisher-liveness-timeout-ms
    :onyx.peer/coordinator-max-sleep-ms
    :onyx.peer/subscriber-liveness-timeout-ms
    :onyx.peer/coordinator-barrier-period-ms
    :onyx.peer/heartbeat-ms
    :onyx.messaging/term-buffer-size.coordinator
    :onyx.messaging/term-buffer-size.heartbeat
    :onyx.messaging/term-buffer-size.segment
    :onyx.messaging/term-buffer-size.segment-short-circuit
    :onyx.peer/idle-min-sleep-ns
    :onyx.peer/idle-max-sleep-ns
    :onyx.peer/stop-task-timeout-ms
    :onyx.peer/inbox-capacity
    :onyx.peer/outbox-capacity
    :onyx.peer/storage
    :onyx.peer/storage.timeout
    :onyx.peer/storage.zk.insanely-allow-windowing?
    :onyx.peer/storage.s3.auth-type
    :onyx.peer/storage.s3.auth.access-key
    :onyx.peer/storage.s3.auth.secret-key
    :onyx.peer/storage.s3.bucket
    :onyx.peer/storage.s3.region
    :onyx.peer/storage.s3.endpoint
    :onyx.peer/storage.s3.accelerate?
    :onyx.peer/storage.s3.encryption
    :onyx.peer/storage.s3.multipart-copy-part-size
    :onyx.peer/storage.s3.multipart-copy-threshold
    :onyx.peer/storage.s3.protocol
    :onyx.peer/retry-start-interval
    :onyx.peer/join-failure-back-off
    :onyx.peer/drained-back-off
    :onyx.peer/peer-not-ready-back-off
    :onyx.peer/job-not-ready-back-off
    :onyx.peer/fn-params
    :onyx.windowing/min-value
    :onyx.peer/trigger-timer-resolution
    :onyx.peer/tags
    :onyx.peer/initial-sync-backoff-ms
    :onyx.zookeeper/backoff-base-sleep-time-ms
    :onyx.zookeeper/backoff-max-sleep-time-ms
    :onyx.zookeeper/backoff-max-retries
    :onyx.zookeeper/prepare-failure-detection-interval
    :onyx.query/server?
    :onyx.query.server/ip
    :onyx.query.server/port
    :onyx.messaging/decompress-fn
    :onyx.messaging/compress-fn :onyx.messaging/impl :onyx.messaging/bind-addr
    :onyx.messaging/external-addr :onyx.messaging/peer-port
    :onyx.messaging.aeron/embedded-driver?
    :onyx.messaging.aeron/embedded-media-driver-threading
    :onyx.messaging.aeron/embedded-media-driver-delete-dirs-on-start?
    :onyx.messaging/allow-short-circuit?
    :onyx.messaging/short-circuit-buffer-size
    :onyx.task-scheduler.colocated/only-send-local?
    :onyx/id]
   :trigger [:trigger/init-state :trigger/init-locals :trigger/next-state :trigger/trigger-fire?]
   :state-refinement [:refinement/create-state-update :refinement/apply-state-update]
   :state-event [:event-type :task-event :segment :grouped? :group-key :lower-bound
                 :upper-bound :window :next-state :watermarks :checkpointed :trigger-state :replica-version :epoch]
   :task-states [:recover :start-iteration :barriers :process-batch :heartbeat]
   :event-map [:onyx.core/job-name
               :onyx.core/task-map
               :onyx.core/catalog
               :onyx.core/workflow
               :onyx.core/flow-conditions
               :onyx.core/windows
               :onyx.core/triggers
               :onyx.core/lifecycles
               :onyx.core/resume-point
               :onyx.core/fn
               :onyx.core/job-config
               :onyx.core/params
               :onyx.core/metadata
               :onyx.core/batch
               :onyx.core/write-batch
               :onyx.core/transformed
               :onyx.core/triggered
               :onyx.core/since-barrier-count
               :onyx.core/id
               :onyx.core/job-id
               :onyx.core/task
               :onyx.core/task-id
               :onyx.core/slot-id
               :onyx.core/lifecycle-id
               :onyx.core/scheduler-event
               :onyx.core/tenancy-id
               :onyx.core/peer-opts
               :onyx.core/replica-atom
               :onyx.core/task-information
               :onyx.core/group-ch
               :onyx.core/outbox-ch
               :onyx.core/kill-flag
               :onyx.core/task-kill-flag
               :onyx.core/log-prefix
               :onyx.core/job-config
               :onyx.core/serialized-task
               :onyx.core/log
               :onyx.core/storage
               :onyx.core/input-plugin
               :onyx.core/output-plugin
               :onyx.core/monitoring
               :onyx.core/results]
   :env-config
   [:onyx/tenancy-id
    :zookeeper/server?
    :zookeeper.server/port
    :zookeeper/address
    :onyx.bookkeeper/server?
    :onyx.bookkeeper/delete-server-data?
    :onyx.bookkeeper/local-quorum?
    :onyx.bookkeeper/local-quorum-ports :onyx.bookkeeper/port
    :onyx.bookkeeper/base-journal-dir
    :onyx.bookkeeper/base-ledger-dir
    :onyx.bookkeeper/disk-usage-threshold
    :onyx.bookkeeper/disk-usage-warn-threshold
    :onyx.bookkeeper/zk-ledgers-root-path
    :onyx/id]})
