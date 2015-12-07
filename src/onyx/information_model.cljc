(ns onyx.information-model)

(def model
  {:catalog-entry
   {:summary "All inputs, outputs, and functions in a workflow must be described via a catalog. A catalog is a vector of maps, strikingly similar to Datomic’s schema. Configuration and docstrings are described in the catalog."
    :link nil
    :model {:onyx/name
            {:doc "The name of the task that represents this catalog entry. Must correspond to a keyword in the workflow associated with this catalog."
             :type :keyword
             :choices :any
             :tags [:task]
             :restrictions ["Must be unique across all catalog entries."
                            "Value cannot be `:none`."
                            "Value cannot be `:all`."]
             :optional? false}

            :onyx/type
            {:doc "The role that this task performs. `:input` reads data. `:function` applies a transformation. `:output` writes data."
             :type :keyword
             :tags [:task]
             :choices [:input :function :output]
             :optional? false}

            :onyx/batch-size
            {:doc "The number of segments a peer will wait to read before processing them all in a batch for this task. Segments will be processed when either `:onyx/batch-size` segments have been received at this peer, or `:onyx/batch-timeout` milliseconds have passed - whichever comes first. This is a knob that is used to tune throughput and latency, and it goes hand-in-hand with `:onyx/batch-timeout`."
             :type :integer
             :tags [:latency :throughput]
             :restrictions ["Value must be greater than 0."]
             :optional? false}

            :onyx/batch-timeout
            {:doc "The number of milliseconds a peer will wait to read more segments before processing them all in a batch for this task. Segments will be processe when either `:onyx/batch-timeout` milliseconds passed, or `:onyx/batch-size` segments have been read - whichever comes first. This is a knob that is used to tune throughput and latency, and it goes hand-in-hand with `:onyx/batch-size`."
             :type :integer
             :unit :milliseconds
             :tags [:latency :throughput]
             :restrictions ["Value must be greater than 0."]
             :default 50
             :optional? true}

            :onyx/doc
            {:doc "A docstring for this catalog entry."
             :type :string
             :tags [:documentation]
             :optional? true}

            :onyx/max-peers
            {:doc "The maximum number of peers that will ever be assigned to this task concurrently."
             :type :integer
             :tags [:aggregation :grouping]
             :restrictions ["Value must be greater than 0."]
             :optional? true}

            :onyx/min-peers
            {:doc "The minimum number of peers that will be concurrently assigned to execute this task before it begins. If the number of peers working on this task falls below its initial count due to failure or planned departure, the choice of `:onyx/flux-policy` defines the strategy for what to do."
             :type :integer
             :tags [:aggregation :grouping]
             :restrictions ["Value must be greater than 0."]
             :optional? true}

            :onyx/n-peers
            {:doc "A convenience parameter which expands to `:onyx/min-peers` and `:onyx/max-peers` set to the same value. This is useful if you want to specify exactly how many peers should concurrently execute this task - no more, and no less."
             :type :integer
             :tags [:aggregation :grouping]
             :restrictions ["Value must be greater than 0."
                            "`:onyx/min-peers` cannot also be defined for this catalog entry."
                            "`:onyx/max-peers` cannot also be defined for this catalog entry."]
             :optional? true}

            :onyx/language
            {:doc "Designates the language that the function denoted by `:onyx/fn` is implemented in."
             :type :keyword
             :tags [:interoperability]
             :choices [:clojure :java]
             :default :clojure
             :optional? true}

            :onyx/restart-pred-fn
            {:doc "A fully-qualified namespaced keyword pointing to function which takes an exception as a parameter, returning a boolean indicating whether the peer that threw this exception should restart its task."
             :type :keyword
             :choices :any
             :tags [:fault-tolerance]
             :restrictions ["Must resolve to a function on the classpath at runtime."]
             :optional? true}

            :onyx/params
            {:doc "A vector of keys to obtain from the task map, and inject into the parameters of the function defined in :onyx/fn."
             :type :vector
             :tags [:function]
             :optional? true}

            :onyx/medium
            {:doc "Denotes the kind of input or output communication or storage that is being read from or written to (e.g. `:kafka` or `:web-socket`). This is currently does not affect any functionality, and is reserved for the future."
             :type :keyword
             :tags [:plugin]
             :choices :any
             :required-when ["`:onyx/type` is set to `:input`"
                             "`:onyx/type` is set to `:output`"]}

            :onyx/plugin
            {:doc "When `:onyx/language` is set to `:clojure`, this is a fully qualified, namespaced keyword pointing to a function that takes the Event map and returns a Record implementing the Plugin interfaces. When `:onyx/language` is set to `:java`, this is a keyword pointing to a Java class that is constructed with the Event map. This class must implement the interoperability interfaces."
             :type :keyword
             :tags [:plugin]
             :choices :any
             :restrictions ["Namespaced keyword required unless :onyx/language :java is set, in which case a non-namespaced keyword is required."]
             :required-when ["`:onyx/type` is set to `:input`"
                             "`:onyx/type` is set to `:output`"]}

            :onyx/pending-timeout
            {:doc "The duration of time, in milliseconds, that a segment that enters an input task has to be fully acknowledged and processed. That is, this segment, and any subsequent segments that it creates in downstream tasks, must be fully processed before this timeout occurs. If the segment is not fully processed, it will automatically be retried."
             :type :integer
             :default 60000
             :tags [:input :plugin :latency :fault-tolerance]
             :units :milliseconds
             :optionally-allowed-when ["`:onyx/type` is set to `:input`"
                                       "Value must be greater than 0."]}

            :onyx/input-retry-timeout
            {:doc "The duration of time, in milliseconds, that the input task goes dormant between checking which segments should expire from its internal pending pool. When segments expire, they are automatically retried."
             :type :integer
             :default 1000
             :tags [:input :plugin :latency :fault-tolerance]
             :units :milliseconds
             :optionally-allowed-when ["`:onyx/type` is set to `:input`"
                                       "Value must be greater than 0."]}

            :onyx/max-pending
            {:doc "The maximum number of segments that a peer executing an input task will allow in its internal pending message pool. If this pool is filled to capacity, it will not accept new segments - exhibiting backpressure to upstream message produces."
             :type :integer
             :default 10000
             :tags [:input :plugin :latency :backpressure :fault-tolerance]
             :units :segments
             :optionally-allowed-when ["`:onyx/type` is set to `:input`"
                                       "Value must be greater than 0."]}

            :onyx/fn
            {:doc "A fully qualified, namespaced keyword that points to a function on the classpath. This function takes at least one argument - an incoming segment, and returns either a segment or a vector of segments. This function may not return `nil`. This function can be parameterized further through a variety of techniques."
             :type :keyword
             :tags [:function]
             :required-when ["`:onyx/type` is set to `:function`"]
             :optionally-allowed-when ["`:onyx/type` is set to `:input`"
                                       "`:onyx/type` is set to `:output`"]}

            :onyx/group-by-key
            {:doc "The key, or vector of keys, to group incoming segments by. Keys that hash to the same value will always be sent to the same virtual peer."
             :type [:any [:any]]
             :tags [:aggregation :grouping :windows]
             :optionally-allowed-when ["`:onyx/type` is set to `:function` or `:output`"]
             :restrictions ["Cannot be defined when `:onyx/group-by-fn` is defined."
                            "`:onyx/flux-policy` must also be defined in this catalog entry."]}

            :onyx/group-by-fn
            {:doc "A fully qualified, namespaced keyword that points to a function on the classpath. This function takes a single argument, a segment, as a parameter. The value that the function returns will be hashed. Values that hash to the same value will always be sent to the same virtual peer."
             :type :keyword
             :tags [:aggregation :grouping :windows :function]
             :optionally-allowed-when ["`:onyx/type` is set to `:function` or `:output`"]
             :restrictions ["Cannot be defined when `:onyx/group-by-key` is defined."
                            "`:onyx/flux-policy` must also be defined in this catalog entry."]}

            :onyx/bulk?
            {:doc "Boolean value indicating whether the function in this catalog entry denoted by `:onyx/fn` should take a single segment, or the entire batch of segments that were read as a parameter. When set to `true`, this function's return value is ignored. The segments are identically propogated to the downstream tasks."
             :type :boolean
             :default false
             :tags [:function]
             :optionally-allowed-when ["`:onyx/type` is set to `:function`"]}

            :onyx/flux-policy
            {:doc "The policy that should be used when a task with grouping enabled loses a peer. Losing a peer means that the consistent hashing used to pin the same hashed values to the same peers will be altered. Using the `:kill` flux policy will kill the job. This is useful for jobs that cannot tolerate an altered hashing strategy. Using `:continue` will allow the job to continue running. With `:kill` and `:continue`, new peers will never be added to this job. The final policy is `:recover`, which is like `:continue`, but will allow peers to be added back to this job to meet the `:onyx/min-peers` number of peers working on this task concurrently."
             :type :keyword
             :choices [:kill :continue :recover]
             :tags [:aggregation :grouping :windows]
             :restrictions ["`:onyx/min-peers` or `:onyx/n-peers` must also be defined for this catalog entry. If `:recover` is used, then `:onyx/max-peers` or `:onyx/n-peers`` must also be defined. "]
             :optionally-allowed-when ["`:onyx/type` is set to `:function` or `:output`"
                                       "`:onyx/group-by-key` or `:onyx/group-by-fn` is set."]}

            :onyx/uniqueness-key
            {:doc "The key of incoming segments that indicates global uniqueness. This is used by the Windowing feature to detect duplicated processing of segments. An example of this would be an `:id` key for segments representing users, assuming `:id` is globally unique in your system. An example of a bad uniqueness-key would be `:first-name` as two or more users may have their first names in common."
             :type :any
             :tags [:aggregation :windows]
             :required-when ["A Window is defined on this task."]}
            
            :onyx/deduplicate?
            {:doc "Does not deduplicate segments using the `:onyx/uniqueness-key`, which is otherwise required when using windowed tasks. Often useful if your segments do not have a unique key that you can use to filter incoming replayed or duplicated segments."
             :type :boolean
             :default true
             :tags [:aggregation :windows]
             :optionally-allowed-when ["A window is defined on this task."]
             :required-when ["A Window is defined on this task and there is no possible :onyx/uniqueness-key to on the segment to deduplicate with."]}}}

   :flow-conditions-entry
   {:summary "Flow conditions are used for isolating logic about whether or not segments should pass through different tasks in a workflow, and support a rich degree of composition with runtime parameterization."
    :link nil
    :model {:flow/from
            {:doc "The source task from which segments are being sent."
             :type :keyword
             :optional? false
             :restrictions ["Must name a task in the workflow."]}

            :flow/to
            {:doc "The destination task where segments will arrive. If set to `:all`, all downstream tasks will receive this segment. If set to `:none`, no downstream tasks will receive this segment. Otherwise it must name a vector of keywords indicating downstream tasks. The order of keywords is irrelevant."
             :type [:keyword [:keyword]]
             :choices [[:any] :all :none]
             :optional? false
             :restrictions ["When the value is a vector of keyword, every keyword must name a task in the workflow."]}

            :flow/predicate
            {:doc "When denoted as a keyword, this must be a fully qualified, namespaced keyword pointing to a function on the classpath at runtime. This function takes at least 4 arguments - the Event map, the old segment before `:onyx/fn` was applied, the new segment after `:onyx/fn` was applied, and the sequence of new segments generated by the old semgent. If the old segment generated exactly one segment, and not a sequence of segments, the value of the last parameter will be a collection with only the new segment in it.

                  When denoted as a vector of keywords, the first value in the vector  may either be the keyword `:and`, `:or`, or `:not`, or be a keyword as described above. In the latter case, any subsequent values must be keywords that resolve to keys in the flow condition entries map. The values of these keys are resolved and passed as additional parameters to the function. In the former case, the result of the function (which may again be wrapped with a vector to nest logical operators or parameters), is applied with the designated logical operator. This yields predicate composition."
             :type [:keyword [:keyword]]
             :optional? false}

            :flow/exclude-keys
            {:doc "If any of the keys are present in the segment, they will be `dissoc`ed from the segment before it is sent downstream. This is useful when values in the segment are present purely for the purpose of making a decision about which downstream tasks it should be sent to."
             :type [[:keyword]]
             :optional? true}

            :flow/short-circuit?
            {:doc "When multiple flow condition entry predicates evaluated to true, the tasks in `:flow/to` are set unioned. If this behavior is undesirable, and you want exactly the tasks in this flow condition's `:flow/to` key to be used, plus any previously matched flow conditions `:flow/to` values. Setting `:flow/short-circuit?` to `true` will force the matcher to stop executing and immediately return with the values that it matched."
             :type :boolean
             :optional? true
             :default false
             :restrictions ["Any entry that has :flow/short-circuit? set to true must come before any entries for an task that have it set to false or nil."]}

            :flow/thrown-exception?
            {:doc "If an exception is thrown from an Onyx transformation function, you can capture it from within your flow conditions by setting this value to `true`. If an exception is thrown, only flow conditions with `:flow/thrown-exception?` set to `true` will be evaluated. The value that is normally the segment which is sent to the predicate will be the exception object that was thrown. Note that exceptions don't serialize. This feature is meant to be used in conjunction with Post-transformations and Actions for sending exception values to downstream tasks."
             :type :boolean
             :optional? true
             :default false
             :restrictions ["Exception flow conditions must have `:flow/short-circuit?` set to `true`"]}

            :flow/post-transform
            {:doc "A fully qualified, namespaced keyword that points to a function on the classpath at runtime. This function is invoked when an exception is thrown processing a segment in `:onyx/fn` and this flow condition's predicate evaluates to `true`. The function takes 3 parameters - the Event map, the segment that causes the exception to be thrown, and the exception object. The return value of this function is sent to the downstream tasks instead of trying to serialize the exception. The return value must be a segment or sequence of segments, and must serialize."
             :type :keyword
             :optional? true
             :default nil
             :restrictions ["`:flow/thrown-exception?` must be set to `true`."]}

            :flow/action
            {:doc "Names a side effect to perform in response to processing this segment. If set to `:retry`, this segment will be immediately, forcibly retried from the root input task from which it eminated. This segment will not be sent to any downstream tasks."
             :type :keyword
             :choices [:retry]
             :optional? true
             :default nil
             :restrictions ["Any flow condition clauses with `:flow/action` set to `:retry` must also have `:flow/short-circuit?` set to `true`, and `:flow/to` set to `:none`."]}

            :flow/doc
            {:doc "A docstring for this flow condition."
             :type :string
             :optional? true}}}

   :window-entry
   {:summary "Windows allow you to group and accrue data into possibly overlapping buckets. Windows are intimately related to the Triggers feature."
    :link nil
    :model {:window/id
            {:doc "A unique identifier for this window."
             :type :keyword
             :optional? false
             :restrictions ["Must be unique across all Window entries."]}

            :window/task
            {:doc "The task that this window will be applied to."
             :type :keyword
             :optional? false
             :restrictions ["Must name a task in the workflow."]}

            :window/type
            {:doc "The type of Window to use. See the User Guide for what each type means."
             :type :keyword
             :choices [:fixed :sliding :global :session]
             :optional? false}

            :window/aggregation
            {:doc "If this value is a keyword, it is a fully qualified, namespaced keyword pointing to a symbol on the classpath at runtime. This symbol must be a map with keys as further specified by the information model. Onyx comes with a handful of aggregations built in, such as `:onyx.windowing.aggregation/min`. See the User Guide for the full list. Users can also implement their own aggregations.

                  If this value is a vector, it contain two values: a keyword as described above, and another keyword which represents the key to aggregate over."
             :type [:keyword [:keyword]]
             :optional? false}

            :window/window-key
            {:doc "The key of the incoming segments to window over. This key can represent any totally ordered domain, for example `:event-time`."
             :type :any
             :required-when ["`:window/type` is set to `:fixed`"
                             "`:window/type` is set to `:sliding`"
                             "`:window/type` is set to `:session`"]}

            :window/min-key
            {:doc "A globally minimum value that values of `:window/window-key` will never be less than. This is used for calculating materialized aggregates for windows in a space efficient manner."
             :type :integer
             :optional? true
             :default 0}

            :window/session-key
            {:doc "The key of the incoming segments to calculate a session window over. This key can represent any totally ordered domain, e.g. `:event-time`"
             :type :any
             :optional? true}

            :window/range
            {:doc "The span of time, or other totally ordered domain, that this window will capture data within."
             :type [:unit]
             :optional? false
             :required-when ["The `:window/type` is `:fixed` or `:sliding`."]}

            :window/slide
            {:doc "To offset of time, or other totally ordered domain, to wait before starting a new window after the previous window."
             :type [:unit]
             :required-when ["The `:window/type` is `:sliding`."]}

            :window/init
            {:doc "The initial value to be used for the aggregate, if required. Some aggregates require this, such as the Minimum aggregate. Others, such as the Conj aggregate, do not, as empty vector makes a suitable initial value."
             :type :any
             :required-when ["The `:window/aggregation` has no predefined initial value."]}

            :window/timeout-gap
            {:doc "The duration of dormant activity that constitutes a session window being closed."
             :type :unit
             :required-when ["The `window/type` is `:session`."]}

            :window/doc
            {:doc "A docstring for this window."
             :type :string
             :optional? true}}}

   :state-aggregation
   {:summary "Onyx provides the ability to perform stateful updates for segments calculated over windows. For example, a grouping task may accumulate incoming values for a number of keys over windows of 5 minutes."
    :link nil
    :model {:aggregation/init {:doc "Fn (window) to initialise the state."
                               :type :function
                               :optional? true}
            :aggregation/fn {:doc "Fn (state, window, segment) to generate a serializable state machine update."
                             :type :function
                             :optional? false}
            :aggregation/apply-state-update {:doc "Fn (state, entry) to apply state machine update entry to a state."
                                             :type :function
                                             :optional? false}
            :aggregation/super-aggregation-fn {:doc "Fn (state-1, state-2, window) to combine two states in the case of two windows being merged, e.g. session windows."
                                               :type :function
                                               :optional? true}}}
   :trigger-entry
   {:summary "Triggers are a feature that interact with Windows. Windows capture and bucket data over time. Triggers let you release the captured data over a variety stimuli."
    :link nil
    :model {:trigger/window-id
            {:doc "The name of a `:window/id` window to fire the trigger against."
             :type :keyword
             :optional? false
             :restrictions ["Must name a `:window/id` in the window entries."]}

            :trigger/refinement
            {:doc "The refinement mode to use when firing the trigger against a window. When set to `:accumulating`, the window contents remain. When set to `:discarding`, the window contents are destroyed, resetting the window to the initial aggregation value. The initial value is set lazily so expired windows do not unnecessarily consume memory."
             :type :keyword
             :choices [:accumulating :discarding]
             :optional? false}

            :trigger/on
            {:doc "The event to trigger in reaction to, such as a segment with a special feature, or on a timer. See the User Guide for the full list of prepackaged Triggers."
             :type :keyword
             :optional? false}

            :trigger/sync
            {:doc "A fully qualified, namespaced keyword pointing to a function on the classpath at runtime. This function takes the window contents as its argument. Its return value is ignored. This function is invoked when the trigger fires, and is used to do any arbitrary action with the window contents, such as sync them to a database."
             :type :keyword
             :optional? false}

            :trigger/period
            {:doc "A timer trigger sleeps for a duration of `:trigger/period`. When it is done sleeping, the `:trigger/sync` function is invoked with its usual arguments. The trigger goes back to sleep and repeats itself."
             :type :keyword
             :required-when ["`:trigger/on` is `:timer`"]
             :choices [:milliseconds :seconds :minutes :hours :days]
             :optional? true}

            :trigger/threshold
            {:doc "A segment trigger will fire every threshold of segments."
             :required-when ["`:trigger/on` is `:segments`"]
             :type [:integer :elements]
             :example [5 :elements]
             :optional? true}

            :trigger/fire-all-extents?
            {:doc "When set to `true`, if any particular extent fires in reaction to this trigger, all extents also fire."
             :type :boolean
             :optional? true
             :default false}

            :trigger/doc
            {:doc "A docstring for this trigger."
             :type :string
             :optional? true}}}

   :lifecycle-entry
   {:summary "Lifecycles are a feature that allow you to control code that executes at particular points during task execution on each peer. Lifecycles are data driven and composable."
    :link nil
    :model {:lifecycle/task
            {:doc "The task that this lifecycle applies to."
             :type :keyword
             :optional? false
             :restrictions ["Must be a task defined in the workflow."]}

            :lifecycle/calls
            {:doc "A fully qualified, namespaced keyword pointing to a symbol on the classpath at runtime. This symbol must be a map with keys further specified by the information model. The keys in this map denote the concrete functions to invoke at execution time."
             :type :keyword
             :optional? false}

            :lifecycle/doc
            {:doc "A docstring for this lifecycle."
             :type :string
             :optional? true}}}

   :lifecycle-calls
   {:summary "Lifecycle calls are related to lifecycles. They consist of a map of functions that are used when resolving lifecycle entries to their corresponding functions."
    :link nil
    :model {:lifecycle/doc {:doc "A docstring for these lifecycle calls."
                            :type :string
                            :optional? true}
            :lifecycle/start-task? {:doc "A function that takes two arguments - an event map, and the matching lifecycle map. Must return a boolean value indicating whether to start the task or not. If false, the process backs off for a preconfigured amount of time and calls this task again. Useful for lock acquisition. This function is called prior to any processes inside the task becoming active."
                                    :type :function
                                    :optional? true}
            :lifecycle/before-task-start {:doc "A function that takes two arguments - an event map, and the matching lifecycle map. Must return a map that is merged back into the original event map. This function is called after processes in the task are launched, but before the peer listens for incoming segments from other peers."
                                          :type :function
                                          :optional? true}
            :lifecycle/before-batch {:doc "A function that takes two arguments - an event map, and the matching lifecycle map. Must return a map that is merged back into the original event map. This function is called prior to receiving a batch of segments from the reading function."
                                     :type :function
                                     :optional? true}
            :lifecycle/after-read-batch {:doc "A function that takes two arguments - an event map, and the matching lifecycle map. Must return a map that is merged back into the original event map. This function is called immediately after a batch of segments has been read by the peer. The segments are available in the event map by the key `:onyx.core/batch`."
                                         :type :function
                                         :optional? true}
            :lifecycle/after-batch {:doc "A function that takes two arguments - an event map, and the matching lifecycle map. Must return a map that is merged back into the original event map. This function is called immediately after a batch of segments has been procesed by the peer, but before the batch is acked."
                                    :type :function
                                    :optional? true}
            :lifecycle/after-task-stop {:doc "A function that takes two arguments - an event map, and the matching lifecycle map. Must return a map that is merged back into the original event map. This function is called before the peer relinquishes its task. No more segments will be received."
                                        :type :function
                                        :optional? true}
            :lifecycle/after-ack-segment {:doc "A function that takes four arguments - an event map, a message id, the return of an input plugin ack-segment call, and the matching lifecycle map. May return a value of any type which will be discarded. This function is whenever a segment at the input task has been fully acked."
                                          :type :function
                                          :optional? true}
            :lifecycle/after-retry-segment {:doc "A function that takes four arguments - an event map, a message id, the return of an input plugin ack-segment call, and the matching lifecycle map. May return a value of any type which will be discarded. This function is whenever a segment at the input task has been pending for greater than pending-timeout time and will be retried."
                                            :type :function
                                            :optional? true}}}

   :peer-config
   {:summary "All options available to configure the virtual peers and development environment."
    :link nil
    :model {:onyx/id 
	    {:doc "The ID for the cluster that the peers will coordinate via. Provides a way to provide strong, multi-tenant isolation of peers."
	     :type [:one-of [:string :uuid]]
	     :optional? false}

	    :onyx.peer/job-scheduler 
	    {:doc "Each running Onyx instance is configured with exactly one job scheduler. The purpose of the job scheduler is to coordinate which jobs peers are allowed to volunteer to execute."
	     :type :keyword
	     :choices [:onyx.job-scheduler/percentage :onyx.job-scheduler/balanced :onyx.job-scheduler/greedy]
	     :optional? false}

	    :zookeeper/address
            {:doc "The addresses of the ZooKeeper servers to use for coordination e.g. 192.168.1.1:2181,192.168.1.2:2181"
             :type :string
             :optional? false}

            :onyx.peer/inbox-capacity
            {:doc "Maximum number of messages to try to prefetch and store in the inbox, since reading from the log happens asynchronously."
             :type :integer
             :unit :messages
             :default 1000
             :optional? true}

            :onyx.peer/outbox-capacity
            {:doc "Maximum number of messages to buffer in the outbox for writing, since writing to the log happens asynchronously."
             :type :integer
             :unit :messages
             :default 1000
             :optional? true}

            :onyx.peer/retry-start-interval
            {:doc "Number of ms to wait before trying to reboot a virtual peer after failure."
             :type :integer
             :unit :milliseconds
             :default 2000
             :optional? true}

            :onyx.peer/join-failure-back-off
            {:doc "Number of ms to wait before trying to rejoin the cluster after a previous join attempt has aborted."
             :type :integer
             :unit :milliseconds
             :default 250
             :optional? true}

            :onyx.peer/drained-back-off
            {:doc "Number of ms to wait before trying to complete the job if all input tasks have been exhausted."
             :type :integer
             :unit :milliseconds
             :default 400
             :optional? true}

            :onyx.peer/peer-not-ready-back-off
            {:doc "Number of ms to back off and wait before retrying the call to `start-task?` lifecycle hook if it returns false."
             :type :integer
             :unit :milliseconds
             :default 500
             :optional? true}

            :onyx.peer/job-not-ready-back-off
            {:doc "Number of ms to back off and wait before trying to discover configuration needed to start the subscription after discovery failure."
             :type :integer
             :unit :milliseconds
             :optional? true
             :default 500}

            :onyx.peer/fn-params
            {:doc "A map of keywords to vectors. Keywords represent task names, vectors represent the first parameters to apply to the function represented by the task. For example, `{:add [42]}` for task `:add` will call the function underlying `:add` with `(f 42 <segment>)` This will apply to any job with this task name."
             :type :map
             :optional? true
             :default {}}

            :onyx.peer/backpressure-check-interval
            {:doc "Number of ms between checking whether the virtual peer should notify the cluster of backpressure-on/backpressure-off."
             :type :integer
             :unit :milliseconds
             :optional? true
             :default 10}

            :onyx.peer/backpressure-low-water-pct
            {:doc "Percentage of messaging inbound-buffer-size that constitutes a low water mark for backpressure purposes."
             :type :integer
             :optional? true
             :default 30}

            :onyx.peer/backpressure-high-water-pct
            {:doc "Percentage of messaging inbound-buffer-size that constitutes a high water mark for backpressure purposes."
             :type :integer
             :optional? true
             :default 60}

            :onyx.windowing/min-value
            {:doc "A default strict miminum value that `:window/window-key` can ever be. Note, this is generally best configured individually via :window/min-value in the task map."
             :type :integer
             :optional? true
             :default 0}

            :onyx.zookeeper/backoff-base-sleep-time-ms
            {:doc "Initial amount of time to wait between ZooKeeper connection retries"
             :unit :milliseconds
             :optional? true
             :type :integer
             :default 1000}

            :onyx.zookeeper/backoff-max-sleep-time-ms
            {:doc "Maximum amount of time in ms to sleep on each retry"
             :unit :milliseconds
             :optional? true
             :type :integer
             :default 30000}

            :onyx.zookeeper/backoff-max-retries
            {:doc "Maximum number of times to retry connecting to ZooKeeper"
             :optional? true
             :type :integer
             :default 5}

            :onyx.zookeeper/prepare-failure-detection-interval
            {:doc "Number of ms to wait between checking if the peer that joins this peer via prepare has failed. This value is used within a loop to periodically detect a false-positive case where a ZooKeeper ephemeral node is still present even though the process has (recently died). This value is only used within the prepare phase of joining a peer, and is not used for the normal failure detection path when a peer has fully joined the cluster."
             :unit :milliseconds
             :optional? true
             :type :integer
             :default 1000}

            :onyx.messaging/inbound-buffer-size
            {:doc "Number of messages to buffer in the core.async channel for received segments."
             :optional? true
             :type :integer
             :default 50000}

            :onyx.messaging/completion-buffer-size
            {:doc "Number of messages to buffer in the core.async channel for completing messages on an input task."
             :optional? true
             :type :integer
             :default 10000}

            :onyx.messaging/release-ch-buffer-size
            {:doc "Number of messages to buffer in the core.async channel for released completed messages."
             :optional? true
             :type :integer
             :default 10000}

            :onyx.messaging/retry-ch-buffer-size
            {:doc "Number of messages to buffer in the core.async channel for retrying timed-out messages."
             :optional? true
             :type :integer
             :default 10000}

            :onyx.messaging/peer-link-gc-interval
            {:doc "The interval in milliseconds to wait between closing idle peer links."
             :unit :milliseconds
             :optional? true
             :type :integer
             :default 90000}

            :onyx.messaging/peer-link-idle-timeout
            {:doc "The maximum amount of time that a peer link can be idle (not looked up in the state atom for usage) before it is eligible to be closed. The connection will be reopened from scratch the next time it is needed."
             :unit :milliseconds
             :optional? true
             :type :integer
             :default 60000}

            :onyx.messaging/ack-daemon-timeout
            {:doc "Number of milliseconds that an ack value can go without being updates on a daemon before it is eligible to time out."
             :unit :milliseconds
             :optional? true
             :type :integer
             :default 480000}

            :onyx.messaging/ack-daemon-clear-interval
            {:doc "Number of milliseconds to wait for process to periodically clear out ack-vals that have timed out in the daemon."
             :unit :milliseconds
             :optional? true
             :type :integer
             :default 15000}

            :onyx.messaging/decompress-fn
            {:doc "The Clojure function to use for messaging decompression. Receives one argument - a byte array. Must return the decompressed value of the byte array."
             :optional? true
             :type :function
             :default 'onyx.compression.nippy/decompress}

            :onyx.messaging/compress-fn
            {:doc "The Clojure function to use for messaging compression. Receives one argument - a sequence of segments. Must return a byte array representing the segment seq."
             :optional? true
             :type :function
             :default 'onyx.compression.nippy/compress}

            :onyx.messaging/impl
            {:doc "The messaging protocol to use for peer-to-peer communication."
             :optional? false
             :type :keyword
             :choices [:aeron]}

            :onyx.messaging/bind-addr
            {:doc "An IP address to bind the peer to for messaging. Defaults to `nil`. When `nil`, Onyx binds to it's external IP to the result of calling `http://checkip.amazonaws.com`."
             :optional? false
             :type :string
             :default nil}

            :onyx.messaging/external-addr
            {:doc "An IP address to advertise to other peers. Useful in case of firewalling, port forwarding, etc, where the interface/IP that is bound is different to the address that other peers should connect to."
             :optional? true
             :type :string
             :default nil}

            :onyx.messaging/peer-port
            {:doc "Port that peers should use to communicate."
             :optional? false
             :type :integer
             :default nil}

            :onyx.messaging/allow-short-circuit?
            {:doc "A boolean denoting whether to allow virtual peers to short circuit networked messaging when colocated with the other virtual peer. Short circuiting allows for direct transfer of messages to a virtual peer's internal buffers, which improves performance where possible. This configuration option is primarily for use in perfomance testing, as peers will not generally be able to short circuit messaging after scaling to many nodes."
             :optional? true
             :type :boolean
             :default true}

            :onyx.messaging.aeron/embedded-driver?
            {:doc "A boolean denoting whether an Aeron media driver should be started up with the environment. See [Aeron Media Driver](../../src/onyx/messaging/aeron_media_driver.clj) for an example for how to start the media driver externally."
             :optional? true
             :type :boolean
             :default true}

            :onyx.messaging.aeron/subscriber-count
            {:doc "The number of Aeron subscriber threads that receive messages for the peer-group.  As peer-groups are generally configured per-node (machine), this setting can bottleneck receive performance if many virtual peers are used per-node, or are receiving and/or de-serializing large volumes of data. A good guidline is is `num cores = num virtual peers + num subscribers`, assuming virtual peers are generally being fully utilised."
             :optional? true
             :type :integer
             :default 2}

            :onyx.messaging.aeron/write-buffer-size
            {:doc "Size of the write queue for the Aeron publication. Writes to this queue will currently block once full."
             :optional? true
             :type :integer
             :default 1000}

            :onyx.messaging.aeron/poll-idle-strategy
            {:doc "The Aeron idle strategy to use between when polling for new messages. Currently, two choices `:high-restart-latency` and `:low-restart-latency` can be chosen. low-restart-latency may result in lower latency message, at the cost of higher CPU usage or potentially reduced throughput."
             :optional? true
             :type :keyword
             :default :high-restart-latency
             :choices [:high-restart-latency :low-restart-latency]}

            :onyx.messaging.aeron/offer-idle-strategy
            {:doc "The Aeron idle strategy to use between when offering messages to another peer. Currently, two choices `:high-restart-latency` and `:low-restart-latency` can be chosen. low-restart-latency may result in lower latency message, at the cost of higher CPU usage or potentially reduced throughput."
             :optional? true
             :type :keyword
             :default :high-restart-latency
             :choices [:high-restart-latency :low-restart-latency]}

            :onyx.messaging.aeron/publication-creation-timeout
            {:doc "Timeout after a number of ms on attempting to create an Aeron publication"
             :optional? true
             :type :integer
             :default 1000}
            :onyx.peer/state-log-impl
            {:doc "Choice of state persistence implementation."
             :optional? true
             :type :keyword
             :default :bookkeeper
             :choices [:bookkeeper]}

            :onyx.bookkeeper/read-batch-size
            {:doc "Number of bookkeeper ledger entries to read at a time when recovering state. Effective batch read of state entries is write-batch-size * read-batch-size."
             :optional? true
             :type :integer
             :default 50}

            :onyx.bookkeeper/write-batch-size
            {:doc "Number of state persistence writes to batch into a single BookKeeper ledger entry."
             :optional? true
             :type :integer
             :default 20}

            :onyx.bookkeeper/write-batch-timeout
            {:doc "Maximum amount of time to wait while batching BookKeeper writes, before writing the batch to BookKeeper. In case of a full batch read, timeout will not be hit."
             :unit :milliseconds
             :optional? true
             :type :integer
             :default 50}

            :onyx.bookkeeper/ledger-ensemble-size
            {:doc "The number of BookKeeper instances over which entries will be striped. For example, if you have an ledger-ensemble-size of 3, and a ledger-quorum-size of 2, the first write will be written to server1 and server2, the second write will be written to server2, and server3, etc."
             :optional? true
             :type :integer
             :default 3}

            :onyx.bookkeeper/ledger-quorum-size
            {:doc "The number of BookKeeper instances over which entries will be written to. For example, if you have an ledger-ensemble-size of 3, and a ledger-quorum-size of 2, the first write will be written to server1 and server2, the second write will be written to server2, and server3, etc."
             :optional? true
             :type :integer
             :default 3}

            :onyx.bookkeeper/ledger-id-written-back-off
            {:doc "Number of milliseconds to back off (sleep) after writing BookKeeper ledger id to the replica."
             :optional? true
             :type :integer
             :unit :milliseconds
             :default 50}

            :onyx.bookkeeper/ledger-password
            {:doc "Password to use for Onyx state persisted to BookKeeper ledgers. Highly recommended this is changed on cluster wide basis."
             :optional? true
             :type :string
             :default "INSECUREDEFAULTPASSWORD"}

            :onyx.bookkeeper/client-throttle
            {:doc "Tunable write throttle for BookKeeper ledgers."
             :optional? true
             :type :integer
             :default 30000}

            :onyx.bookkeeper/write-buffer-size
            {:doc "Size of the buffer to which BookKeeper ledger writes are buffered via."
             :optional? true
             :type :integer
             :default 10000}

            :onyx.bookkeeper/client-timeout
            {:doc "BookKeeper client timeout."
             :optional? true
             :type :integer
             :unit :milliseconds
             :default 60000}

            :onyx.peer/state-filter-impl
            {:doc "Choice of uniqueness key filtering implementation."
             :optional? true
             :type :keyword
             :default :rocksdb
             :choices [:rocksdb]}

            :onyx.rocksdb.filter/base-dir
            {:doc "Temporary directory to persist uniqueness filtering data."
             :optional? true
             :type :string
             :default "/tmp/rocksdb_filter"}


            :onyx.rocksdb.filter/bloom-filter-bits 
            {:doc "Number of bloom filter bits to use per uniqueness key value"
             :optional? true
             :type :integer
             :default 10}

            :onyx.rocksdb.filter/compression
            {:doc "Whether to use compression in rocksdb filter. It is recommended that `:none` is used unless your uniqueness keys are large and compressible."
             :optional? true
             :type :string
             :choices [:bzip2 :lz4 :lz4hc :none :snappy :zlib] 
             :default :none}

            :onyx.rocksdb.filter/block-size 
            {:doc "RocksDB block size. May worth being tuned depending on the size of your uniqueness-key values."
             :optional? true
             :type :integer
             :default 4096}
            :onyx.rocksdb.filter/peer-block-cache-size 
            {:doc "RocksDB block cache size in bytes. Larger caches reduce the chance that the peer will need to check for the prescence of a uniqueness key on disk. Defaults to 100MB."
             :optional? true
             :type :integer
             :default 104857600}

            :onyx.rocksdb.filter/num-buckets 
            {:doc "Number of rotating filter buckets to use. Buckets are rotated every `:onyx.rocksdb.filter/num-ids-per-bucket`, with the oldest bucket being discarded if num-buckets already exist."
             :optional? true
             :type :integer
             :default 10}
            
            :onyx.rocksdb.filter/num-ids-per-bucket 
            {:doc "Number of uniqueness key values that can exist in a RocksDB filter bucket."
             :optional? true
             :type :integer
             :default 10000000}
            :onyx.rocksdb.filter/rotation-check-interval-ms
            {:doc "Check whether filter bucket should be rotated every interval ms"
             :optional? true
             :type :integer
             :default 50}}}
:env-config
{:summary "All options available to configure the node environment."
 :link nil
 :model {:zookeeper/server?
         {:doc "Bool to denote whether to startup a local, in-memory ZooKeeper. **Important: for TEST purposes only.**"
          :type :boolean
          :optional? true}

         :zookeeper.server/port
         {:doc "Port to use for the local in-memory ZooKeeper"
          :type :integer
          :required-when ["The `:zookeeper/server?` is `true`."]}

         :onyx/id 
         {:doc "The ID for the cluster that the peers will coordinate via. Provides a way to provide strong, multi-tenant isolation of peers."
          :type [:one-of [:string :uuid]]
          :required-when ["`:onyx.bookkeeper/server?` is `true`."]
          :optional? true}

         :zookeeper/address
         {:doc "The addresses of the ZooKeeper servers to use for coordination e.g. 192.168.1.1:2181,192.168.1.2:2181"
          :type :string
          :optional? false}

         :onyx.bookkeeper/server?
         {:doc "Bool to denote whether to startup a BookKeeper instance on this node, for use in persisting Onyx state information."
          :type :boolean
          :default false
          :optional? true}

         :onyx.bookkeeper/delete-server-data? 
         {:doc "Bool to denote whether to delete all BookKeeper server instance data on environment shutdown. Set to true when using BookKeeper for unit/integration test runs."
          :type :boolean
          :default false
          :optional? true}

         :onyx.bookkeeper/local-quorum?
         {:doc "Bool to denote whether to startup a full quorum of BookKeeper instances on this node. **Important: for TEST purposes only.**"
          :default false
          :type :boolean
          :required-when ["The `:onyx.bookkeeper/server?` is `true` and `:onyx.bookkeeper/local-quorum?` is `true`"]
          :optional? true}

         :onyx.bookkeeper/local-quorum-ports
         {:doc "Ports to use for the local BookKeeper quorum."
          :type :vector
          :default [3196 3197 3198]
          :required-when ["The `:onyx.bookkeeper/server?` is `true` and `:onyx.bookkeeper/local-quorum?` is `true`"]
          :optional? true}

         :onyx.bookkeeper/port
         {:doc "Port to startup this node's BookKeeper instance on."
          :type :integer
          :default 3196
          :required-when ["The `:onyx.bookkeeper/server?` is `true` and `:onyx.bookkeeper/local-quorum?` is `false`"]}

         :onyx.bookkeeper/base-journal-dir
         {:doc "Directory to store BookKeeper's journal in. It is recommended that this is altered to somewhere fast, preferably on a different disk to the BookKeeper ledger."
          :type :string
          :default "/tmp/bookkeeper_journal"
          :optional? true}

         :onyx.bookkeeper/base-ledger-dir
         {:doc "Directory to store BookKeeper's ledger in. It is recommended that this is altered to somewhere fast, preferably on a different disk to the BookKeeper journal"
          :type :string
          :default "/tmp/bookkeeper_ledger"
          :optional? true}}}}) 

(def model-display-order
  {:catalog-entry
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
    :onyx/pending-timeout 
    :onyx/input-retry-timeout 
    :onyx/max-pending 
    :onyx/fn
    :onyx/group-by-key 
    :onyx/group-by-fn 
    :onyx/bulk?  
    :onyx/flux-policy
    :onyx/uniqueness-key
    :onyx/deduplicate?
    :onyx/restart-pred-fn]
   :flow-conditions-entry
   [:flow/from :flow/to :flow/predicate :flow/exclude-keys :flow/short-circuit?
    :flow/thrown-exception?  :flow/post-transform :flow/action :flow/doc]
   :window-entry
   [:window/id :window/task :window/type :window/aggregation :window/window-key
    :window/min-key :window/session-key :window/range :window/slide
    :window/init :window/timeout-gap :window/doc]
   :state-aggregation
   [:aggregation/init :aggregation/fn :aggregation/apply-state-update :aggregation/super-aggregation-fn] 
   :trigger-entry
   [:trigger/window-id :trigger/refinement :trigger/on :trigger/sync
    :trigger/period :trigger/threshold :trigger/fire-all-extents?
    :trigger/doc] 
   :lifecycle-entry
   [:lifecycle/task :lifecycle/calls :lifecycle/doc]
   :lifecycle-calls
   [:lifecycle/doc 
    :lifecycle/start-task? 
    :lifecycle/before-task-start 
    :lifecycle/before-batch 
    :lifecycle/after-read-batch 
    :lifecycle/after-batch 
    :lifecycle/after-task-stop 
    :lifecycle/after-ack-segment 
    :lifecycle/after-retry-segment]
   :peer-config
   [:onyx/id :onyx.peer/job-scheduler :zookeeper/address
    :onyx.peer/inbox-capacity :onyx.peer/outbox-capacity
    :onyx.peer/retry-start-interval :onyx.peer/join-failure-back-off
    :onyx.peer/drained-back-off :onyx.peer/peer-not-ready-back-off
    :onyx.peer/job-not-ready-back-off :onyx.peer/fn-params
    :onyx.peer/backpressure-check-interval
    :onyx.peer/backpressure-low-water-pct
    :onyx.peer/backpressure-high-water-pct :onyx.windowing/min-value
    :onyx.zookeeper/backoff-base-sleep-time-ms
    :onyx.zookeeper/backoff-max-sleep-time-ms
    :onyx.zookeeper/backoff-max-retries :onyx.messaging/inbound-buffer-size
    :onyx.zookeeper/prepare-failure-detection-interval
    :onyx.messaging/completion-buffer-size
    :onyx.messaging/release-ch-buffer-size 
    :onyx.messaging/retry-ch-buffer-size
    :onyx.messaging/peer-link-gc-interval
    :onyx.messaging/peer-link-idle-timeout :onyx.messaging/ack-daemon-timeout
    :onyx.messaging/ack-daemon-clear-interval :onyx.messaging/decompress-fn
    :onyx.messaging/compress-fn :onyx.messaging/impl :onyx.messaging/bind-addr
    :onyx.messaging/external-addr :onyx.messaging/peer-port
    :onyx.messaging/allow-short-circuit?
    :onyx.messaging.aeron/embedded-driver?
    :onyx.messaging.aeron/subscriber-count
    :onyx.messaging.aeron/write-buffer-size
    :onyx.messaging.aeron/poll-idle-strategy
    :onyx.messaging.aeron/offer-idle-strategy 
    :onyx.messaging.aeron/publication-creation-timeout
    :onyx.peer/state-log-impl
    :onyx.bookkeeper/read-batch-size 
    :onyx.bookkeeper/write-batch-size
    :onyx.bookkeeper/write-batch-timeout 
    :onyx.bookkeeper/ledger-ensemble-size
    :onyx.bookkeeper/ledger-quorum-size
    :onyx.bookkeeper/ledger-id-written-back-off
    :onyx.bookkeeper/ledger-password 
    :onyx.bookkeeper/client-throttle
    :onyx.bookkeeper/write-buffer-size 
    :onyx.bookkeeper/client-timeout
    :onyx.peer/state-filter-impl 
    :onyx.rocksdb.filter/base-dir
    :onyx.rocksdb.filter/bloom-filter-bits 
    :onyx.rocksdb.filter/compression
    :onyx.rocksdb.filter/block-size 
    :onyx.rocksdb.filter/peer-block-cache-size
    :onyx.rocksdb.filter/num-buckets 
    :onyx.rocksdb.filter/num-ids-per-bucket
    :onyx.rocksdb.filter/rotation-check-interval-ms]
   :env-config
   [:zookeeper/server? :zookeeper.server/port :onyx/id :zookeeper/address
    :onyx.bookkeeper/server? 
    :onyx.bookkeeper/delete-server-data?
    :onyx.bookkeeper/local-quorum?
    :onyx.bookkeeper/local-quorum-ports :onyx.bookkeeper/port
    :onyx.bookkeeper/base-journal-dir :onyx.bookkeeper/base-ledger-dir]})
