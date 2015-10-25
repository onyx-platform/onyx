{:catalog-entry
 {:onyx/name
  {:doc "The name of the task that represents this catalog entry. Must correspond to a keyword in the workflow associated with this catalog."
   :type :keyword
   :choices :any
   :restrictions ["Must be unique across all catalog entries."
                  "Value cannot be `:none`."
                  "Value cannot be `:all`."]
   :optional? false}

  :onyx/type
  {:doc "The role that this task performs. `:input` reads data. `:function` applies a transformation. `:output` writes data."
   :type :keyword
   :choices [:input :function :output]
   :optional? false}

  :onyx/batch-size
  {:doc "The number of segments a peer will wait to read before processing them all in a batch for this task. Segments will be processed when either `:onyx/batch-size` segments have been received at this peer, or `:onyx/batch-timeout` milliseconds have passed - whichever comes first. This is a knob that is used to tune throughput and latency, and it goes hand-in-hand with `:onyx/batch-timeout`."
   :type :integer
   :restrictions ["Value must be greater than 0."]
   :optional? false}

  :onyx/batch-timeout
  {:doc "The number of milliseconds a peer will wait to read more segments before processing them all in a batch for this task. Segments will be processe when either `:onyx/batch-timeout` milliseconds passed, or `:onyx/batch-size` segments have been read - whichever comes first. This is a knob that is used to tune throughput and latency, and it goes hand-in-hand with `:onyx/batch-size`."
   :type :integer
   :unit :milliseconds
   :restrictions ["Value must be greater than 0."]
   :default 1000
   :optional? true}

  :onyx/doc
  {:doc "A docstring for this catalog entry."
   :type :string
   :optional? true}

  :onyx/max-peers
  {:doc "The maximum number of peers that will ever be assigned to this task concurrently."
   :type :integer
   :restrictions ["Value must be greater than 0."]
   :optional? true}

  :onyx/min-peers
  {:doc "The minimum number of peers that will be concurrently assigned to execute this task before it begins. If the number of peers working on this task falls below its initial count due to failure or planned departure, the choice of `:onyx/flux-policy` defines the strategy for what to do."
   :type :integer
   :restrictions ["Value must be greater than 0."
                  "`:onyx/flux-policy` must also be defined in this catalog entry."]
   :optional? true}

  :onyx/n-peers
  {:doc "A convenience parameter which expands to `:onyx/min-peers` and `:onyx/max-peers` set to the same value. This is useful if you want to specify exactly how many peers should concurrently execute this task - no more, and no less."
   :type :integer
   :restrictions ["Value must be greater than 0."
                  "`:onyx/min-peers` cannot also be defined for this catalog entry."
                  "`:onyx/max-peers` cannot also be defined for this catalog entry."
                  "`:onyx/flux-policy` must also be defined in this catalog entry."]
   :optional? true}

  :onyx/language
  {:doc "Designates the language that the function denoted by `:onyx/fn` is implemented in."
   :type :keyword
   :choices [:clojure :java]
   :default :clojure
   :optional? true}

  :onyx/restart-pred-fn
  {:doc "A fully-qualified namespaced keyword pointing to function which takes an exception as a parameter, returning a boolean indicating whether the peer that threw this exception should restart its task."
   :type :keyword
   :choices :any
   :restrictions ["Must resolve to a function on the classpath at runtime."]
   :optional? true}

  :onyx/medium
  {:doc "Denotes the kind of input or output communication or storage that is being read from or written to (e.g. `:kafka` or `:web-socket`). This is currently does not affect any functionality, and is reserved for the future."
   :type :keyword
   :choices :any
   :required-when ["`:onyx/type` is set to `:input`"
                   "`:onyx/type` is set to `:output`"]}

  :onyx/plugin
  {:doc "When `:onyx/language` is set to `:clojure`, this is a fully qualified, namespaced keyword pointing to a function that takes the Event map and returns a Record implementing the Plugin interfaces. When `:onyx/language` is set to `:java`, this is a keyword pointing to a Java class that is constructed with the Event map. This class must implement the interoperability interfaces."
   :type :keyword
   :choices :any
   :required-when ["`:onyx/type` is set to `:input`"
                   "`:onyx/type` is set to `:output`"]}

  :onyx/pending-timeout
  {:doc "The duration of time, in milliseconds, that a segment that enters an input task has to be fully acknowledged and processed. That is, this segment, and any subsequent segments that it creates in downstream tasks, must be fully processed before this timeout occurs. If the segment is not fully processed, it will automatically be retried."
   :type :integer
   :default 60000
   :units :milliseconds
   :optionally-allowed-when ["`:onyx/type` is set to `:input`"
                             "Value must be greater than 0."]}

  :onyx/input-retry-timeout
  {:doc "The duration of time, in milliseconds, that the input task goes dormant between checking which segments should expire from its internal pending pool. When segments expire, they are automatically retried."
   :type :integer
   :default 1000
   :units :milliseconds
   :optionally-allowed-when ["`:onyx/type` is set to `:input`"
                             "Value must be greater than 0."]}

  :onyx/max-pending
  {:doc "The maximum number of segments that a peer executing an input task will allow in its internal pending message pool. If this pool is filled to capacity, it will not accept new segments - exhibiting backpressure to upstream message produces."
   :type :integer
   :default 10000
   :units :segments
   :optionally-allowed-when ["`:onyx/type` is set to `:input`"
                             "Value must be greater than 0."]}

  :onyx/fn
  {:doc "A fully qualified, namespaced keyword that points to a function on the classpath. This function takes at least one argument - an incoming segment, and returns either a segment or a vector of segments. This function may not return `nil`. This function can be parameterized further through a variety of techniques."
   :type :keyword
   :required-when ["`:onyx/type` is set to `:function`"]
   :optionally-allowed-when ["`:onyx/type` is set to `:input`"
                             "`:onyx/type` is set to `:output`"]}

  :onyx/group-by-key
  {:doc "The key, or vector of keys, to group incoming segments by. Keys that hash to the same value will always be sent to the same virtual peer."
   :type [:any [:any]]
   :optionally-allowed-when ["`:onyx/type` is set to `:function`"]
   :restrictions ["Cannot be defined when `:onyx/group-by-fn` is defined."]}

  :onyx/group-by-fn
  {:doc "A fully qualified, namespaced keyword that points to a function on the classpath. This function takes a single argument, a segment, as a parameter. The value that the function returns will be hashed. Values that hash to the same value will always be sent to the same virtual peer."
   :type :keyword
   :optionally-allowed-when ["`:onyx/type` is set to `:function`"]
   :restrictions ["Cannot be defined when `:onyx/group-by-key` is defined."]}

  :onyx/bulk?
  {:doc "Boolean value indicating whether the function in this catalog entry denoted by `:onyx/fn` should take a single segment, or the entire batch of segments that were read as a parameter. When set to `true`, this function's return value is ignored. The segments are identically propogated to the downstream tasks."
   :type :boolean
   :default false
   :optionally-allowed-when ["`:onyx/type` is set to `:function`"]}


  :onyx/flux-policy
  {:doc "The policy that should be used when a task with grouping enabled loses a peer. Losing a peer means that the consistent hashing used to pin the same hashed values to the same peers will be altered. Using the `:kill` flux policy will kill the job. This is useful for jobs that cannot tolerate an altered hashing strategy. Using `:continue` will allow the job to continue running. With `:kill` and `:continue`, new peers will never be added to this job. The final policy is `:recover`, which is like `:continue`, but will allow peers to be added back to this job to meet the `:onyx/min-peers` number of peers working on this task concurrently."
   :type :keyword
   :choices [:kill :continue :recover]
   :optionally-allowed-when ["`:onyx/type` is set to `:function`"
                             "`:onyx/group-by-key` or `:onyx/group-by-fn` is set."]}

  :onyx/uniqueness-key
  {:doc "The key of incoming segments that indicates global uniqueness. This is used by the Windowing feature to detect duplicated processing of segments. An example of this would be an `:id` key for segments representing users, assuming `:id` is globally unique in your system. An example of a bad uniqueness-key would be `:first-name` as two or more users may have their first names in common."
   :type :any
   :required-when ["A Window is defined on this task."]}}

 :flow-conditions-entry
 {:flow/from
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

  :flow/action?
  {:doc "Names a side effect to perform in response to processing this segment. If set to `:retry`, this segment will be immediately, forcibly retried from the root input task from which it eminated. This segment will not be sent to any downstream tasks."
   :type :keyword
   :choices [:retry]
   :optional? true
   :default nil
   :restrictions "Any flow condition clauses with `:flow/action` set to `:retry` must also have `:flow/short-circuit?` set to `true`, and `:flow/to` set to `:none`."}}

 :window-entry
 {:window/id
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
   :type :keyword
   :optional? false}

  :window/min-key
  {:doc "A globally minimum value that values of `:window/window-key` will never be less than. This is used for calculating materialized aggregates for windows in a space efficient manner."
   :type :integer
   :optional? true
   :default 0}

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
   :optional? true}}

 :trigger-entry
 {:trigger/window-id
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

  :trigger/fire-all-extents?
  {:doc "When set to `true`, if any particular extent fires in reaction to this trigger, all extents also fire."
   :type :boolean
   :optional? true
   :default false}
  
  :trigger/doc
  {:doc "A docstring for this trigger."
   :type :string
   :optional? true}}

 :lifecycle-entry
 {:lifecycle/name
  {:doc "A unique name identifying this lifecycle."
   :type :keyword
   :optional? false
   :restrictions ["Must be unique across all lifecycle entries."]}

  :lifecycle/calls
  {:doc "A fully qualified, namespaced keyword pointing to a symbol on the classpath at runtime. This symbol must be a map with keys further specified by the information model. The keys in this map denote the concrete functions to invoke at execution time."
   :type :keyword
   :optional? false}

  :lifecycle/doc
  {:doc "A docstring for this lifecycle."
   :type :string
   :optional? true}}

 :peer-config
 {:onyx.peer/inbox-capacity
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
   :optional? true}}}
