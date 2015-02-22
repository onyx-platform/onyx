## Flow Conditions

This section covers flow conditions. Flow conditions are used for isolating logic about whether or not segments should pass through different tasks in a workflow.

### Summary

Workflows specify the structure of your computation as a directed, acyclic graph. A workflow describes all *possible* routes that a segment can take as it enters your workflow. On the other hand, we often have the need to articulate how an *individual* segment moves throughout your workflow. Many times, a segment conditionally moves from one task to another. This is a concept that Onyx takes apart and turns into its own idea, independent of the rest of your computation. They're called Flow Conditions.

### Motivating Example

The easiest way to learn how to use flow conditions is to see an example. Suppose we have the following workflow snippet:

```clojure
[[:input-stream :children]
 [:input-stream :adults]
 [:input-stream :western-females]
 [:input-stream :everyone]
 ...]
```

This workflow takes some input in (presumably a stream of people), and directs segments to four possible tasks - `:process-children`, `:process-adults`, `:process-western-females`, and `:process-everyone`. Suppose we want to *conditionally* direct a segment to zero or more of these tasks, depending on some predicates. We use flow conditions to carry out this work. Flow conditions are their own data structure that are bundled along with the workflow and catalog to `onyx.api/submit-job`. Here's an example of what a flow conditions data structure would look like for our proposed workflow:

```clojure
[{:flow/from :input-stream                                                                                                                                  
  :flow/to [:process-children]
  :my/max-child-age 17
  :flow/predicate [:my.ns/child? :my/max-child-age]
  :flow/doc "Emits segment if this segment is a child."}
  
 {:flow/from :input-stream
  :flow/to [:process-adults]
  :flow/predicate :my.ns/adult?
  :flow/doc "Emits segment if this segment is an adult."}

 {:flow/from :input-stream
  :flow/to [:process-western-females]
  :flow/predicate [:and :my.ns/female? :my.ns/western?]
  :flow/doc "Emits segment if this segment is a western female."}
  
 {:flow/from :input-stream
  :flow/to [:process-everyone]
  :flow/predicate :my.ns/constantly-true
  :flow/doc "Always emit this segment"}]
```

The basic idea is that every entry in the Flow Conditions data structure denotes a relationship between a task and its downstream tasks. `:flow/from` indicates the task that the segment is leaving, and `:flow/to` indicates the tasks that the segment should be sent to if the predicate evaluates to true. The predicate is denoted by `:flow/predicate`, which is a keyword or sequence of keywords that are resolved to a function. Later in this section, we'll cover how exactly the predicate function is constructed.

There is *one* flow conditions data structure per job - that is, there is one vector of maps. The order that you specify the flow conditions in matters. More on that later in this section.

Flow Conditions are entirely optional! Omitting them leads to the default behavior, which sends a segment to all immediate downstream tasks.

### Predicate Function Signatures

A predicate function is a Clojure function that takes at least two parameters - a context map, and a segment. Predicates can take parameters at runtime. They will be appended to the end of the function invocation. See Predicate Parameters for further discussion of how to use runtime parameters.

Predicates for the above examples can be seen below:

```clojure
Fill me in.
```

### Predicate Parameters

Predicate functions can take parameters at runtime. In this first flow condition, we use the parameter `:my/max-child-age` and set its value to `17`. We pass this value to the predicate by surrounding it with brackets, as in: `[:my.ns/child? :my/max-child-age]`. The parameters are appended to the end of the function call to the predicate. See Predicate Function Signatures in this section to see the arguments that are passed into the predicate regardless each invocation.

### Key Exclusion

Sometimes, the decision of whether to allow a segment to pass through to the next task depends on some side effects that were a result of the original segment transformation. Onyx allows you to handle this case by adding extra keys to your segment that comes out of the transformation function. These extra keys are visible in your predicate function, and then stripped off before being sent to the next task. You can indicate these "extra keys" by the setting `:onyx/exclude-keys` to a vector of keys.

### Predicate Composition

One extraordinarily powerful feature of Flow Conditions is its composition characters. Predicates can be composed with logical `and`, `or`, and `not`. We use composition to check if the segment is both female and western living in `[:and :my.ns/female? :my.ns/western?]`. Logical function calls must be surrounded with brackets, and may be nested arbitrarily. Functions inside of logical operator calls may be parameterized, as in `[:and :my.ns/female? [:my.ns/western? :my/state-param]]` Parameters *may not* specify logical functions.

### Match All/None

Sometimes, you want a flow condition that emits a value to all tasks if the predicate is true. You can use short hand to emit to all downstream tasks:

```clojure
 {:flow/from :input-stream
  :flow/to :all
  :flow/short-circuit? true
  :flow/predicate :my.ns/adult?}
```

Similarly, sometimes you want to emit to no downstream tasks:

```clojure
 {:flow/from :input-stream
  :flow/to :none
  :flow/short-circuit? true
  :flow/predicate :my.ns/adult?}
```

If a flow condition specifies `:all` as its `:flow/to`, it must come before any other flow conditions. If a flow condition specifies `:none` as its `:flow/to`, it must come directly behind an `:all` condition, or first if there is no `:all` condition. This is because of the semantics of short circuiting. We'll discuss what short circuiting means later in this section.

### Short Circuiting

If multiple flow condition entries evaluate to a true predicate, their `:flow/to` values are combined, as well as their `:flow/exclude-keys`. Sometimes you don't want this behavior, and you want to specify exactly the downstream tasks to emit to - and not check any more flow condition entries. You can do this with `:flow/short-circuit?` set to `true`. Any entry that has `:flow/short-circuit?` set to `true` must come before any entries for an task that have it set to `false` or `nil`.