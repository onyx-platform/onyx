## Functions

This section outlines how Onyx programs execute behavior. Onyx uses plain Clojure functions to carry out distributed activity. You have the option of performing grouping and aggregation on each function.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [Functional Transformation](#functional-transformation)
- [Function Parameterization](#function-parameterization)
- [Grouping & Aggregation](#grouping-aggregation)
- [Group By Key](#group-by-key)
- [Group By Function](#group-by-function)
- [Flux Policies](#flux-policies)
  - [Continue Policy](#continue-policy)
  - [Kill Policy](#kill-policy)
  - [Recover Policy](#recover-policy)
- [Bulk Functions](#bulk-functions)
- [Leaf Functions](#leaf-functions)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

#### Functional Transformation

A Function is a construct that takes a segment as a parameter and outputs a segment or a seq of segments. Functions are meant to literally transform a single unit of data in a functional manner. The following is an example of a function:

```clojure
(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))
```

Note that you may *only* pass segments between functions - no other shape of data is allowed.

Example project: [filtering](https://github.com/onyx-platform/onyx-examples/tree/0.8.x/filtering)


#### Function Parameterization

A function can be parameterized before a job is submitted to Onyx. The segment is always the last argument to the function. If more than one of these options are used, the arguments are concatenated in the order that this documentation section lists them. There are three ways to parameterize a function:

- Via the `:onyx.peer/fn-params` peer configuration

```clojure
(def peer-opts
  {...
   :onyx.peer/fn-params {:my-fn-name [:my-args-here]}})
```

The function is then invoked with `(partial f :my-args-here)`.

- Via the `:onyx.core/params` in the `before-task-start` lifecycle hook

```clojure
(defn before-task-start-hook [event lifecycle]
  {:onyx.core/params [:my-args-here]})
```

The function is then invoked with `(partial f :my-args-here)`.

- Via the catalog `:onyx/params` entry

```clojure
(def catalog
{...
 :my/param-1 "abc"
 :my/param-2 "def"
 :onyx/params [:my/param-1 :my/param-2]
 ...}
```

The function is then invoked with `(partial f "abc" "def")`. The order is controlled by the vector of `:onyx/params`.

Example projects: [parameterized](https://github.com/onyx-platform/onyx-examples/tree/0.8.x/parameterized), [interface-injection](https://github.com/onyx-platform/onyx-examples/tree/0.8.x/interface-injection), [catalog-parameters](https://github.com/onyx-platform/onyx-examples/tree/0.8.x/catalog-parameters)

#### Grouping & Aggregation

Grouping ensures that "like" values are always routed to the same virtual peer, presumably to compute an aggregate. Grouping is specified inside of a catalog entry. There are two ways to group: by key of segment, or by arbitrary function. Grouping by key is a convenience that will reach into each segment and pin all segments with the same key value in the segment together. Grouping functions receive a single segment as input. The output of a grouping function is the value to group on. Grouped functions must set keys `:onyx/min-peers` and `:onyx/flux-policy`. See below for a description of these.

#### Group By Key

To group by a key or a vector of keys in a segment, use `:onyx/group-by-key` in the catalog entry:

```clojure
{:onyx/name :sum-balance
 :onyx/fn :onyx.peer.kw-grouping-test/sum-balance
 :onyx/type :function
 :onyx/group-by-key :name
 :onyx/min-peers 3
 :onyx/flux-policy :continue
 :onyx/batch-size 1000}
```

#### Group By Function

To group by an arbitrary function, use `:onyx/group-by-fn` in the catalog entry:

```clojure
{:onyx/name :sum-balance
 :onyx/fn :onyx.peer.fn-grouping-test/sum-balance
 :onyx/type :function
 :onyx/group-by-fn :onyx.peer.fn-grouping-test/group-by-name
 :onyx/min-peers 3
 :onyx/flux-policy :continue
 :onyx/batch-size 1000}
```

#### Flux Policies

Functions that use the grouping feature are presumably stateful. For this reason, once a job begins, no matter how many peers are added to the cluster, no new peers will be allocated to grouping tasks. If we added more peers after the job began, the hashing algorithm lose its consistency, and stateful operations wouldn't work correctly.

Given the fact the Onyx will not add more peers to a grouping task after it begins, we introduce a new parameter - `:onyx/min-peers`. This should be set to an integer that indicates the minimum number of peers that will be allocated to this task before the job can begin. Onyx *may* schedule more than the minimum number that you set. You can create an upper bound by also using `:onyx/max-peers` (example project: [max-peers](https://github.com/onyx-platform/onyx-examples/tree/0.8.x/max-peers)).

One concern that immediately needs to be handled is addressing what happens if a peer on a grouping task leaves the cluster after the job has begun? Clearly, removing a peer from a grouping task also breaks the consistent hashing algorithm that supports statefulness. The policy that is enforced is configurable, and must be chosen by the developer. We offer two policies, outlined below.

##### Continue Policy

When `:onyx/flux-policy` is set to `:continue` on a catalog entry, a peer leaving a grouped task will allow the job to continue executing. The hashing algorithm will not be consistent with its previous behavior, but will be consistent from this point forward unless any other peers leave the task. This is desirable for streaming jobs where the data is theoretically infinite.

##### Kill Policy

When `:onyx/flux-policy` is set to `:kill`, the job is killed and all peers abort execution of the job. Some jobs cannot compute correct answers if there is a shift in the hashing algorithm's consistency. An example of this is a word count batch job.

##### Recover Policy

When `:onyx/flux-policy` is set to `:recover`, the job is continues as is if any peers abort execution of the task. If any other peers are available, they will be added to this task to progressively meet the `:onyx/min-peers` number of peers concurrently working on this task.

#### Bulk Functions

Sometimes you might be able to perform a function more efficiently over a batch of segments rather than processing one segment at a time, such as writing segments to a database in a non-output task. You can receive the entire batch of segments in bulk as an argument to your task by setting `:onyx/bulk?` to `true` in your catalog entry for your function. Onyx will *ignore* the output of your function and pass the same segments that you received downstream. The utility of this feature is that you receive the entire batch in one shot. Onyx ignores your output because it would make it impossible to track which specific messages are children of particular upstream messages - breaking Onyx's fault tolerance mechanism.

Functions with this key enabled may *not* be used with flow conditions. These segments are passed to all immediate downstream tasks.

An example catalog entry:

```clojure
{:onyx/name :inc
 :onyx/fn :onyx.peer.batch-function-test/my-inc
 :onyx/type :function
 :onyx/bulk? true
 :onyx/batch-size batch-size}
```

And an example catalog function to correspond to this entry:

```clojure
(defn my-inc [segments]
  (prn segments)
  :ignored-return-value)
```

The default value for this option is `false`.

#### Leaf Functions

Sometimes you're going to want a node in your workflow with no outgoing connections that doesn't perform I/O against a database. You can do this by setting `:onyx/type` to `:output`, `:onyx/medium` to `:function`, and `:onyx/plugin` to `onyx.peer.function/function`. Then you can specify an `:onyx/fn` pointing to a regular Clojure function. For example:

```clojure
{:onyx/name :leaf-task
 :onyx/fn ::add-to-results
 :onyx/plugin :onyx.peer.function/function
 :onyx/medium :function
 :onyx/type :output
 :onyx/batch-size 20}
```
