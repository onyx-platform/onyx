## Conditional Task Routing

This design proposal outlines an approach that Onyx could take to routing
segments conditionally between tasks.

### Background

It may be useful to be able to run jobs that implement conditional routing of segments.

In order to implement the conditional routing of segments from an :in task to
one of two output tasks (:out1, :out2) one can currently define a workflow:

##### Workflow
```
[[:in :filter-even] 
 [:in :filter-odd] 
 [:filter-odd :odd-out] 
 [:filter-even :even-out]]
```

##### Catalog
```
 [{:onyx/name :in
   :onyx/ident :core.async/read-from-chan
   :onyx/type :input
   :onyx/medium :core.async
   :onyx/consumption :concurrent
   :onyx/batch-size batch-size
   :onyx/doc "Reads segments from a core.async channel"}

  {:onyx/name :filter-even
   :onyx/fn :filtering.core/filter-even
   :onyx/type :function
   :onyx/consumption :concurrent
   :onyx/batch-size batch-size}

  {:onyx/name :filter-odd
   :onyx/fn :filtering.core/odd?
   :onyx/type :function
   :onyx/consumption :concurrent
   :onyx/batch-size batch-size}

  {:onyx/name :even-out
   :onyx/ident :core.async/write-to-chan
   :onyx/type :output
   :onyx/medium :core.async
   :onyx/consumption :concurrent
   :onyx/batch-size batch-size
   :onyx/doc "Writes segments to a core.async channel"}

  {:onyx/name :odd-out
   :onyx/ident :core.async/write-to-chan
   :onyx/type :output
   :onyx/medium :core.async
   :onyx/consumption :concurrent
   :onyx/batch-size batch-size
   :onyx/doc "Writes segments to a core.async channel"}]
```

and the required functions:

```
(defn filter-even [segment]
  (if (even? (:n segment))
      segment
      []))

(defn filter-odd [segment]
  (if (odd? (:n segment))
      segment
      []))
```

A segment {:n 0} placed on the input core.async channel, will be routed to the
:even-out task (and respective channel). The segment will be dropped by the
:filter-odd task and will not be routed to the :odd-out task.

One upside of this approach is that the dataflow is still clear from the
workflow. Segments that appear on :in have a path to :even-out and :odd-out,
and it is easy to check that there are no cycles in the DAG.

There are a few downsides of this approach however. The :in task must send all
the segments twice (additional communication overhead), and the conditional
code is run twice. In addition, the conditional code comes at the cost of some
complexity and debuggability, as it
is separated into the filter-even and filter-odd function.

### Proposal

Task entries in catalogs can additionally define a :onyx/route-filter-fn
with the following definition:

example-router-fn (input-segment, output-segment, catalog, task, output-tasks) 

and which returns a list of the tasks that the segment should be routed to.
These tasks must be all be  tasks with incoming edges from the task, hence the
"filter" in the name.

Alternative definition under this format:


#### Workflow
```
[[:in :odd-out] 
 [:in :even-out]]
```

#### Catalog
```
 [{:onyx/name :in
   :onyx/ident :core.async/read-from-chan
   :onyx/type :input
   :onyx/medium :core.async
   :onyx/route-filter-fn :filter.core/even-odd-router
   :onyx/consumption :concurrent
   :onyx/batch-size batch-size
   :onyx/doc "Reads segments from a core.async channel"}

  {:onyx/name :even-out
   :onyx/ident :core.async/write-to-chan
   :onyx/type :output
   :onyx/medium :core.async
   :onyx/consumption :concurrent
   :onyx/batch-size batch-size
   :onyx/doc "Writes segments to a core.async channel"}

  {:onyx/name :odd-out
   :onyx/ident :core.async/write-to-chan
   :onyx/type :output
   :onyx/medium :core.async
   :onyx/consumption :concurrent
   :onyx/batch-size batch-size
   :onyx/doc "Writes segments to a core.async channel"}])
```

and the required routing function:

```
(defn even-odd-router [_ output-segment _ _ output-tasks]
  {:post [(= % (clojure.set/intersection output-tasks %))]}
  (if (even? (:n output-segment))
      #{:even-out}
      #{:odd-out}))
```

Note that in this case, as :in is an input task, input-segment will be nil. One
open question: it may not be necessary to pass in the input segment to the
routing tasks. It may also come at the cost of some complexity so this should
be considered. 

You can also define routing functions that do not return any output tasks, e.g.
a routing function that randomly drops segments.

```
(defn random-dropping-router [_ output-segment _ _ output-tasks]
  {:post [(= % (clojure.set/intersection output-tasks %))]}
  (if (zero? (rand-int 2))
      #{:even-out :odd-out}
      #{}))
```

The default behaviour (i.e. without a routing-fn defined) would essentially be
a function that returned all tasks with incoming edges from the task, i.e.
``` #{:even-out :odd-out} ``` 
in this example.

### Additional proposals

#### ``` :onyx/route-filter-by-key ```

Similar to ``` :onyx/group-by-key ```, routes tasks based on a lookup by key in
the output segment. This is probably not as useful as group-by-key as you probably
don't want the task info in the segments that you send around - however maybe
some of the receiving tasks want to know where else the segment was sent?

#### Filter on the incoming task

Probably quite difficult to do this right.

### Alternative proposal

Something to do with lifecycle and pipeline extensions that allows the user to
override the behaviour but which will require them to get into more onyx
internals. Implementing it this way also takes it out of the catalog data and
puts it somewhere that it is not as data driven.

### Separate data structure proposal

Computational structure (workflow) is its own concern. Parameterization (catalog) is its own concern. Data flow conditions (to be named) is its own concern, and hence merits its own data structure.

To figure out which downstream tasks a segment should be sent to, a predicate function needs more than the segment itself. Sometimes the direction to take is a function of the old segment that the new segment was derived from, a function of *all* the new segments generated by an old segment, or side effects of the transformation function (think the result value of calling an upsert database function).

A helpful way to consider routing in our case would be a state machine. Task A may have downstream tasks B, C, and D. Routes might include sending a segment to B, C, D, B & C, B & D, C & D, none of the tasks, or all of the tasks. The combinations are where this all starts to get interesting. One could conceive of a routing data structure passed to `submit-job` that looks like:

```clojure
[{:route/from :a
  :route/to :all
  :route/exclude-keys [:my-side-effects-result-key]
  :route/predicate :my/all-predicate-fn}
 {:route/from :a
  :route/to :none
  :route/exclude-keys [:my-side-effects-result-key]
  :route/predicate :my/none-predicate-fn}
 {:route/from :a
  :route/to [:b]
  :route/exclude-keys [:my-side-effects-result-key]
  :route/predicate :my/b-only-predicate-fn}
 ...
 ]
```

If any `:route/to` matches that *are not* sequences, the scan of which routes to take stops. Otherwise, any matches sequences (`[:b]`, etc), are conj'ed together to produce the final route set.

Since the predicate function needs to access things like side effect results, we use a `:route/exclude-keys` to strip out any keys from the result *before* passing the segmnt downstream.

Predicate functions should have one arity:

```clojure
(defn my-pred
  ([event old-segment all-new-semgents this-new-segment]))
```

`event` is the event context map passed to all pipeline multimethods.

#### Composition

We need a way to compose predicates at runtime with `and`, `or`, and `not`.

```clojure
{:route/from :a
 :route/to :all
 :route/exclude-keys [:my-side-effects-result-key]
 :route/predicate [:or [:my-pred-1 [:and :my-pred-1 :my-pred-3] [:not :my-pred-4]]]}
```

TBD: How to parameterize these predicate functions?
