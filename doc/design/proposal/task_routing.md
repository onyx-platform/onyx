## Conditional Task Routing

This design proposal outlines an approach that Onyx could take to routing segments conditionally to tasks.

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

Task entries in catalogs can additionally define a :onyx/routing-fn with the following definition:
example-router-fn (input-segment, output-segment, catalog, task)

and which returns a list of the tasks that the segment should be routed to
(which must all be tasks with incoming edges from the task).

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
   :onyx/routing-fn :filter.core/even-odd-router
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
(defn even-odd-router [_ output-segment _ _]
  (if (even? (:n output-segment))
      #{:even-out}
      #{:odd-out}))

```

Note that in this case, as :in is an input task, input-segment will be nil. One
open question: it may not be necessary to pass in the input segment to the
routing tasks. It may also come at the cost of some complexity so this should
be considered. 

You could also implement a routing function like the following:

```
(defn dropping-router [_ output-segment _ _]
  (if (even? (:n output-segment))
      #{:even-out :odd-out}
      #{}))
```

The default behaviour (i.e. without a routing-fn defined) would essentially be
a function that returned all tasks with incoming edges from the task, i.e.
``` #{:even-out :odd-out} ``` 
in this example.

### Additional proposals

#### `` :onyx/route-by-key ```

Similar to ``` :onyx/group-by-key ```, routes tasks based on a key in the
output segment from the task. Probably not as useful as group-by-key as you
probably don't want the task info in the segments that you send around -
however maybe some of the receiving tasks want to know where they came from.

### Alternative proposal

Something to do with lifecycle and pipeline extensions that allows the user to
override the behaviour but which will require them to get into more onyx
internals.
