## Onyx Batch Processing Design

### Motivation

From the beginning of the project, one of the primary goals of Onyx has been to unify batch and stream processing behind a single interface. Until now, we've attempted to service both needs with a single design, treating batch and streaming workloads like a sliding scale, never drifting too far in one direction at the cost of another. About one year into the project, we switched from a queueing-based messaging design to a point-to-point messaging layer. The expectation has been that developers can use this architecture directly for streaming, and layer on additional components to achieve batch process semantics. In practice, this ended up being both difficult and inefficient. Some operations, such as sorting, are nearly unachievable with good characteristics using this model.

Onyx 0.8.0 will retain it's current point-to-point direct messaging model, and will adopt an additional, parallel design to handle batch processing. Jobs that are submitted to Onyx will specify whether they should be run in batch or streaming mode, and peers will adjust their internal machinery according to what type of job the task they are executing is.

The emphasis on this architectural shift is on API unification. Reusing as many constructs as possible (workflows, catalogs, functions, and flow conditions) across batch and stream jobs remains a central goal to Onyx. Many streaming jobs should be able to run in batch mode with little-to-no adjustment, with the exception of the input sources of these jobs. Some operations, such as sorting, or taking the distinct elements of a data set, will remain exclusive to batch mode in Onyx. While it's not possible to perform these sorts of operations on infinite streams, we can run validation sanity checks against catalogs and workflows to ensure that an execution mode of Onyx cannot attempt an impossible operation.

The world of batch computation is ripe with analysis and optimization. A huge amount of engineering effort has gone into coming up with the best general strategy for analyzing a static piece of data. Therefore, we'll do the same thing that we did with our streaming design. That is, we will adapt an approach that is well-known, studied, and not hugely difficult to replicate. In this case, we can either try to replicate the work of Hadoop Map-Reduce or Apache Spark. Spark's performance characteristics, in general, are far ahead of Map-Reduce. Onyx's batch design will seek to imitate Spark's design in a limited way. Spark is a fundamentally different kind of processing platform - one that targets interactivity. With that comes special design choices to enhance laziness. Not all of these properties are appropriate for Onyx.

A question that will certainly come up time and time again will be: "If you're implementing Spark's design, why would I use Onyx to begin with when I could just use Spark?" Onyx users stand to gain on several fronts due to it's API being decomposed into data structures.

- Certain Spark transformations, such as `union`, are entirely implicit in Onyx. Since Onyx uses a dedicated data structure, namely the workflow, to articulate the DAG of operations, `union` operations are self-evident from two or more tasks converging on a common task. The explicit API that Onyx needs to expose to developers becomes more concise.

- Performance tuning, among other things, leak directly into Spark programs. The main parallelism knob that Spark users control in their program are partitions. Partitions can be adjusted from node-to-node in the DAG of operations by calling the `repartition`. Performance tuning information is dependent on the details of what cluster the job is running on. Operations like `repartition` will not appear in the Onyx API, as this information can be more readily maintained in the catalog. The logic of the computation doesn't get poisoned by the details of *where* the computation is running.

- Other transformations, such as `group-by-key`, also become implicit. Spark allows users to construct their DAG via function calls, but leaves no other attachment points to parameterize operations on a node-by-node basis. Onyx decomplects most non-functional parameters through the catalog, and communicates information back to DAG tasks at runtime. Onyx's streaming API already handles grouping by one or more keys through the catalog, making grouping an implied step on a task that performs another operation - and only incidentally needs to have data shuffled before it can correctly return the result of its operations. This is again another relief from the incidental complexity of larger data sets. Programmers shouldn't need to pollute their computational structure (workflows) with instructions on how to rearrange data to satisfy a correctness condition for a computation.

### API

Here we sketch out what Onyx's API should look like after we add all of the internal design to support batch processing first class. We extend `:onyx/type` from the values `:input`, `:output`, and `:function` to include more values: `:batch-function` and `:merge-function`. The former allows a function to receive an entire partition of data, and produce an entire partition of data as a result, whereas the latter takes two or more entire partitions and produces one partition.

##### Functions

Some transformations are implicit by using normal Onyx functions. These require no change. Type `:function` gets an element, and produces 0 or more elements.

- `map`
- `mapcat`
- `filter`
- `remove`

Catalog:

```clojure
[{:onyx/name :capitalize-name
  :onyx/fn :my.ns/capitalize-name
  :onyx/type :function
  :onyx/batch-size batch-size
  :onyx/batch-timeout batch-timeout}]
```

##### Batch Functions

Some transformations will shell out to a predefined function via the catalog. `batch-function` means "need to see the entire RDD to produce the next RDD". In other words, "get the entire data set, produce the entire data set".

- `sample`
- `distinct`

Catalog:

```clojure
[{:onyx/name :remove-duplicates
  :onyx/fn :onyx.batch/distinct
  :onyx/type :batch-function
  :onyx/batch-size batch-size
  :onyx/batch-timeout batch-timeout}
  
 {:onyx/name :sample-students
  :onyx/fn :onyx.batch/sample
  :sample/fraction 0.01
  :onyx/type :batch-function
  :onyx/batch-size batch-size
  :onyx/batch-timeout batch-timeout}]
```

##### Implicits

Some operations in Spark that require function calls are implicit in Onyx because the workflow is dedicated to describing the structure of the data movement. Therefore, functions like `union` can be elided and are obvious from a workflow such as `[[:a :c] [:b :c]]`. C's RDD will be the union of A and B. In other cases like grouping, these operations can also become implicit by using the catalog entry to leverage shuffle behavior.

Implicits that won't be present in the Onyx API include:

- `union`
- `group-by-key`

##### By-Key Implicits

Anything that requires a shuffling or grouping by a particular key or keyset can be implicit using the `:onyx/group-by-key` attribute on the task's catalog entry.

These include:
- `reduce`
- `aggregate`
- `sort`

##### Merging Functions

These functions implicitly shuffle their RDDs, and must take more than one upstream task. This can be checked at job submission time.

- `intersection`
- `join`
- `cogroup`
- `cartesian`

```clojure
[{:onyx/name :select-students-and-teachers
  :onyx/fn :onyx.batch/intersection
  :onyx/type :merging-function
  :onyx/batch-size batch-size
  :onyx/batch-timeout batch-timeout}]
```

### Implementation

#### Partitions

#### Log Interactions

#### Fusion

#### Disk I/O

#### Dropping the sentinel value from streaming workloads