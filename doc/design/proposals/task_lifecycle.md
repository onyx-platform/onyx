## Scratch pad for Task Lifecycle Redesign

### Overview

As of Onyx 0.8.0, we're ready to rewrite what is now known as `task_lifecycle.clj`. This namespace has served as the backbone for stream processing since this project has begun. It started out as a simple conveyor belt to move values across a sequence of core.async channels. Over time, we added new features - like flow conditions, windowing, and runtime compiled lifecycles. Each addition has contributed to the deterioration of this namespace.

This file is particularly important not only because it's the "brains" of the stream processing engine, but also because it's extremely performance sensitive. We've extensively tuned the code in this path. To our detrement, it's not always when we've done something in the name of performance. Accepting user patches in this space comes with a very high risk.

Now that Onyx is stabilizing feature-wise, this is a good time to rewrite this portion of the code base. We have a significantly better idea about what behavior this code needs to support, how to tune it for high performance, and what the threading model we've elected looks like.

### Goals

- Have a thorough documentation about what task lifecycle is, what it's pieces are, and how it works.
- Maintain a series of notes, referred to in the code by header (e.g. `;; perf-64`), that indicate what is being done for performance.
- List out each thread that we're running (both Java and Go green threads) and what its purpose is.
- Increase the test coverage of this area by approaching it with a test.check mindset.
- Prepare for the implementation of a batch engine by factoring out streaming engine specifics.
- Reduce the number of mutable components as close to 1 atom as possible.
- Incrementally phase this new proposed design into the `master` branch.

### Current Pieces

After examining the code in its current state (Git SHA `4dd1ce7373c7ad9a812a33c3b6f99e70b90b844b`), we've identified the following as components, concernes, or features in `task_lifecycle.clj`:

- Runtime compilation
- Asynchronous event handling
- Searching
- Grouping
- Message lineage tracking
- Bitwise message fusion
- Message acknowledgment
- Flow conditions
- Lifecycles
- Windowing
- Triggers
- Error handling
- Performance optimizations

#### Runtime compilation

We often make use of runtime compilation by turning a keyword into a symbol, then resolving it to a var. A more advanced example of this is how we handle task lifecycles. A resolve several variables, then construct a new function by composing the functions that we already resolved.

##### Pain Points

- The function that we compile is very opaque. It's hard to tell what it takes as its input, and what output it will produce.
- The functions that we produce are often not tested.
- It's not always obvious which keys in the event map are precompiled ahead of time, and which are compiled during each batch.

##### Suggestions

- Use a special notation for runtime compiled functions in the event map, such as `:onyx.compiled/my-function`, rather than `:onyx.core/my-function`.
- Enhance `task_compile.clj` to be a more serious compiler. This would mean finding the commonality of all the runtime compiled pieces and documenting what happens at each pass.

##### Examples

- [Task Lifecycle Atomic Compilation](https://github.com/onyx-platform/onyx/blob/4dd1ce7373c7ad9a812a33c3b6f99e70b90b844b/src/onyx/peer/task_compile.clj#L82-L101)
- [Task Lifecycle Composed Compilation](https://github.com/onyx-platform/onyx/blob/4dd1ce7373c7ad9a812a33c3b6f99e70b90b844b/src/onyx/peer/task_compile.clj#L57-L66)
- [Compiled Windowing Function](https://github.com/onyx-platform/onyx/blob/4dd1ce7373c7ad9a812a33c3b6f99e70b90b844b/src/onyx/peer/task_compile.clj#L171)

#### Asynchronous event handling

Task lifecycle, as it stands, needs to be able to react to outside, asynchronous events. These events include notification of a fully acknowledged segment, notification of a force-retried segment, a command to shut down the current task, and so forth.

##### Pain Points

- Since all of the state that a task accretes in held within task lifecycle, events that need affect change to that state must asynchronously contact the task. This requires launching multiple threads, reading from each thread asynchronously, and manipulating the stateful component. There are a lot of subtle places that we've made mistakes here - like having an exception thrown in the asynchronous reading and not recovering.
- Channel buffers have proven to be a finicky thing. When we use regular core.async buffers, we halt upstream work completely and potentionally deadlock. When we use sliding or dropping buffers, we lose data, and often have a very hard time figuring out where we lost it, and why.
- Figuring out the default size for any channel buffer has been hard.
- Some tasks need more threads than others. For example, input tasks have the responsibility of listening for fully completed segments, and for replaying timed out segments. This gets awkward for function and output tasks, since they can completely ignore this work.

##### Suggestions

- Move as much data outside of the task lifecycle as possible into "sibling components". These components should pass the data into the lifecycle. Doing it this way encourages task lifecycle to be a piece of sequential (non-concurrent) code.
- Thoroughly document what each channel's purpose is, and what channels it receives data from and writes data to. Understanding the full graph of channels will help us come up with better default buffer sizes, and understand any performance or retry mishaps.

##### Examples

- [Task Lifecycle Auxillary Threads](https://github.com/onyx-platform/onyx/blob/4dd1ce7373c7ad9a812a33c3b6f99e70b90b844b/src/onyx/peer/task_lifecycle.clj#L446-L486)
- [Input Retried Segments](https://github.com/onyx-platform/onyx/blob/4dd1ce7373c7ad9a812a33c3b6f99e70b90b844b/src/onyx/peer/task_lifecycle.clj#L488-L505)

#### Searching

There are a handful of instances where we need to look for something in a collection either before the task begins, or as it runs. Examples include finding an entry in the catalog, or finding information about a downstream task for grouping.

##### Pain Points

- We don't always consider what happens when we don't find the thing we were looking for, or what happens if we find more than one matching thing. Sometimes garbage-in-garbage out is good, but other times we can give ourselves much better error messages to detect deeper problems.
- We can probably optimize this code a lot, or at least document it when we don't need high performance when searching through a collection.

##### Suggestions

- Create a standard set of search utilities that are tuned for performance, documenting to the caller what happens when the target isn't found.
- Add only the search utilities that we need. It's tempting to add an entire suite of search tools, but this is best left for repositories like lib-onyx.
- Remove all ad-hoc searching code. I (Michael) certainly wrote this kind of code more than a few times in haste.

##### Examples

- [Lifecycle Searching](https://github.com/onyx-platform/onyx/blob/4dd1ce7373c7ad9a812a33c3b6f99e70b90b844b/src/onyx/peer/task_compile.clj#L37-L39)
- [Trigger Searching](https://github.com/onyx-platform/onyx/blob/4dd1ce7373c7ad9a812a33c3b6f99e70b90b844b/src/onyx/peer/task_compile.clj#L127-L130)
- [Sentinel ID Searching](https://github.com/onyx-platform/onyx/blob/4dd1ce7373c7ad9a812a33c3b6f99e70b90b844b/src/onyx/peer/task_lifecycle.clj#L64-L66)

#### Grouping

Grouping is the behavior of sending segments to downstream peers in a "sticky" fashion. We do this by hashing the segment according to a particular function, then matching it up with the last peer that we sent the hash value to.

##### Pain Points

- The idea of "hash grouping" everything got a little hazy. It's hard to understand the relationship between the segments and their hashed values.
- Handling what happens when a peer leaves, and hence the hash function becomes inconsistent, is a ill-defined in terms of code. We support this first class with flux conditions, but the methodology by how we prove stability could be clarified.

##### Suggestions

- ?

##### Examples

- [Segment Hashing](https://github.com/onyx-platform/onyx/blob/4dd1ce7373c7ad9a812a33c3b6f99e70b90b844b/src/onyx/peer/task_lifecycle.clj#L124-L131)
- [Peer Picking](https://github.com/onyx-platform/onyx/blob/4dd1ce7373c7ad9a812a33c3b6f99e70b90b844b/src/onyx/peer/function.clj#L30)
- [Peer Picking Function Compilation](https://github.com/onyx-platform/onyx/blob/4dd1ce7373c7ad9a812a33c3b6f99e70b90b844b/src/onyx/log/commands/peer_replica_view.clj#L15-L37)

#### Message Lineage Tracking

When a segment enters a task, it may create new segments. The lineage of the segment to its children, and ancestors, is tracked through a shared identifier. Within a task, we need to track all of the new segments, and which segment they were created from.

##### Pain Points

- We created a data structure to point from the original segment to the new segments, but it's deeply nested, and the keys have confusing names.

##### Suggestions

- Create and document a new data structure that maintains the mapping from old to new segments. It's critical that we document the structure of this as it's one of the main pieces for ensuring correctness. We also should consider using Schema here.

##### Examples

- [Building New Segments](https://github.com/onyx-platform/onyx/blob/4dd1ce7373c7ad9a812a33c3b6f99e70b90b844b/src/onyx/peer/task_lifecycle.clj#L188-L201)
#### Bitwise Message Fusion

Our streaming engine maintains a compact representation of the lineage of a single segment in just a few bytes. This algorithm is documented in the user guide. In order to minimize the number of bytes that we send over the network, we perform an exclusive-or operation at the site of the task, then we send that value over the wire instead. We call this process fusion.

##### Pain Points

- We sneak in this fusion at an arbitrary point in time. It's hard to test and debug any potentional problems that are related to ack values.

##### Suggestions

- Make fusion a function of a Result type - something that can be created on-demand, rather than stored and tracked within the type itself.

##### Examples

- [Add New Segments](https://github.com/onyx-platform/onyx/blob/4dd1ce7373c7ad9a812a33c3b6f99e70b90b844b/src/onyx/peer/task_lifecycle.clj#L141-L156)

#### Message Acknowledgment

Message acknowledge is the process by which an input task maintains a set of segments pending completion. An external signal, via the acking daemon, sends a message to release segments from the pool.

##### Pain Points

- The state for the pending pool is kept within the task itself. This means signals need to be asynchronously propagated into the task, bringing all the pains of the asynchronous event handling section.
- Since not all tasks need to perform this operation, there is conditional code in task lifecycle to only do certain things depending on the type of task.

##### Suggestions

- Move the stateful component (pool of pending messages) outside of the task lifecycle and into a sibling component.

##### Examples

- [Periodic Message Retry](https://github.com/onyx-platform/onyx/blob/4dd1ce7373c7ad9a812a33c3b6f99e70b90b844b/src/onyx/peer/task_lifecycle.clj#L488-L505)