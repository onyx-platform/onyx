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
- Flow conditions
- Lifecycles
- Windowing
- Triggers
- Grouping
- Message acknowledgment
- Bitwise message fusion
- Message lineage tracking
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