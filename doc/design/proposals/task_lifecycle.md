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
- Incrementally phase this new proposed design into the `master` branch.

### Current Pieces

After examining the code in its current state (Git SHA `4dd1ce7373c7ad9a812a33c3b6f99e70b90b844b`), we've identified the following as components, concernes, or features in `task_lifecycle.clj`:

- Runtime compilation
- Asynchronous event handling
- Flow conditions
- Lifecycles
- Windowing
- Triggers
- Grouping
- Message acknowledgment
- Bitwise message fusion
- Message lineage tracking
- Error handling
- Searching
- Performance optimizations

#### Runtime compilation

We often make use of runtime compilation by turning a keyword into a symbol, then resolving it to a var. A more advanced example of this is how we handle task lifecycles. A resolve several variables, then construct a new function by composing the functions that we already resolved.

##### Pain Points

- The function that we compile is very opaque. It's hard to tell what it takes as its input, and what output it will produce.
- The functions that we produce are often not tested.

##### Examples

- [Task Lifecycle Atomic Compilation](https://github.com/onyx-platform/onyx/blob/4dd1ce7373c7ad9a812a33c3b6f99e70b90b844b/src/onyx/peer/task_compile.clj#L82-L101)
- [Task Lifecycle Composed Compilation](https://github.com/onyx-platform/onyx/blob/4dd1ce7373c7ad9a812a33c3b6f99e70b90b844b/src/onyx/peer/task_compile.clj#L57-L66)
- [Compiled Windowing Function](https://github.com/onyx-platform/onyx/blob/4dd1ce7373c7ad9a812a33c3b6f99e70b90b844b/src/onyx/peer/task_compile.clj#L171)
