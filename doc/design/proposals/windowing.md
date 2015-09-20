## Onyx Windowing

### Research

We're actively looking over the following papers for ideas on how to best design windowing. We summarize the papers to some extent, and note which parts we want to use and what the drawbacks are.

- [The Dataflow Model: A Practical Approach to Balancing Correctness, Latency, and Cost in Massive-Scale, Unbounded, Out-of-Order Data Processing](vldb.org/pvldb/vol8/p1792-Akidau.pdf)
- [Semantics and Evaluation Techniques for Window Aggregates in Data Streams](http://web.cecs.pdx.edu/~tufte/papers/WindowAgg.pdf)
- [Exploiting Predicate-window Semantics over Data Streams](http://docs.lib.purdue.edu/cgi/viewcontent.cgi?article=2621&context=cstech)
- [How Soccer Players Would do Stream Joins](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.362.2471&rep=rep1&type=pdf)
- [No Pane, No Gain: Efficient Evaluation of Sliding-Window Aggregates over Data Streams](https://cs.brown.edu/courses/cs227/papers/opt-slidingwindowagg.pdf)


#### No Pane, No Gain: Efficient Evaluation of Sliding-Window Aggregates over Data Streams

This paper is mainly about an optimization that can be applied to windowing that helps computation and memory costs. Panes split windows into pieces, and those pieces can be shared across other windows to avoid recomputation. Directly from the paper, we see a good problem statement:

> Current proposals for evaluating sliding-window aggregate
> queries buffer each input tuple until it is no longer needed
> [1]. Since each input tuple belongs to multiple windows,
> such approaches buffer a tuple until it is processed for the
> aggregate over the last window to which it belongs. Each
> input tuple is accessed multiple times, once for each window
> that it participates in.

The authors go on to say:

> We see two problems with such approaches. First the
> buffer size required is unbounded: At any time instant, all
> tuples contained in the current window are in the buffer,
> and so the size of the required buffers is determined by the
> window range and the data arrival rate. Second, processing
> each input tuple multiple times leads to a high computation
> cost. For example in Query 1, each input tuple is processed
> four times. As the ratio of RANGE over SLIDE increases,
> so does the number of times each tuple is processed. Considering
> the large volume and fast arrival rate of streaming
> data, reducing the amount of required buffer space (ideally
> to a constant bound) and computation time is an important issue.

##### Motivation

A good example of where panes come in handy is derived from Query 4 of the
paper:

> Over the past 10 minutes, find the ids of the
> auction items on which the number of bids is greater than
> or equal to 5% of the total number of bids; update the result
> every 1 minute.

This is a relatively complex query that requires a lot of intermediate state
to progressively update the answer every minute. This query's internal representation
would get unwieldly without some form of optimization.

##### Implementation

When we use panes, we take the original query specification and split
it into a Window Level Query (WLQ) and Pane Level Query (PLQ). The PLQ
does *sub-aggregation*. The WLQ does *super-aggregation*. The number of
panes, the range, and slide values of each query are determined programmatically.
They are not tunable configuration values because the number of panes is
used to determine windows can share panes.

##### Types of Queries

Not all queries with panes should behave the same way. Some queries can be
even further optimized. This paper breaks queries down into:

- holistic
- bounded
- fully differential
- pseudo differential
- none of the above

Each of these have properties that allows varying levels of computational shortcuts,
so we should figure out which we can handle most easily and plan for future
implementations.

##### Drawbacks

Panes can fail to be useful and degrade performance when there aren't
"enough" segments in each pane. We should provide a configuration
switch to not use panes. This configuration option can be a secondary
feature, though.

### Windowing API characteristics

Onyx's windowing API should:

- Be a low-level data structure that Continuous Query Language (CQL) can compile to
- Support fixed (tumbling), sliding, session, and global windows.
- Provide enough expressivity for the window to be created by features of the data itself
- Support time-based windows (event timestamps, processing timestamps) and "tuple-based" windows (e.g. window of n tuples)
- Support punctuation, timer-based, external event, and watermark triggers
- Allow triggers to compose
- Be internally optimized to use panes
- Allow expression of predicates for when a segment should enter and exit a window
- Allow triggers to be reused across different windows
- Support different strategies to change data after trigger fires (e.g. discarding, accumulating, etc)
- Support retraction (e.g. negative tuples)
- Support incremental aggregation for things like sums
- Support buffered aggregation for things like windowed joins where all the data for a window is needed
- Provide *some* support for load shedding in the case of aggregation where data must be buffered
- Provide expressivity for merging windows back together.
