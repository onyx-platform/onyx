## Onyx Windowing

Research:

- [The Dataflow Model: A Practical Approach to Balancing Correctness, Latency, and Cost in Massive-Scale, Unbounded, Out-of-Order Data Processing](vldb.org/pvldb/vol8/p1792-Akidau.pdf)
- [Semantics and Evaluation Techniques for Window Aggregates in Data Streams](http://web.cecs.pdx.edu/~tufte/papers/WindowAgg.pdf)
- [Exploiting Predicate-window Semantics over Data Streams](http://docs.lib.purdue.edu/cgi/viewcontent.cgi?article=2621&context=cstech)
- [How Soccer Players Would do Stream Joins](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.362.2471&rep=rep1&type=pdf)
- [No Pane, No Gain: Efficient Evaluation of Sliding-Window Aggregates over Data Streams](https://cs.brown.edu/courses/cs227/papers/opt-slidingwindowagg.pdf)


#### No Pane, No Gain: Efficient Evaluation of Sliding-Window Aggregates over Data Streams

This paper is mainly about an optimization that can be applied to windowing that helps computation and memory costs. Panes split windows into pieces, and those pieces can be shared across other windows. Directly from the paper, we see a good problem statement:

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