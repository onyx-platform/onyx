## Windowing and Aggregation

This section discusses a feature called windowing. Windows allow you to group and accrue data into possibly overlapping buckets.

### Summary

Windowing partitions a possibly unbounded data set into finite, possibly overlapping portions. Windows all us to work with aggregations over distinct portions of a stream, rather than stalling and waiting for the entire data stream to arrive, if it's even bounded. Windows are intimately related to the Triggers feature. When you're finished reading this section, head over to the Triggers chapter next.

### Window Types

#### Fixed Windows

Fixed windows, sometimes called Tumbling windows, span a particular range and do not slide. That is, fixed windows never overlap one another. Consequently, a data point will fall into exactly one instance of a window (called an *extent* in the literature) As it turns out, fixed windows are a special case of sliding windows where the range and slide values are equal. You can see a visual below, where the `|--|` drawings represent extents. Time runs horizontally, while the right-hand side features the extent bound running vertically. The first extent captures all values between 0 and 4.99999... for instance.

```text
1, 5, 10, 15, 20, 25, 30, 35, 40
|--|                                [0  - 4]
   |--|                             [5  - 9]
      |---|                         [10 - 14]
          |---|                     [15 - 19]
              |---|                 [20 - 24]
                  |---|             [25 - 29]
                      |---|         [30 - 34]
                          |---|     [35 - 39]
```

#### Sliding Windows

#### Range and Slide

#### Units

#### Aggregation

Initial values.


