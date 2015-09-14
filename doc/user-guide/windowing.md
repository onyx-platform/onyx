## Windowing and Aggregation

This section discusses a feature called windowing. Windows allow you to group and accrue data into possibly overlapping buckets.  Windows are intimately related to the Triggers feature. When you're finished reading this section, head over to the Triggers chapter next.

### Summary

Windowing splits up a possibly unbounded data set into finite, possibly overlapping portions. Windows allow us create aggregations over distinct portions of a stream, rather than stalling and waiting for the entire data data set to arrive. In Onyx, Windows strictly describe how data is accrued. When you want to *do* something with the windowed data, you use a Trigger. See the chapter on Triggers for more information.

### Window Types

The topic of windows has been widely explored in the literature. There are different *types* of windows. Currently, Onyx supports Fixed and Sliding windows. In the future, we will support landmark, global, and session windows. We will now explain the supported window types.

#### Fixed Windows

Fixed windows, sometimes called Tumbling windows, span a particular range and do not slide. That is, fixed windows never overlap one another. Consequently, a data point will fall into exactly one instance of a window (often called an *extent* in the literature). As it turns out, fixed windows are a special case of sliding windows where the range and slide values are equal. You can see a visual below of how this works, where the `|--|` drawings represent extents. Time runs horizontally, while the right-hand side features the extent bound running vertically. The first extent captures all values between 0 and 4.99999...

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

In contrast to fixed windows, sliding windows allow extents to overlap. When a sliding window is specified, we have to give it a range for which the window spans, and a *slide* value for how long to wait between spawning a new window extent. Every data point will fall into exactly `range / slide` number of window extents. We draw out what this looks like for a sliding window with range `15` and slide `5`:

```text
1, 5, 10, 15, 20, 25, 30, 35, 40
|---------|                         [0  - 14]
   |----------|                     [5  - 19]
      |-----------|                 [10 - 24]
          |-----------|             [15 - 29]
              |-----------|         [20 - 34]
                  |-----------|     [25 - 39]
```

#### Units

Onyx allows you to specify range and slide values in different magnitudes of units, so long as the units can be coverted to the same unit in the end. For example, you can specify the range in minutes, and the slide in seconds. Any value that requires units takes a vector of two elemenets. The first element represents the value, and the second the unit. For example, window specifications denoting range and slide might look like:

```clojure
{:window/range [1 :minute]
 :window/slide [30 :seconds]}
```

See the information model for all supported units. You can use a singular form (e.g. `:minute`) instead of the plural (e.g. `:minutes`) where it makes sense for readability.

Onyx is also capable of sliding by `:elements`. This is often referred to as "slide-by-tuple" in research. Onyx doesn't require a time-based range and slide value. Any totally ordered value will work equivalently.

#### Aggregation

Initial values.


#### Window Specifications

