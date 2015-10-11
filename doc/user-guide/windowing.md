## Windowing and Aggregation

This section discusses a feature called windowing. Windows allow you to group and accrue data into possibly overlapping buckets.  Windows are intimately related to the Triggers feature. When you're finished reading this section, head over to the Triggers chapter next.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [Summary](#summary)
- [Window Types](#window-types)
  - [Fixed Windows](#fixed-windows)
  - [Sliding Windows](#sliding-windows)
  - [Global Windows](#global-windows)
- [Units](#units)
- [Aggregation](#aggregation)
  - [`:conj`](#conj)
  - [`:count`](#count)
  - [`:sum`](#sum)
  - [`:min`](#min)
  - [`:max`](#max)
  - [`:average`](#average)
- [Window Specification](#window-specification)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Summary

Windowing splits up a possibly unbounded data set into finite, possibly overlapping portions. Windows allow us create aggregations over distinct portions of a stream, rather than stalling and waiting for the entire data data set to arrive. In Onyx, Windows strictly describe how data is accrued. When you want to *do* something with the windowed data, you use a Trigger. See the chapter on Triggers for more information. Onyx's windowing mechanisms are strong enough to handle stream disorder. If your data arrives in an order that isn't "logical" (for example, `:event-time` keys moving backwards in time), Onyx can sort out the appropriate buckets to put the data in.

We used a significant amount of academic and industrial research to implement these features. If you're interested, we documented and summarized a portion of the papers we used [here](../design/proposals/windowing.md).

### Window Types

The topic of windows has been widely explored in the literature. There are different *types* of windows. Currently, Onyx supports Fixed, Sliding, Global, and Session windows. We will now explain the supported window types.

#### Fixed Windows

Fixed windows, sometimes called Tumbling windows, span a particular range and do not slide. That is, fixed windows never overlap one another. Consequently, a data point will fall into exactly one instance of a window (often called an *extent* in the literature). As it turns out, fixed windows are a special case of sliding windows where the range and slide values are equal. You can see a visual below of how this works, where the `|--|` drawings represent extents. Each window is of range `5`. Time runs horizontally, while the right-hand side features the extent bound running vertically. The first extent captures all values between 0 and 4.99999...

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

#### Global Windows

Global windows are perhaps the easiest to understand. With global windows, there is one window extent that match all data that enters it. This lets you capture events that span over an entire domain of time. Global windows are useful for batch-style computations.

#### Session Windows

Session windows are windows that dynamically resize their upper and lower bounds. Sessions capture a time span of activity for a specific key, such as a user ID. If no activity occurs within a timeout gap, the session ends. If an event occurs within the bounds of a session, the window size is fused with the new event, and the session is extended by its timeout gap.

### Units

Onyx allows you to specify range and slide values in different magnitudes of units, so long as the units can be coverted to the same unit in the end. For example, you can specify the range in minutes, and the slide in seconds. Any value that requires units takes a vector of two elemenets. The first element represents the value, and the second the unit. For example, window specifications denoting range and slide might look like:

```clojure
{:window/range [1 :minute]
 :window/slide [30 :seconds]}
```

See the information model for all supported units. You can use a singular form (e.g. `:minute`) instead of the plural (e.g. `:minutes`) where it makes sense for readability.

Onyx is also capable of sliding by `:elements`. This is often referred to as "slide-by-tuple" in research. Onyx doesn't require a time-based range and slide value. Any totally ordered value will work equivalently.

### Aggregation

Windows allow you accrete data over time. Sometimes, you want to store all the data. Othertimes you want to incrementally compact the data. Window specifications must provide a `:window/aggregation` key. We'll go over an example of every type of aggregation that Onyx supports.

#### `:onyx.windowing.aggregation/conj`

The `:conj` aggregation is the simplest. It collects segments for this window and retains them in a vector, unchanged.

```clojure
{:window/id :collect-segments
 :window/task :identity
 :window/type :sliding
 :window/aggregation :onyx.windowing.aggregation/conj
 :window/window-key :event-time
 :window/range [30 :minutes]
 :window/slide [5 :minutes]
 :window/doc "Collects segments on a 30 minute window sliding every 5 minutes"}
```

#### `:onyx.windowing.aggregation/count`

The `:onyx.windowing.aggregation/count` operation counts the number of segments in the window.

```clojure
{:window/id :count-segments
 :window/task :identity
 :window/type :fixed
 :window/aggregation :onyx.windowing.aggregation/count
 :window/window-key :event-time
 :window/range [1 :hour]
 :window/doc "Counts segments in one hour fixed windows"}
```

#### `:onyx.windowing.aggregation/sum`

The `:sum` operation adds the values of `:age` for all segments in the window.

```clojure
{:window/id :sum-ages
 :window/task :identity
 :window/type :fixed
 :window/aggregation [:onyx.windowing.aggregation/sum :age]
 :window/window-key :event-time
 :window/range [1 :hour]
 :window/doc "Adds the :age key in all segments in 1 hour fixed windows"}
```

#### `:onyx.windowing.aggregation/min`

The `:min` operation retains the minimum value found for `:age`. An initial value must be supplied via `:window/init`.

```clojure
{:window/id :min-age
 :window/task :identity
 :window/type :fixed
 :window/aggregation [:onyx.windowing.aggregation/min :age]
 :window/init 100
 :window/window-key :event-time
 :window/range [30 :minutes]
 :window/doc "Finds the minimum :age in 30 minute fixed windows, default is 100"}
```

#### `:onyx.windowing.aggregation/max`

The `:max` operation retains the maximum value found for `:age`. An initial value must be supplied via `:window/init`.

```clojure
{:window/id :max-age
 :window/task :identity
 :window/type :fixed
 :window/aggregation [:onyx.windowing.aggregation/max :age]
 :window/init 0
 :window/max-key :age
 :window/window-key :event-time
 :window/range [30 :minutes]
 :window/doc "Finds the maximum :age in 30 minute fixed windows, default is 0"}
```

#### `:onyx.windowing.aggregation/average`

The `:average` operation maintains an average over `:age`. The state is maintained as a map with two keys - `:n`, the number of elements, and `:average`, the running average.

```clojure
{:window/id :average-age
 :window/task :identity
 :window/type :fixed
 :window/aggregation [:onyx.windowing.aggregation/average :age]
 :window/window-key :event-time
 :window/range [30 :minutes]
 :window/doc "Finds the average :age in 30 minute fixed windows, default is 0"}
```

### Window Specification

See the Information Model chapter for an exact specification of what values the Window maps need to supply. Here we will describe what each of the keys mean.

| key name             |description
|----------------------|-----------
|`:window/id`          | A unique identifier per window
|`:window/task`        | The workflow task over which the window operates
|`:window/type`        | Which type of window this is (fixed, sliding, etc)
|`:window/aggregation` | The aggregation function to apply, as described above. If this operation is over a key, this is a vector, with the second element being the key.
|`:window/window-key`  | The key over which the range will be calculated
|`:window/range`       | The span of the window
|`:window/slide`       | The delay to wait to start a new window after the previous window
|`:window/init`        | The initial value required for some types of aggregation
|`:window/min-value`   | A strict miminum value that `:window/window-key` can ever be, default is 0.
|`:window/doc`         | An optional docstring explaining the window's purpose
