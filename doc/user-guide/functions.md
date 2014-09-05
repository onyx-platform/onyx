## Functions

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Functions](#functions)
  - [Transformers](#transformers)
  - [Groupers](#groupers)
  - [Aggregators](#aggregators)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Functions

This section outlines how Onyx programs execute behavior. Onyx uses plain Clojure functions to carry out distributed activity. There are 3 kinds of functions you can use to apply changes to data.

#### Transfomers

A Transformer is a function that takes a segment as a parameter and outputs a segment or a seq of segments. Transformers are meant to literally transform a single unit of data in a functional manner. The following is an example of a transformer:

```clojure
(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))
```

#### Groupers

A Grouper is a function that consolidates "like" values together. Grouping functions receive a single segment as input. The output of a grouper is the value to group on. All segments that are grouped with the same value will be emitted to the *same* virtual peer. If that virtual peer fails, all remaining segments of the group will be pinned to another virtual peer. The following is an example of a grouper:

```clojure
(defn group-by-name [{:keys [name] :as segment}]
  name)
```

#### Aggregators

Aggregators receive segments that have been grouped together by a Grouper. Aggegators must directly follow Groupers in workflows. Aggregators receive a single segment as input, and output a segment or seq of segments. It's often useful to accrue local or durable state in an aggregator. The following is an example of an aggregator. This aggregator is parameterized with an atom to accrue state:

```clojure
(defn sum-balance [state {:keys [name amount] :as segment}]
  (swap! state (fn [v] (assoc v name (+ (get v name 0) amount))))
  [])
```

