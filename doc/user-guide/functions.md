## Functions

This section outlines how Onyx programs execute behavior. Onyx uses plain Clojure functions to carry out distributed activity. You have the option of performing grouping and aggregation on each function.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [Functional Transformation](#functional-transformation)
- [Function Parameterization](#function-parameterization)
- [Grouping & Aggregation](#grouping-&-aggregation)
- [Group By Key](#group-by-key)
- [Group By Function](#group-by-function)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

#### Functional Transformation

A Function is a construct that takes a segment as a parameter and outputs a segment or a seq of segments. Functions are meant to literally transform a single unit of data in a functional manner. The following is an example of a function:

```clojure
(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))
```

#### Function Parameterization

A function can be parameterized before a job is submitted to Onyx. The segment is always the last argument to the function. If more than one of these options are used, the arguments are concatenated in the order that this documentation section lists them. There are three ways to parameterize a function:

- Via the `:onyx.peer/fn-params` peer configuration

```clojure
(def peer-opts
  {...
   :onyx.peer/fn-params {:my-fn-name [:my-args-here]}})
```

The function is then invoked with `(partial f :my-args-here)`.

- Via the `:onyx.peer-fn-params` in the `inject-lifecycle-resources` multimethod

```clojure
(defmethod onyx.peer.task-lifecycle-extensions/inject-lifecycle-resources
  :my-fn-name-or-identity
  [_ context]
    {:onyx.core/fn-params [:my-args-here]})
```

The function is then invoked with `(partial f :my-args-here)`.

- Via the catalog `:onyx/params` entry

```clojure
(def catalog
{...
 :my/param-1 "abc"
 :my/param-2 "def"
 :onyx/params [:my/param-1 :my/param-2]
 ...}
```

The function is then invoked with `(partial f "abc" "def")`. The order is controlled by the vector of `:onyx/params`.

#### Grouping & Aggregation

Grouping means consolidates "like" values together to the same virtual peer to presumably compute an aggregate. Grouping is specified inside of a catalog entry. There are two ways to two group: by key of segment, or by arbitrary function. Grouping by key is a convenience that will reach into each segment and pin all segments with the same key value in the segment together. Grouping functions receive a single segment as input. The output of a grouping function is the value to group on. If a virtual peer receiving grouped segments fails, all remaining segments of the group will be pinned to another virtual peer.

#### Group By Key

To group by a key in a segment, use `:onyx/group-by-key` in the catalog entry:

```clojure
{:onyx/name :sum-balance
 :onyx/ident :onyx.peer.kw-grouping-test/sum-balance
 :onyx/fn :onyx.peer.kw-grouping-test/sum-balance
 :onyx/type :function
 :onyx/group-by-key :name
 :onyx/batch-size 1000}
```

#### Group By Function

To group by an arbitrary function, use `:onyx/group-by-fn` in the catalog entry:

```clojure
{:onyx/name :sum-balance
 :onyx/ident :onyx.peer.fn-grouping-test/sum-balance
 :onyx/fn :onyx.peer.fn-grouping-test/sum-balance
 :onyx/type :function
 :onyx/group-by-fn :onyx.peer.fn-grouping-test/group-by-name
 :onyx/batch-size 1000}
```

