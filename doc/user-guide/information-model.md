## Information Model

This section specifies what a valid catalog, workflow, and flow conditions look like.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [Workflow](#workflow)
- [Catalog](#catalog)
    - [All maps in the vector must have these keys](#all-maps-in-the-vector-must-have-these-keys)
    - [All maps may optionally have these keys](#all-maps-may-optionally-have-these-keys)
    - [Maps with `:onyx/type` set to `:input` or `:output` must have these keys](#maps-with-onyxtype-set-to-input-or-output-must-have-these-keys)
    - [Maps with `:onyx/type` set to `:input` may optionally have these keys](#maps-with-onyxtype-set-to-input-may-optionally-have-these-keys)
    - [Maps with `:onyx/type` set to `:function` must have these keys](#maps-with-onyxtype-set-to-function-must-have-these-keys)
    - [Maps with `:onyx/type` set to `:function` may optionally have these keys](#maps-with-onyxtype-set-to-function-may-optionally-have-these-keys)
    - [Maps with `:onyx/group-by-key` or `:onyx/group-by-fn` must have these keys](#maps-with-onyxgroup-by-key-or-onyxgroup-by-fn-must-have-these-keys)
- [Flow Conditions](#flow-conditions)
- [Lifecycles](#lifecycles)
- [Windows](#windows)
- [Triggers](#triggers)
- [Units](#units)
- [Event Context](#event-context)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Workflow

- a single Clojure vector of vectors which is EDN serializable/deserializable
- all elements in the inner vectors are keywords
- all keywords must correspond to an `:onyx/name` entry in the catalog
- the "root" keywords of the workflow must have catalog entries of `:onyx/type` that map to `:input`

### Catalog

- a single Clojure vector which is EDN serializable/deserializable
- all elements in the vector must be Clojure maps

##### All maps in the vector must have these keys

| key name             | type       | choices                          |
|----------------------|------------|----------------------------------|
|`:onyx/name`          | `keyword`  | `any`                            |
|`:onyx/type`          | `keyword`  | `:input`, `:output`, `:function` |
|`:onyx/batch-size`    | `integer`  | `>= 0`                           |

##### All maps may optionally have these keys

| key name              | type       | choices              | default    |    Meaning                                                            |
|-----------------------|------------|----------------------|------------|-----------------------------------------------------------------------|
|`:onyx/batch-timeout`  | `integer`  | `>= 0`               | `1000`     |                                                                       |
|`:onyx/max-peers`      | `integer`  | `> 0`                |            |                                                                       |
|`:onyx/n-peers`        | `integer`  | `> 0`                |            | Expands to make `:onyx/min-peers` and `:onyx/max-peers this value.    |
|`:onyx/language`       | `keyword`  | `:clojure`, `:java`  | `:clojure` | Affects `:onyx/fn` and `:onyx/plugin` function and plugin resolution  |
|`:onyx/restart-pred-fn`| `keyword`  | `any`                |            | Keyword pointing to function taking an exception which returns a boolean for whether a peer is restartable following that exception  |

##### Maps with `:onyx/type` set to `:input` or `:output` must have these keys

| key name          | type       | choices
|-------------------|------------|----------
|`:onyx/medium`     | `keyword`  | `any`
|`:onyx/plugin`     | `keyword`  | `any`

##### Maps with `:onyx/type` set to `:input` may optionally have these keys

| key name                   | type     | default | unit        |
|----------------------------|----------|---------|-------------|
|`:onyx/pending-timeout`     |`integer` | `60000` | milliseconds|
|`:onyx/input-retry-timeout `|`integer` | `1000`  | milliseconds|
|`:onyx/max-pending`         |`integer` | `10000` | segments    |

##### Maps with `:onyx/type` set to `:function` must have these keys

| key name          | type       | choices
|-------------------|------------|----------
|`:onyx/fn`         | `keyword`  | `any`

##### Maps with `:onyx/type` set to `:function` may optionally have these keys

| key name                 | type       | choices | default
|--------------------------|------------|---------|--------
|`:onyx/group-by-key`      | `[keyword]`| `any`   |
|`:onyx/group-by-fn`       | `keyword`  | `any`   |
|`:onyx/bulk?`             | `boolean`  |         | `false`

##### Maps with `:onyx/group-by-key` or `:onyx/group-by-fn` must have these keys

| key name                 | type       | choices             |
|--------------------------|------------|---------------------|
|`:onyx/min-peers`         | `integer`  | `any`               |
|`:onyx/flux-policy`       | `keyword`  | `:kill`, `:continue`|

##### Maps with associated windows, may have these keys

| key name                 | type       | choices             |
|--------------------------|------------|---------------------|
|`:onyx/uniqueness-key`    | `any`      |                     |


### Flow Conditions

- a single Clojure vector which is EDN serializable/deserializable
- all elements in the vector must be Clojure maps
- allows arbitrary key/values in the map as parameters

| key name                |type                          | optional?| default
|-------------------------|------------------------------|----------|--------
|`:flow/from`             |`keyword`                     | no       |
|`:flow/to`               |`:all`, `:none` or `[keyword]`| no       |
|`:flow/predicate`        |`keyword` or `[keyword]`      | no       |
|`:flow/exclude-keys`     |`[keyword]`                   | yes      | `[]`
|`:flow/short-circuit?`   |`boolean`                     | yes      |`false`
|`:flow/thrown-exception?`|`boolean`                     | yes      |`false`
|`:flow/post-transform?`  |`keyword`                     | yes      |`nil`
|`:flow/action?`          |`keyword`                     | yes      |`nil`

### Lifecycles

- a single Clojure vector which is EDN serializable/deserializable
- all elements in the vector must be Clojure maps
- allows arbitrary key/values in the map as parameters

| key name                |type                          | optional?|
|-------------------------|------------------------------|----------|
|`:lifecycle/name`        |`keyword`                     | no       |
|`:lifecycle/calls`       |`keyword`                     | no       |
|`:lifecycle/doc`         |`string`                      | yes      |

### Windows

- a single Clojure vector which is EDN serializable/deserializable
- all elements in the vector must be Clojure maps
- allows arbitrary key/values in the map as parameters

| key name             |type                 | optional?|
|----------------------|---------------------|----------|
|`:window/id`          |`keyword`            | no       |
|`:window/task`        |`keyword`            | no       |
|`:window/type`        |`keyword`            | no       |
|`:window/aggregation` |`vector` or `keyword`| no       |
|`:window/window-key`  |`keyword`            | no       |
|`:window/min-value`   |`int`                | yes      |
|`:window/range`       |`vector`             | no       |
|`:window/slide`       |`vector`             | sometimes|
|`:window/init`        |`any`                | sometimes|
|`:window/doc`         |`string`             | yes      |

`:window/range` and `:window/slide` are values that require Units. See below for a description of Units.

### Triggers

- a single Clojure vector which is EDN serializable/deserializable
- all elements in the vector must be Clojure maps
- allows arbitrary key/values in the map as parameters

| key name                   |type       | optional?|
|----------------------------|-----------|----------|
|`:trigger/window-id`        |`keyword`  | no       |
|`:trigger/refinement`       |`keyword`  | no       |
|`:trigger/on`               |`keyword`  | no       |
|`:trigger/sync`             |`keyword`  | no       |
|`:trigger/fire-all-extents?`|`boolean`  | yes      |
|`:trigger/doc`              |`string`   | yes      |

### Units

Several values in Onyx require units. Units are vectors of two elements. The first element represents a value, and the second the unit (e.g. `[5 :minutes]`). Onyx supports the following units:

| unit             
|---------------
|`:milliseconds`
|`:seconds`
|`:minutes`
|`:hours`
|`:days`
|`:elements`

Onyx also allows you to specify a single form (`:minute`) anywhere that it is needed for readability.

### Event Context

Onyx exposes an "event context" through many of its APIs. This is a description of
what you will find in this map and what each of its key/value pairs mean. More keys
may be added by the user as the context is associated to throughout the task pipeline.

| key name                     |value type | Meaning       |
|------------------------------|-----------|---------------|
|`:onyx.core/id`               |`uuid`     | The unique ID of this peer's lifecycle|
|`:onyx.core/lifecycle-id`     |`uuid`     | The unique ID for this *execution* of the lifecycle|
|`:onyx.core/job-id`           |`uuid`     | The Job ID of the task that this peer is executing|
|`:onyx.core/task-id`          |`uuid`     | The Task ID that this peer is executing|
|`:onyx.core/task`             |`keyword`  | The task name that this peer is executing|
|`:onyx.core/catalog`          |`vector`   | The full catalog for this job|
|`:onyx.core/workflow`         |`vector`   | The workflow for this job|
|`:onyx.core/flow-conditions`  |`vector`   | The flow conditions for this job|
|`:onyx.core/compiled-norm-fcs`|`vector`   | A sequence of flow conditions with precompiled predicate functions, excluding exception flow conditions|
|`:onyx.core/compiled-ex-fcs`  |`vector`   | A sequence of flow conditions with precompiled predicate functions, excluding non-exception flow conditions|
|`:onyx.core/task-map`         |`map`      | The catalog entry for this task|
|`:onyx.core/serialized-task`  |`map`      | The task that this peer is executing that has been serialized to ZooKeeper|
|`:onyx.core/params`           |`vector`   | The parameter sequence to be applied to the function that this task uses|
|`:onyx.core/drained-back-off` |`long`     | The amount of time to back off when the input is drained|
|`:onyx.core/log`              |`record`   | The Log record Component|
|`:onyx.core/messenger-buffer` |`channel`  | The messenger buffer core.async channel for this peer|
|`:onyx.core/messenger`        |`record`   | The Messenger record Component|
|`:onyx.core/outbox-ch`        |`channel`  | The outbox core.async channel for this peer|
|`:onyx.core/seal-ch`          |`channel`  | The core.async channel to deliver seal notifications for this job|
|`:onyx.core/peer-opts`        |`map`      | The options that this peer was started with|
|`:onyx.core/replica`          |`atom`     | The replica that this peer has currently accrued|
|`:onyx.core/state`            |`atom`     | The state that this peer has accrued|
|`:onyx.core/batch`            |`vector`   | The sequence of segments read by this peer|
|`:onyx.core/results`          |`map`      | A map of read segment to a vector of segments produced by applying the function of this task|
