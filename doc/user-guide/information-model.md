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
  - [Maps with `:onyx/type` set to `:function` must have these keys](#maps-with-onyxtype-set-to-function-must-have-these-keys)
  - [Maps with `:onyx/type` set to `:function` may optionally have these keys](#maps-with-onyxtype-set-to-function-may-optionally-have-these-keys)
- [Flow Conditions](#flow-conditions)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Workflow

- a single Clojure vector of vectores which is EDN serializable/deserializable
- all elements in the inner vectors are keywords
- all keywords must correspond to an `:onyx/name` entry in the catalog
- the "root" keywords of the workflow must have catalog entries of `:onyx/type` that map to `:input`
- the "leaf" values of the workflow must have catalog entries of `:onyx/type` that map to `:output`

### Catalog

- a single Clojure vector which is EDN serializable/deserializable
- all elements in the vector must be Clojure maps

#### All maps in the vector must have these keys

| key name             | type       | choices                          | default
|----------------------|------------|----------------------------------|--------
|`:onyx/name`          | `keyword`  | `any`                            |
|`:onyx/type`          | `keyword`  | `:input`, `:output`, `:function` |
|`:onyx/batch-size`    | `integer`  | `>= 0`                           |

#### All maps may optionally have these keys

| key name             | type       | choices    | default
|----------------------|------------|------------|--------
|`:onyx/ident`         | `keyword`  | `any`      |
|`:onyx/batch-timeout` | `integer`  | `>= 0`     | `1000`
|`:onyx/max-peers`     | `integer`  | `> 0`      |

#### Maps with `:onyx/type` set to `:input` or `:output` must have these keys

| key name          | type       | choices
|-------------------|------------|----------
|`:onyx/medium`     | `keyword`  | `any`

#### Maps with `:onyx/type` set to `:input` may optionally have these keys

| key name              | type     | default
|-----------------------|----------|--------
|`:onyx/pending-timeout`|`integer` | `60000`
|`:onyx/max-pending`    |`integer` | `10000`

#### Maps with `:onyx/type` set to `:function` must have these keys

| key name          | type       | choices
|-------------------|------------|----------
|`:onyx/fn`         | `keyword`  | `any`

#### Maps with `:onyx/type` set to `:function` may optionally have these keys

| key name           | type       | choices
|--------------------|------------|----------
|`:onyx/group-by-key`| `keyword`  | `any`
|`:onyx/group-by-fn` | `keyword`  | `any`


### Flow Conditions

- a single Clojure vector which is EDN serializable/deserializable
- all elements in the vector must be Clojure maps

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

