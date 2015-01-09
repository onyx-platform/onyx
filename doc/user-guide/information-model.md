## Information Model

This chapter specifies what a valid catalog and workflow look like, as well as how the underlying ZooKeeper representation is realized.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Information Model](#information-model)
  - [Workflow](#workflow)
  - [Catalog](#catalog)
    - [All maps in the vector must have these keys](#all-maps-in-the-vector-must-have-these-keys)
    - [All maps may optionally have these keys](#all-maps-may-optionally-have-these-keys)
    - [Maps with `:onyx/type` set to `:input` or `:output` must have these keys](#maps-with-onyxtype-set-to-input-or-output-must-have-these-keys)
    - [Maps with `:onyx/type` set to `:function` must have these keys](#maps-with-onyx-type-set-to-function-must-have-these-keys)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Workflow

- a single Clojure map which is EDN serializable/deserializable
- all elements in the map are keywords
- all elements in the map must correspond to an `:onyx/name` entry in the catalog
- the outer-most keys of the map must have catalog entries of `:onyx/type` that map to `:input`
- only innermost values of the map may have catalog entries of `:onyx/type` that map to `:output`

### Catalog

- a single Clojure vector which is EDN serializable/deserializable
- all elements in the vector must be Clojure maps

#### All maps in the vector must have these keys

| key name             | type       | choices                          | default
|----------------------|------------|----------------------------------|--------
|`:onyx/name`          | `keyword`  | `any`                            |
|`:onyx/type`          | `keyword`  | `:input`, `:output`, `:function` |
|`:onyx/consumption`   | `keyword`  | `:sequential`, `:concurrent`     |
|`:onyx/batch-size`    | `integer`  | `>= 0`                           |

#### All maps may optionally have these keys

| key name             | type       | choices   | default
|----------------------|------------|---------------------
|`:onyx/ident`         | `keyword`  | `any`     |
|`:onyx/batch-timeout` | `integer`  | `>= 0`    | 1000
|`:onyx/max-peers`     | `integer`  | `> 0`     |

#### Maps with `:onyx/type` set to `:input` or `:output` must have these keys

| key name          | type       | choices
|-------------------|------------|----------
|`:onyx/medium`     | `keyword`  | `any`

#### Maps with `:onyx/type` set to `:function` must have these keys

| key name          | type       | choices
|-------------------|------------|----------
|`:onyx/fn`         | `keyword`  | `any`

#### Maps with `:onyx/type` set to `:function` may optionally have these keys

| key name           | type       | choices
|--------------------|------------|----------
|`:onyx/group-by-key`| `keyword`  | `any`
|`:onyx/group-by-fn` | `keyword`  | `any`

