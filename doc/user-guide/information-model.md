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
    - [Maps with `:onyx/type` set to `:transformer`, `:grouper`, or `:aggregator` must have these keys](#maps-with-onyxtype-set-to-transformer-grouper-or-aggregator-must-have-these-keys)
  - [ZooKeeper](#zookeeper)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Workflow

- a single Clojure map which is EDN serializable/deserializable
- all elements in the map are keywords
- all elements in the map must correspond to an `:onyx/name` entry in the catalog
- the outer-most keys of the map must have catalog entries of `:onyx/type` that map to `:input`
- only innermost values of the map may have catalog entries of `:onyx/type` that map to `:output`
- elements in the map with `:onyx/type` mapping to `:aggregator` can only directly follow elements with `:onyx/type` mapping to `:grouper`

### Catalog

- a single Clojure vector which is EDN serializable/deserializable
- all elements in the vector must be Clojure maps

#### All maps in the vector must have these keys

| key name          | type       | choices
|-------------------|------------|----------
|`:onyx/name`       | `keyword`  | `any`
|`:onyx/type`       | `keyword`  | `:input`, `:output`, `:transformer`, `:grouper`, `:aggregator`
|`:onyx/consumption`| `keyword`  | `:sequential`, `:concurrent`
|`:onyx/batch-size` | `integer`  | `>= 0`

#### All maps may optionally have these keys

| key name          | type       | choices
|-------------------|------------|----------
|`:onyx/ident`      | `keyword`  | `any`

#### Maps with `:onyx/type` set to `:input` or `:output` must have these keys

| key name          | type       | choices
|-------------------|------------|----------
|`:onyx/medium`     | `keyword`  | `any`

#### Maps with `:onyx/type` set to `:transformer`, `:grouper`, or `:aggregator` must have these keys

| key name          | type       | choices
|-------------------|------------|----------
|`:onyx/fn`         | `keyword`  | `any`

### ZooKeeper

ZooKeeper itself has an information model which Onyx uses to propagate messages and information to clients. It generally uses a flat structure with UUIDs to cross-reference data nodes. See the diagram below:

![ZooKeeper](http://i.imgur.com/mQ7I9Le.png)
