## onyx-hornetq

Onyx plugin providing read and write facilities for HornetQ non-clustered.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [Installation](#installation)
- [Catalog entries](#catalog-entries)
  - [read-segments](#read-segments)
  - [write-segments](#write-segments)
- [Attributes](#attributes)
- [Contributing](#contributing)
- [License](#license)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

#### Installation

This plugin is shipped by Onyx itself, so there's nothing extra to include in your project file.

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.hornetq])
```

#### Catalog entries

##### read-segments

```clojure
{:onyx/name :read-segments
 :onyx/ident :hornetq/read-segments
 :onyx/type :input
 :onyx/medium :hornetq
 :hornetq/queue-name in-queue
 :hornetq/host hornetq-host
 :hornetq/port hornetq-port
 :onyx/batch-size batch-size
 :onyx/doc "Reads segments from HornetQ"}
```

##### write-segments

```clojure
{:onyx/name :out
 :onyx/ident :hornetq/write-segments
 :onyx/type :output
 :onyx/medium :hornetq
 :hornetq/queue-name out-queue
 :hornetq/host hornetq-host
 :hornetq/port hornetq-port
 :onyx/batch-size batch-size
 :onyx/doc "Writes segments to HornetQ"}
```

#### Attributes

|key                     | type      | description
|------------------------|-----------|------------
|`:hornetq/queue-name`   | `string`  | The name of the queue to connect to
|`:hornetq/host`         | `string`  | The hostname of the HornetQ server to connect to
|`:hornetq/port`         | `integer` | The port of the HornetQ server to connect to

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2014 Michael Drogalis

Distributed under the Eclipse Public License, the same as Clojure.
