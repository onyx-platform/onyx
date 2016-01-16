---
layout: user_guide_page
title: core.async Plugin
categories: [user-guide-page]
---

## onyx-core-async

Onyx plugin providing read and write facilities for Clojure core.async.

#### Installation

This plugin is included with Onyx. You do not need to add it as a separate dependency.

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.core-async])
```

#### Functions

##### read-from-chan

Catalog entry:

```clojure
{:onyx/name :in
 :onyx/plugin :onyx.plugin.core-async/input
 :onyx/type :input
 :onyx/medium :core.async
 :onyx/batch-size batch-size
 :onyx/max-peers 1
 :onyx/doc "Reads segments from a core.async channel"}
```

Lifecycle entries:

```clojure
[{:lifecycle/task :your-task-name
  :lifecycle/calls :my.ns/in-calls}
 {:lifecycle/task :your-task-name
  :lifecycle/calls :onyx.plugin.core-async/reader-calls}]
```

There's a little extra baggage with core.async because you need a reference to the channel.
Make sure that `my.ns/in-calls` is a map that references a function to inject the channel in:

```clojure
(def in-chan (chan capacity))

(defn inject-in-ch [event lifecycle]
  {:core.async/chan in-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})
```

##### write-to-chan

Catalog entry:

```clojure
{:onyx/name :out
 :onyx/plugin :onyx.plugin.core-async/output
 :onyx/type :output
 :onyx/medium :core.async
 :onyx/batch-size batch-size
 :onyx/max-peers 1
 :onyx/doc "Writes segments to a core.async channel"}
```

Lifecycle entries:

```clojure
[{:lifecycle/task :your-task-name
  :lifecycle/calls :my.ns/out-calls}
 {:lifecycle/task :your-task-name
  :lifecycle/calls :onyx.plugin.core-async/writer-calls}]
```

Again, as with `read-from-chan`, there's a little extra to do since core.async has some exceptional behavior compared to other plugins:

```clojure
(def out-chan (chan capacity))

(defn inject-out-ch [event lifecycle]
  {:core.async/chan out-chan})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})
```

#### Utility Functions

##### `take-segments!`

This additional function is provided as a utility for removing segments
from a channel until `:done` is found. After `:done` is encountered, all prior segments,
including `:done`, are returned in a seq.

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2015 Michael Drogalis

Distributed under the Eclipse Public License, the same as Clojure.
