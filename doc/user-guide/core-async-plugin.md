## onyx-core-async

Onyx plugin providing read and write facilities for Clojure core.async.

#### Installation

This plugin is included with Onyx. You do not need to add it as a separate dependency.

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.core-async])
```

#### Catalog entries

##### read-from-chan

```clojure
{:onyx/name :in
 :onyx/ident :core.async/read-from-chan
 :onyx/type :input
 :onyx/medium :core.async
 :onyx/batch-size batch-size
 :onyx/max-peers 1
 :onyx/doc "Reads segments from a core.async channel"}
```

##### write-to-chan

```clojure
{:onyx/name :out
 :onyx/ident :core.async/write-to-chan
 :onyx/type :output
 :onyx/medium :core.async
 :onyx/batch-size batch-size
 :onyx/max-peers 1
 :onyx/doc "Writes segments to a core.async channel"}
```

#### Attributes

This plugin does not use any attributes.

#### Lifecycle Arguments

References to core.async channels must be injected for both the input and output tasks.

##### `read-from-chan`

```clojure
(defmethod l-ext/inject-lifecycle-resources :my.input.task.identity-or-name
  [_ _] {:core.async/chan (chan capacity)})
```

##### `write-to-chan`

```clojure
(defmethod l-ext/inject-lifecycle-resources :my.output.task.identity-or-name
  [_ _] {:core.async/chan (chan capacity)})
```

#### Functions

##### `take-segments!`

This additional function is provided as a utility for removing segments
from a channel until `:done` is found. After `:done` is encountered, all prior segments,
including `:done`, are returned in a seq.

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2015 Michael Drogalis

Distributed under the Eclipse Public License, the same as Clojure.
