## Lifecycles

Lifecycles are a feature that allow you to control code that executes at particular points during task execution on each peer. Lifecycles are data driven and composable.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Summary

There are several interesting points to execute arbitrary code during a task in Onyx. Onyx lets you plug in and calls functions before a task, after a task, before a batch, and after a batch on every peer. Additionally, there is another lifecycle hook that allows you to delay starting a task in case you need to do some work like acquiring a lock under contention. A peer's lifecycle is isolated to itself, and lifecycles never coordinate across peers. Usage of lifecycles are entirely optional. Lifecycle data is submitted as a data structure at job submission time.

### Example

Let's work with an example to show how lifecycles work. Suppose you want to print out a message at all the possible lifecycle hooks. You'd start by defining 5 functions for the 5 hooks:

```clojure
(ns my.ns)

(defn start-task? [event lifecycle]
  (println "Executing once before the task starts.")
  true)

(defn before-task [event lifecycle]
  (println "Executing once before the task starts.")
  {})

(defn after-task [event lifecycle]
  (println "Executing once after the task is over.")
  {})

(defn before-batch [event lifecycle]
  (println "Executing once before each batch.")
  {})

(defn after-batch [event lifecycle]
  (println "Executing once after each batch.")
  {})
```

Notice that all lifecycle functions return maps exception `start-task?`. This map is merged back into the `event` parameter that you received. `start-task?` is a boolean function that allows you to block and back off if you don't want to start the task quite yet. This function will be called periodically as long as `false` is returned. If more than one `start-task?` is specified in your lifecycles, they must all return `true` for the task to begin. `start-task?` is invoked *before* `before-task`.

Next, define a map that wires all these functions together:

```clojure
(def calls
  {:lifecycle/start-task? :my.ns/start-task?
   :lifecycle/before-task :my.ns/before-task
   :lifecycle/before-batch :my.ns/before-batch
   :lifecycle/after-batch :my.ns/after-batch
   :lifecycle/after-task :my.ns/after-task})
```

Each of these 5 keys maps to a fully qualified function as a keyword. All of these keys are optional, so you can mix and match depending on which functions you actually need to use.

Finally, create a lifecycle data structure and pass it to your `onyx.api/submit-job` call:

```clojure
(def lifecycles
  [{:lifecycle/task :my-task-name-here
    :lifecycle/calls :my.ns/calls
    :lifecycle/doc "Test lifecycles and print a message at each stage"}])

(onyx.api/submit-job
  peer-config
  {
  ...
  :lifecycles lifecycles
  ...
  }
```

You can supply as many sets of lifecycles as you want. They are invoked in the order that they are supplied in the vector, giving you a predictable sequence of calls. Be sure that all the keyword symbols and functions are required onto the classpath for the peer that will be executing them.
