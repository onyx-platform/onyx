## Lifecycles

Lifecycles are a feature that allow you to control code that executes at particular points during task execution on each peer. Lifecycles are data driven and composable.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [Summary](#summary)
- [Lifecycle Phases](#lifecycle-phases)
  - [Before task set up](#before-task-set-up)
  - [Before task execution](#before-task-execution)
  - [Before segment batch start](#before-segment-batch-start)
  - [After segment batch start](#after-segment-batch-start)
  - [After task execution](#after-task-execution)
- [Example](#example)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Summary

There are several interesting points to execute arbitrary code during a task in Onyx. Onyx lets you plug in and calls functions before a task, after a task, before a batch, and after a batch on every peer. Additionally, there is another lifecycle hook that allows you to delay starting a task in case you need to do some work like acquiring a lock under contention. A peer's lifecycle is isolated to itself, and lifecycles never coordinate across peers. Usage of lifecycles are entirely optional. Lifecycle data is submitted as a data structure at job submission time.

### Lifecycle Phases

#### Before task set up

A function that takes two arguments - an event map, and the matching lifecycle map. Must return a boolean value indicating whether to start the task or not. If false, the process backs off for a preconfigured amount of time and calls this task again. Useful for lock acquisition. This function is called prior to any processes inside the task becoming active.

#### Before task execution

A function that takes two arguments - an event map, and the matching lifecycle map. Must return a map that is merged back into the original event map. This function is called after processes in the task are launched, but before the peer listens for incoming segments from other peers.

#### Before segment batch start

A function that takes two arguments - an event map, and the matching lifecycle map. Must return a map that is merged back into the original event map. This function is called prior to receiving a batch of segments from the reading function.

#### After segment batch start

A function that takes two arguments - an event map, and the matching lifecycle map. Must return a map that is merged back into the original event map. This function is called after all messages have been written and acknowledged.

#### After task execution

A function that takes two arguments - an event map, and the matching lifecycle map. Must return a map that is merged back into the original event map. This function is called before the peer relinquishes its task. No more segments will be received.

#### After ack message

A function that takes four arguments - an event map, a message id, the return of an input plugin ack-message call, and the matching lifecycle map. May return a value of any type which will be discarded. This function is whenever a segment at the input task has been fully acked.

#### After retry message

A function that takes four arguments - an event map, a message id, the return of an input plugin ack-message call, and the matching lifecycle map. May return a value of any type which will be discarded. This function is whenever a segment at the input task has been pending for greater than pending-timeout time and will be retried.


### Example

Let's work with an example to show how lifecycles work. Suppose you want to print out a message at all the possible lifecycle hooks. You'd start by defining 5 functions for the 5 hooks:

```clojure
(ns my.ns)

(defn start-task? [event lifecycle]
  (println "Executing once before the task starts.")
  true)

(defn before-task-start [event lifecycle]
  (println "Executing once before the task starts.")
  {})

(defn after-task-stop [event lifecycle]
  (println "Executing once after the task is over.")
  {})

(defn before-batch [event lifecycle]
  (println "Executing once before each batch.")
  {})

(defn after-batch [event lifecycle]
  (println "Executing once after each batch.")
  {})

(defn after-ack-message [event message-id rets lifecycle]
  (println "Message " message-id " is fully acked"))

(defn after-retry-message [event message-id rets lifecycle]
  (println "Retrying message " message-id))

```

Notice that all lifecycle functions return maps exception `start-task?`. This map is merged back into the `event` parameter that you received. `start-task?` is a boolean function that allows you to block and back off if you don't want to start the task quite yet. This function will be called periodically as long as `false` is returned. If more than one `start-task?` is specified in your lifecycles, they must all return `true` for the task to begin. `start-task?` is invoked *before* `before-task-start`.

Next, define a map that wires all these functions together by mapping predefined keywords to the functions:

```clojure
(def calls
  {:lifecycle/start-task? start-task?
   :lifecycle/before-task-start before-task
   :lifecycle/before-batch before-batch
   :lifecycle/after-batch after-batch
   :lifecycle/after-task-stop after-task
   :lifecycle/after-ack-message after-ack-message
   :lifecycle/after-retry-message after-retry-message})
```

Each of these 5 keys maps to a function. All of these keys are optional, so you can mix and match depending on which functions you actually need to use.

Finally, create a lifecycle data structure by pointing `:lifecycle/calls` to the fully qualified namespaced keyword that represents the calls map that we just defined. Pass it to your `onyx.api/submit-job` call:

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
