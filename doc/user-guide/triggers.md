## Triggers

In this section, we talk about Triggers. Triggers are a feature that interact with *Windows*. Windows capture and bucket data over time. Triggers let you release the captured data over a variety stimuli.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [Summary](#summary)
- [Trigger Types](#trigger-types)
  - [`:timer`](#timer)
  - [`:segment`](#segment)
  - [`:punctuation`](#punctuation)
- [Refinement Modes](#refinement-modes)
  - [`:accumulating`](#accumulating)
  - [`:discarding`](#discarding)
- [Syncing](#syncing)
- [Trigger Specification](#trigger-specification)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Summary

Windows capture data over time and place segments into discrete, possibly overlapping buckets. By itself, this is a relatively useless concept. In order to hardness the information that has been captured and rolled up, we need to *move* it somewhere. Triggers let us interact with the state in each extent of a window.

### Trigger Types

Onyx ships a number of trigger implementations that can be used out of the box. We outline each here and show an example of each in action.

#### `:timer`

This trigger sleeps for a duration of `:trigger/period`. When it is done sleeping, the `:trigger/sync` function is invoked with its usual arguments. The trigger goes back to sleep and repeats itself.

```clojure
{:trigger/window-id :collect-segments
 :trigger/refinement :discarding
 :trigger/on :timer
 :trigger/period [3 :seconds]
 :trigger/sync ::write-to-dynamo
 :trigger/doc "Writes state to DynamoDB every 5 seconds, discarding intermediate state"}
```

#### `:segment`

Trigger wakes up in reaction to a new segment being processed on this task. Trigger only fires once every `:trigger/threshold` segments. When the threshold is exceeded, the count of new segments goes back to `0`, and the looping proceeds again in the same manner.

```clojure
{:trigger/window-id :collect-segments
 :trigger/refinement :accumulating
 :trigger/on :segment
 :trigger/threshold [5 :elements]
 :trigger/sync ::write-to-stdout
 :trigger/doc "Writes the window contents to stdout every 5 segments"}
```

#### `:punctuation`

Trigger wakes up in reaction to a new segment being processed on this task. Trigger only fires if `:trigger/pred` evaluates to `true`. The signature of `:trigger/pred` is of arity-5: `event, window-id, upper, lower, segment`. Punctuation triggers are often useful to send signals through that indicate that no more data will be coming through for a particular window of time.

### Refinement Modes

A refinement mode allows you to articulate what should happen to the state of a window extent after a trigger has been invoked.

#### `:accumulating`

Setting `:trigger/refinement` to `:accumulating` means that the state of a window extent is maintained exactly as is after the trigger invocation. This is useful if you want to an answer to a query to "become more correct over time".

#### `:discarding`

Setting `:trigger/refinement` to `:discarding` means that the state of a window extent is set back to the value it was initialized with after the trigger invocation. You'd want to use this if the results from one periodic update bear no connection to subsequent updates.

### Syncing

Onyx offers you the ultimate flexibility on what to do with your state during a trigger invocation. Set `:trigger/sync` to a fully qualified, namespaced function on the classpath that takes five arguments: the event map, the window ID being triggered, the lower extent bound for this window in the standard units of the window range, the upper extent bound for this window in the standard units of the window range, and the state value. The bounds are useful for determining what this window ID represents. For example, if `:window/range` is specified in `:seconds`, `:minutes`, or some other unit of time, both the upper and lower bounds will be returned in `:milliseconds`. The return value of this function is ignored. You can use lifecycles to supply any stateful connections necessary to sync your data. Supplied values from lifecycles will be available through the first parameter - the event map.

### Trigger Specification

See the Information Model chapter for an exact specification of what values the Trigger maps need to supply. Here we will describe what each of the keys mean.

| key name             |description
|----------------------|-----------
|`:trigger/window-id`  | A `:window/id` specified in the collection of windows
|`:trigger/refinement` | A mode of refinement, one of `:accumlating`, `:discarding`
|`:trigger/on`         | The stimulus to fire the trigger as a reaction.
|`:trigger/sync`       | Fully qualified namespaced keyword of a function to call with the state
|`:trigger/doc`        | An optional docstring explaining the trigger's purpose