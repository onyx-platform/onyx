---
layout: user_guide_page
title: Monitoring
categories: [user-guide-page]
---

## Monitoring

When setting up an Onyx cluster in production, it's helpful to know what Onyx itself is doing. Onyx exposes a set of callbacks that are triggered on certain actions.

### Monitoring Hooks

When you're taking Onyx to production, it's not enough to know what your application-specific code is doing. You need to have insight into how Onyx is operating internally to be able to tune performance at an optimal level. Onyx allows you to register callbacks that are invoked when critical sections of code are exexcuted in Onyx, returning a map that includes latency and data load size if appropriate.

### Callback Specification

Callback functions that are given to Onyx for monitoring take exactly two parameters. The first parameter is the monitoring configuration, the second is a map. The return value of the function is ignored. The event names and keys in their corresponding maps are listed in a later section of this chapter, as well as a discussion about monitoring configuration. Here's an example of a callback function:

```clojure
(defn do-something [config {:keys [event latency bytes]}]
  ;; Write some metrics to Riemann or another type of services.
  (send-to-riemann event latency bytes))
```

### Registering Callbacks

To register callbacks, create a map of event name key to callback function. This map must have a key `:monitoring`, mapped to keyword `:custom`. If you want to ignore all callbacks, you can instead supply `:no-op`. Monitoring is optional, so you can skip any monitoring code entirely if you don't want to use this feature.

A registration example might look like:

```clojure
(defn std-out [config event-map]
  ;; `config` is the `monitoring-config` var.
  (prn event-map))

(def monitoring-config
  {:monitoring :custom
   :zookeeper-write-log-entry std-out
   :zookeeper-read-log-entry std-out
   :zookeeper-write-catalog std-out
   :zookeeper-write-workflow std-out
   :zookeeper-write-flow-conditions std-out
   :zookeeper-force-write-chunk std-out
   :zookeeper-read-catalog std-out
   :zookeeper-read-lifecycles std-out
   :zookeeper-gc-log-entry std-out})

;; Pass the monitoring config as a third parameter to the `start-peers` function.
(def v-peers (onyx.api/start-peers 3 peer-group monitoring-config))
```

### Monitoring Events

This is the list of all monitoring events that you can register hooks for. The keys listed are present in the map that is passed to the callback function. The names of the events should readily identify what has taken place to trigger the callback.

Event name                         | Keys                             |
-----------------------------------|----------------------------------|
`:zookeeper-write-log-entry`       | `:event`, `:latency`, `:bytes`   |
`:zookeeper-read-log-entry`        | `:event`, `:latency`, `:bytes`   |
`:zookeeper-write-catalog`         | `:event`, `:latency`, `:bytes`   |
`:zookeeper-write-workflow`        | `:event`, `:latency`, `:bytes`   |
`:zookeeper-write-flow-conditions` | `:event`, `:latency`, `:bytes`   |
`:zookeeper-write-lifecycles`      | `:event`, `:latency`, `:bytes`   |
`:zookeeper-write-windows`         | `:event`, `:latency`, `:bytes`   |
`:zookeeper-write-triggers`        | `:event`, `:latency`, `:bytes`   |
`:zookeeper-write-job-metadata`    | `:event`, `:latency`, `:bytes`   |
`:zookeeper-write-task`            | `:event`, `:latency`, `:bytes`   |
`:zookeeper-write-job-hash`        | `:event`, `:latency`, `:bytes`   |
`:zookeeper-write-chunk`           | `:event`, `:latency`, `:bytes`   |
`:zookeeper-write-job-scheduler`   | `:event`, `:latency`, `:bytes`   |
`:zookeeper-write-messaging`       | `:event`, `:latency`, `:bytes`   |
`:zookeeper-write-exception`       | `:event`, `:latency`, `:bytes`   |
`:zookeeper-force-write-chunk`     | `:event`, `:latency`, `:bytes`   |
`:zookeeper-write-origin`          | `:event`, `:latency`, `:bytes`   |
`:zookeeper-read-catalog`          | `:event`, `:latency`             |
`:zookeeper-read-workflow`         | `:event`, `:latency`             |
`:zookeeper-read-flow-conditions`  | `:event`, `:latency`             |
`:zookeeper-read-lifecycles`       | `:event`, `:latency`             |
`:zookeeper-read-windows`          | `:event`, `:latency`             |
`:zookeeper-read-triggers`         | `:event`, `:latency`             |
`:zookeeper-read-job-metadata`     | `:event`, `:latency`             |
`:zookeeper-read-task`             | `:event`, `:latency`             |
`:zookeeper-read-job-hash`         | `:event`, `:latency`             |
`:zookeeper-read-chunk`            | `:event`, `:latency`             |
`:zookeeper-read-origin`           | `:event`, `:latency`             |
`:zookeeper-read-job-scheduler`    | `:event`, `:latency`             |
`:zookeeper-read-messaging`        | `:event`, `:latency`             |
`:zookeeper-read-exception`        | `:event`, `:latency`             |
`:zookeeper-gc-log-entry`          | `:event`, `:latency`, `:position`|
`:peer-ack-segments`               | `:event`, `:latency`             |
`:peer-retry-segment`              | `:event`, `:latency`             |
`:peer-complete-message`           | `:event`, `:latency`             |
`:peer-gc-peer-link`               | `:event`                         |
`:peer-backpressure-on`            | `:event`, `:id`                  |
`:peer-backpressure-off`           | `:event`, `:id`                  |
`:group-prepare-join`              | `:event`, `:id`                  |
`:group-notify-join`               | `:event`, `:id`                  |
`:group-accept-join`               | `:event`, `:id`                  |
`:peer-send-bytes`                 | `:event`, `:id`, `:bytes`        |
