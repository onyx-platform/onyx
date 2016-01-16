## Plugins

Plugins serve as an abstract to compose mechanisms for getting data in and out of Onyx. See the README.md of the project for a list of official Onyx plugins, or keep reading to roll your own.

### Interfaces

In order to implement a plugin, one or more protocols need to be implemented from the [Pipeline Extensions API](../../src/onyx/peer/pipeline_extensions.clj). Reader plugins will implement PipelineInput and Pipeline. Writer plugins will implement Pipeline. See the docstrings for instructions on implementation.

### Templates

To help move past the boilerplate of creating new plugins, use Leiningen with [`onyx-plugin`](https://github.com/onyx-platform/onyx-plugin) to generate a template.

### Coordination within Plugins

Often virtual peers allocated to a task may need to coordinate with respect to
allocating work. For example, a Kafka reader task may need to assign partitions
to different peers on the same topic.  The Onyx mechanism for coordinating
peers is the
[log](https://github.com/onyx-platform/onyx/blob/master/doc/user-guide/architecture-low-level-design.md#the-log).
The Onyx log is extensible by plugins, by implementing several extensions defmethods.

For example:

```clojure

(ns your.plugin.log-commands
  (:require [onyx.extensions :as extensions]))


(defmethod extensions/apply-log-entry :yourplugin/coordination-type
  [{:keys [args]} replica]
  replica)

(defmethod extensions/replica-diff :yourplugin/coordination-type
  [{:keys [args]} old new]
  {})
  

(defmethod extensions/reactions :yourplugin/coordination-type
  [{:keys [args]} old new diff peer-args]
  [])

(defmethod extensions/fire-side-effects! :yourplugin/coordination-type
  [{:keys [args]} old new diff {:keys [monitoring] :as state}]
  state)
```

When modifying the replica, please assoc-in into replica under [:task-metadata job-id task-id], so that it will be cleaned up when the job is completed or killed.
