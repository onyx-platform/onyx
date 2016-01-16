---
layout: user_guide_page
title: Testing Onyx Jobs
categories: [user-guide-page]
---

## Testing Onyx Jobs

In this chapter, we'll cover what you need to know about testing your Onyx application code.

### Overview

Onyx eschews customized abstractions for describing distributed programs. As a consequence, application code written on top of Onyx ends up being regular Clojure functions and data structures. Onyx remains out of the way, and you're free to test your functions as you would any other Clojure program. It's also useful to roll up your entire job and test it as it will run in production. This section is dedicated to giving you a set of idioms and tools for working with Onyx jobs during the development phase.

#### Automatic Resource Clean up

While it's easy enough to run Onyx with an in-memory ZooKeeper instance (see the Environment chapter for how to do this), there are a host of development comforts that are missing when working on your Onyx code. One key pain point is the clean shutdown of resources on job completion or failure. Often when developing a new Onyx job, you'll make lots of mistakes and your job will be killed. Before the next time you run your job, it's a good idea to make sure your peers, peer group, and development environment all cleanly shut down. This can be a moderately tricky task at the repl where a Thread may be interrupted by a Control-C sequence. To this end, Onyx provides a `onyx.test-helper` namespace with a handy macro known as `with-test-env`.

`with-test-env` takes three parameters: a symbol to bind the development environment var to, a sequence of `[number-of-peers-to-start, peer-config, env-config, [optional-monitoring-config]]`, and a body of expressions. This macro starts a development environment with the requested number of peers, runs the body of expressions, and cleanly shuts the development enivronment down when either the body of expressions completes, or an exception is thrown (included ThreadInterruptedException). You'll notice that this is an anaphoric macro. The macro creates a development environment, then binds it to the user supplied symbol. Using this macro, you don't need to worry about ports remaining open that should have been closed on shutdown. You can safely interrupt Onyx at any point in time during the job execution.

In practice, this looks something like the following:

```clojure
(deftest my-onyx-job-test
  (let [id (java.util.UUID/randomUUID)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/id id)
        peer-config (assoc (:peer-config config) :onyx/id id)]
    (with-test-env [test-env [3 env-config peer-config]]
      (let [catalog ...
            workflow ...
            lifecycles ...]
            (onyx.api/submit-job peer-config
                                   {:catalog catalog
                                    :workflow workflow
                                    :lifecycles lifecycles
                                    :task-scheduler :onyx.task-scheduler/balanced})
        (let [results (take-segments! ...)
              expected ...]
          (is (= expected results)))))))
```

#### Code Reloading

Another painful part of writing asynchronous code in general is the matter of reloading code without restarting your repl. Something that plays particularly well with Onyx is clojure.tools.namespace. A pattern that we like to use is to create a `user.clj` file for the developer with the following contents:

```clojure
(ns user
  (:require [clojure.tools.namespace.repl :refer [refresh set-refresh-dirs]]))

(set-refresh-dirs "src" "test")

(defn init [])

(defn start [])

(defn stop [])

(defn go []
  (init)
  (start))

(defn reset []
  (stop)
  (refresh))
```

When you want to reload your code, invoke the `(reset)` function. You can supply any extra application specific code in `init`, `start`, and `stop`. Combining this pattern with the `with-test-env` macro, you should virtually never need to restart your repl while developing Onyx code.

#### In-Memory I/O

Production level Onyx jobs talk to input streams like Kafka, or databases like Postgres. It's not always helpful to run those pieces of infrastructure while developing your job. Early on, we like to just focus on what the shape of the data will look like and you in-memory I/O with core.async instead of Kafka or what have you. There's plenty of documentation on how to actually use the core.async plugin. The big question is - how does one most effectively use core.async for development, and more realistic I/O targets for staging and production?

Our approach leverages Onyx's all-data job specification. We've found it helpful to isolate the parts of the catalog and lifecycles that will vary between different environments and use a "builder" pattern. We start with a "base job" - the pieces of the job that are invariants across all environments:

```clojure
(defn base-job [mode onyx-id task-scheduler]
  (let [datomic-uri (my-env-helper/get-datomic-uri mode onyx-id)]
    {:workflow wf/parse-event-workflow
     :catalog (cat/build-parse-event-catalog datomic-uri)
     :flow-conditions (fcp/parser-flow-conditions :parse-log
                                                  [:write-parse-failures]
                                                  [:write-to-datomic])
     :lifecycles sl/base-parse-event-lifecycles
     :task-scheduler task-scheduler}))
```

The function which builds the base job takes a "mode", indicating what environment we should construct. We like to use keywords - things like `:dev`, `:staging`, `:prod`. Functions that receive these keywords are often multimethods which dispatch on mode, building the appropriate configuration files. In this example, we use the mode parameter to vary which Datomic URI we're going to use in our catalog.

Next, we add in environment-specific code using a little utility function to update the base job that's passed in as a parameter:

```clojure
(defn add-metrics [job m-config]
  (my-fun-utilities/add-to-job job {:lifecycles
                                     (metrics/build-metrics-lifecycles
                                       (:riemann/host m-config)
                                       (:riemann/port m-config))}))

(defn add-logging [job]
  (my-fun-utilities/add-to-job job {:lifecycles sl/log-parse-event-lifecycles}))
```

Finally, we put them all together with a simple `cond->`:

```clojure
(let [mode :dev
      log? true
      metrics? false]
  (cond-> (base-job mode onyx-id task-scheduler)
    log? (add-logging)
    metrics? (add-metrics m-config)))
```

It's important to remember that we're working with plain Clojure data here. The sky is the limit on how you can put the building blocks together!
