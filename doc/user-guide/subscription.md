## Event Subscription

Onyx's log-based design provides open-ended access to react to all coordination events. This section describes how to tap into these notifications.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [Explanation](#explanation)
- [Subscribing to the Log](#subscribing-to-the-log)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Explanation

Onyx uses an internal log to totally order all coordination events across nodes in the cluster. This log is maintained as a directory of sequentially ordered znodes in ZooKeeper. It's often of interest to watch the events as they are written to the log. For instance, you may want to know when a particular task is completed, or when a peer joins or leaves the cluster. You can use the log subscriber to do just that.

### Subscribing to the Log

The following is a complete example to pretty print all events as they are written to the log. You need to provide the ZooKeeper address, Onyx ID, and shared job scheduler in the peer config. The subscriber will automatically track recover from sequentially reading errors in the case that a garbage collection is triggered, deleting log entries in its path.

```clojure
(def peer-config
  {:zookeeper/address "127.0.0.1:2181"
   :onyx/id onyx-id
   :onyx.peer/job-scheduler :onyx.job-scheduler/round-robin})

(def ch (chan 100))

(def subscription (onyx.api/subscribe-to-log peer-config ch))

(def log (:log (:env subscription)))

;; Loops forever
(loop [replica (:replica subscription)]
  (let [entry (<!! ch)
        new-replica (onyx.extensions/apply-log-entry entry replica)]
    (clojure.pprint/pprint new-replica)
    (recur new-replica)))

(onyx.api/shutdown-env (:env subscription))
```

Some example output from a test, printing the log position, log entry content, and the replica as-of that log entry:

```clojure
====
Log Entry #0
Entry is {:message-id 0, :fn :prepare-join-cluster, :args {:joiner #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577"}}
Replica is:
{:peer-state {#uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577" :idle},
 :peers [#uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577"],
 :job-scheduler :onyx.job-scheduler/greedy}
====
Log Entry #1
Entry is {:message-id 1, :fn :prepare-join-cluster, :args {:joiner #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"}}
Replica is:
{:prepared
 {#uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577"
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"},
 :peer-state {#uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577" :idle},
 :peers [#uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577"],
 :job-scheduler :onyx.job-scheduler/greedy}
====
Log Entry #2
Entry is {:message-id 2, :fn :notify-join-cluster, :args {:observer #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf", :subject #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577"}, :immediate? true}
Replica is:
{:accepted
 {#uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577"
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"},
 :prepared {},
 :peer-state {#uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577" :idle},
 :peers [#uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577"],
 :job-scheduler :onyx.job-scheduler/greedy}
====
Log Entry #3
Entry is {:message-id 3, :fn :accept-join-cluster, :args {:observer #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf", :subject #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577", :accepted-observer #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577", :accepted-joiner #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"}, :immediate? true}
Replica is:
{:pairs
 {#uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577",
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577"
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"},
 :accepted {},
 :prepared {},
 :peer-state
 {#uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf" :idle,
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577" :idle},
 :peers
 [#uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577"
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"],
 :job-scheduler :onyx.job-scheduler/greedy}
====
Log Entry #4
Entry is {:message-id 4, :fn :prepare-join-cluster, :args {:joiner #uuid "010a1688-47ff-4055-8da5-1f02247351e1"}}
Replica is:
{:pairs
 {#uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577",
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577"
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"},
 :accepted {},
 :prepared
 {#uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577"
  #uuid "010a1688-47ff-4055-8da5-1f02247351e1"},
 :peer-state
 {#uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf" :idle,
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577" :idle},
 :peers
 [#uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577"
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"],
 :job-scheduler :onyx.job-scheduler/greedy}
====
Log Entry #5
Entry is {:message-id 5, :fn :notify-join-cluster, :args {:observer #uuid "010a1688-47ff-4055-8da5-1f02247351e1", :subject #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"}, :immediate? true}
Replica is:
{:pairs
 {#uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577",
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577"
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"},
 :accepted
 {#uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577"
  #uuid "010a1688-47ff-4055-8da5-1f02247351e1"},
 :prepared {},
 :peer-state
 {#uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf" :idle,
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577" :idle},
 :peers
 [#uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577"
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"],
 :job-scheduler :onyx.job-scheduler/greedy}
====
Log Entry #6
Entry is {:message-id 6, :fn :accept-join-cluster, :args {:observer #uuid "010a1688-47ff-4055-8da5-1f02247351e1", :subject #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf", :accepted-observer #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577", :accepted-joiner #uuid "010a1688-47ff-4055-8da5-1f02247351e1"}, :immediate? true}
Replica is:
{:pairs
 {#uuid "010a1688-47ff-4055-8da5-1f02247351e1"
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf",
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577",
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577"
  #uuid "010a1688-47ff-4055-8da5-1f02247351e1"},
 :accepted {},
 :prepared {},
 :peer-state
 {#uuid "010a1688-47ff-4055-8da5-1f02247351e1" :idle,
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf" :idle,
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577" :idle},
 :peers
 [#uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577"
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
  #uuid "010a1688-47ff-4055-8da5-1f02247351e1"],
 :job-scheduler :onyx.job-scheduler/greedy}
====
Log Entry #7
Entry is {:message-id 7, :fn :prepare-join-cluster, :args {:joiner #uuid "e6c35131-f4d9-432d-8915-e8616851bb1c"}}
Replica is:
{:pairs
 {#uuid "010a1688-47ff-4055-8da5-1f02247351e1"
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf",
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577",
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577"
  #uuid "010a1688-47ff-4055-8da5-1f02247351e1"},
 :accepted {},
 :prepared
 {#uuid "010a1688-47ff-4055-8da5-1f02247351e1"
  #uuid "e6c35131-f4d9-432d-8915-e8616851bb1c"},
 :peer-state
 {#uuid "010a1688-47ff-4055-8da5-1f02247351e1" :idle,
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf" :idle,
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577" :idle},
 :peers
 [#uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577"
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
  #uuid "010a1688-47ff-4055-8da5-1f02247351e1"],
 :job-scheduler :onyx.job-scheduler/greedy}
====
Log Entry #8
Entry is {:message-id 8, :fn :notify-join-cluster, :args {:observer #uuid "e6c35131-f4d9-432d-8915-e8616851bb1c", :subject #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"}, :immediate? true}
Replica is:
{:pairs
 {#uuid "010a1688-47ff-4055-8da5-1f02247351e1"
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf",
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577",
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577"
  #uuid "010a1688-47ff-4055-8da5-1f02247351e1"},
 :accepted
 {#uuid "010a1688-47ff-4055-8da5-1f02247351e1"
  #uuid "e6c35131-f4d9-432d-8915-e8616851bb1c"},
 :prepared {},
 :peer-state
 {#uuid "010a1688-47ff-4055-8da5-1f02247351e1" :idle,
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf" :idle,
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577" :idle},
 :peers
 [#uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577"
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
  #uuid "010a1688-47ff-4055-8da5-1f02247351e1"],
 :job-scheduler :onyx.job-scheduler/greedy}
====
Log Entry #9
Entry is {:message-id 9, :fn :accept-join-cluster, :args {:observer #uuid "e6c35131-f4d9-432d-8915-e8616851bb1c", :subject #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf", :accepted-observer #uuid "010a1688-47ff-4055-8da5-1f02247351e1", :accepted-joiner #uuid "e6c35131-f4d9-432d-8915-e8616851bb1c"}, :immediate? true}
Replica is:
{:pairs
 {#uuid "e6c35131-f4d9-432d-8915-e8616851bb1c"
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf",
  #uuid "010a1688-47ff-4055-8da5-1f02247351e1"
  #uuid "e6c35131-f4d9-432d-8915-e8616851bb1c",
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577",
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577"
  #uuid "010a1688-47ff-4055-8da5-1f02247351e1"},
 :accepted {},
 :prepared {},
 :peer-state
 {#uuid "e6c35131-f4d9-432d-8915-e8616851bb1c" :idle,
  #uuid "010a1688-47ff-4055-8da5-1f02247351e1" :idle,
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf" :idle,
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577" :idle},
 :peers
 [#uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577"
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
  #uuid "010a1688-47ff-4055-8da5-1f02247351e1"
  #uuid "e6c35131-f4d9-432d-8915-e8616851bb1c"],
 :job-scheduler :onyx.job-scheduler/greedy}
====
Log Entry #10
Entry is {:message-id 10, :fn :prepare-join-cluster, :args {:joiner #uuid "bf8fd5fc-30fd-424c-af6a-0b32568581a4"}}
Replica is:
{:pairs
 {#uuid "e6c35131-f4d9-432d-8915-e8616851bb1c"
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf",
  #uuid "010a1688-47ff-4055-8da5-1f02247351e1"
  #uuid "e6c35131-f4d9-432d-8915-e8616851bb1c",
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577",
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577"
  #uuid "010a1688-47ff-4055-8da5-1f02247351e1"},
 :accepted {},
 :prepared
 {#uuid "010a1688-47ff-4055-8da5-1f02247351e1"
  #uuid "bf8fd5fc-30fd-424c-af6a-0b32568581a4"},
 :peer-state
 {#uuid "e6c35131-f4d9-432d-8915-e8616851bb1c" :idle,
  #uuid "010a1688-47ff-4055-8da5-1f02247351e1" :idle,
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf" :idle,
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577" :idle},
 :peers
 [#uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577"
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
  #uuid "010a1688-47ff-4055-8da5-1f02247351e1"
  #uuid "e6c35131-f4d9-432d-8915-e8616851bb1c"],
 :job-scheduler :onyx.job-scheduler/greedy}
====
Log Entry #11
Entry is {:message-id 11, :fn :notify-join-cluster, :args {:observer #uuid "bf8fd5fc-30fd-424c-af6a-0b32568581a4", :subject #uuid "e6c35131-f4d9-432d-8915-e8616851bb1c"}, :immediate? true}
Replica is:
{:pairs
 {#uuid "e6c35131-f4d9-432d-8915-e8616851bb1c"
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf",
  #uuid "010a1688-47ff-4055-8da5-1f02247351e1"
  #uuid "e6c35131-f4d9-432d-8915-e8616851bb1c",
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577",
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577"
  #uuid "010a1688-47ff-4055-8da5-1f02247351e1"},
 :accepted
 {#uuid "010a1688-47ff-4055-8da5-1f02247351e1"
  #uuid "bf8fd5fc-30fd-424c-af6a-0b32568581a4"},
 :prepared {},
 :peer-state
 {#uuid "e6c35131-f4d9-432d-8915-e8616851bb1c" :idle,
  #uuid "010a1688-47ff-4055-8da5-1f02247351e1" :idle,
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf" :idle,
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577" :idle},
 :peers
 [#uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577"
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
  #uuid "010a1688-47ff-4055-8da5-1f02247351e1"
  #uuid "e6c35131-f4d9-432d-8915-e8616851bb1c"],
 :job-scheduler :onyx.job-scheduler/greedy}
====
Log Entry #12
Entry is {:message-id 12, :fn :accept-join-cluster, :args {:observer #uuid "bf8fd5fc-30fd-424c-af6a-0b32568581a4", :subject #uuid "e6c35131-f4d9-432d-8915-e8616851bb1c", :accepted-observer #uuid "010a1688-47ff-4055-8da5-1f02247351e1", :accepted-joiner #uuid "bf8fd5fc-30fd-424c-af6a-0b32568581a4"}, :immediate? true}
Replica is:
{:pairs
 {#uuid "bf8fd5fc-30fd-424c-af6a-0b32568581a4"
  #uuid "e6c35131-f4d9-432d-8915-e8616851bb1c",
  #uuid "e6c35131-f4d9-432d-8915-e8616851bb1c"
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf",
  #uuid "010a1688-47ff-4055-8da5-1f02247351e1"
  #uuid "bf8fd5fc-30fd-424c-af6a-0b32568581a4",
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577",
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577"
  #uuid "010a1688-47ff-4055-8da5-1f02247351e1"},
 :accepted {},
 :prepared {},
 :peer-state
 {#uuid "bf8fd5fc-30fd-424c-af6a-0b32568581a4" :idle,
  #uuid "e6c35131-f4d9-432d-8915-e8616851bb1c" :idle,
  #uuid "010a1688-47ff-4055-8da5-1f02247351e1" :idle,
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf" :idle,
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577" :idle},
 :peers
 [#uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577"
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
  #uuid "010a1688-47ff-4055-8da5-1f02247351e1"
  #uuid "e6c35131-f4d9-432d-8915-e8616851bb1c"
  #uuid "bf8fd5fc-30fd-424c-af6a-0b32568581a4"
],
 :job-scheduler :onyx.job-scheduler/greedy}
====
Log Entry #13
Entry is {:message-id 13, :fn :submit-job, :args {:id #uuid "b784ebb4-356f-4e16-8eac-60e051d69ab7", :tasks [#uuid "ce13205e-937b-4af6-9aa9-d5149b31fb2c" #uuid "948f8595-3a0a-4318-b128-91c1d22c0158" #uuid "fb86b977-d668-4c98-abaa-80ee0d29663a"], :task-scheduler :onyx.task-scheduler/round-robin, :saturation Infinity, :task-saturation {#uuid "ce13205e-937b-4af6-9aa9-d5149b31fb2c" Infinity, #uuid "948f8595-3a0a-4318-b128-91c1d22c0158" Infinity, #uuid "fb86b977-d668-4c98-abaa-80ee0d29663a" Infinity}}}
Replica is:
{:job-scheduler :onyx.job-scheduler/greedy,
 :saturation {#uuid "b784ebb4-356f-4e16-8eac-60e051d69ab7" Infinity},
 :peers
 [#uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577"
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
  #uuid "010a1688-47ff-4055-8da5-1f02247351e1"
  #uuid "e6c35131-f4d9-432d-8915-e8616851bb1c"
  #uuid "bf8fd5fc-30fd-424c-af6a-0b32568581a4"],
 :accepted {},
 :jobs [#uuid "b784ebb4-356f-4e16-8eac-60e051d69ab7"],
 :tasks
 {#uuid "b784ebb4-356f-4e16-8eac-60e051d69ab7"
  [#uuid "ce13205e-937b-4af6-9aa9-d5149b31fb2c"
   #uuid "948f8595-3a0a-4318-b128-91c1d22c0158"
   #uuid "fb86b977-d668-4c98-abaa-80ee0d29663a"]},
 :pairs
 {#uuid "bf8fd5fc-30fd-424c-af6a-0b32568581a4"
  #uuid "e6c35131-f4d9-432d-8915-e8616851bb1c",
  #uuid "e6c35131-f4d9-432d-8915-e8616851bb1c"
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf",
  #uuid "010a1688-47ff-4055-8da5-1f02247351e1"
  #uuid "bf8fd5fc-30fd-424c-af6a-0b32568581a4",
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577",
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577"

  #uuid "010a1688-47ff-4055-8da5-1f02247351e1"},
 :allocations {#uuid "b784ebb4-356f-4e16-8eac-60e051d69ab7" {}},
 :prepared {},
 :peer-state
 {#uuid "bf8fd5fc-30fd-424c-af6a-0b32568581a4" :idle,
  #uuid "e6c35131-f4d9-432d-8915-e8616851bb1c" :idle,
  #uuid "010a1688-47ff-4055-8da5-1f02247351e1" :idle,
  #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf" :idle,
  #uuid "b7e5d564-02a3-46d3-863f-c4a2bac7e577" :idle},
 :task-schedulers
 {#uuid "b784ebb4-356f-4e16-8eac-60e051d69ab7"
  :onyx.task-scheduler/round-robin},
 :task-saturation
 {#uuid "b784ebb4-356f-4e16-8eac-60e051d69ab7"
  {#uuid "ce13205e-937b-4af6-9aa9-d5149b31fb2c" Infinity,
   #uuid "948f8595-3a0a-4318-b128-91c1d22c0158" Infinity,
   #uuid "fb86b977-d668-4c98-abaa-80ee0d29663a" Infinity}}}
====
...
```

