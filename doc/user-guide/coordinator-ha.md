## Coordinator High Availability

In a production environment, you'll want at least one additional Coordinator standing by to take over in case of primary failure.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Coordinator High Availability](#coordinator-high-availability)
  - [Launching Another Coordinator](#launching-another-coordinator)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Launching Another Coordinator

To launch a secondary Coordinator, configure the options with the same `:onyx/id` as the primary Coordinator. If you're running another Coordinator on the same box, be sure to use a different port than the original. Starting up a secondary Coordinator should *block* the calling through until the seconary becomes the primary. Interally, Onyx is using ZooKeeper's leader election recipe to watch for failures and perform recovery.

Example:

```clojure
(def coord-opts
  {...
   :onyx/id id
   :onyx.coordinator/host "localhost"
   :onyx.coordinator/port 12345})

(def onyx-server (d/start-distributed-coordinator coord-opts))

(def coord-opts-2
  {...
   :onyx/id id
   :onyx.coordinator/host "localhost"
   :onyx.coordinator/port 54321})

;; Blocks!
(def onyx-server-2 (d/start-distributed-coordinator coord-opts-2))
```