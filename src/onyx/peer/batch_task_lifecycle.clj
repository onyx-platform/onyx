(ns ^:no-doc onyx.peer.batch-task-lifecycle
    (:require [clojure.core.async :refer [alts!! <!! >!! close!]]
              [com.stuartsierra.component :as component]
              [onyx.extensions :as extensions]
              [onyx.log.entry :as entry]
              [onyx.peer.task-lifecycle :as t]
              [onyx.peer.pipeline-extensions :as p-ext]
              [onyx.static.planning :refer [find-task]]
              [onyx.static.default-vals :refer [arg-or-default]]
              [onyx.checkpoint-storage.local-fs :refer [local-fs-storage]]))

(defn run-task-lifecycle [event]
  (cond (= :input (:onyx/type (:onyx.core/task-map event)))
        (let [xs (p-ext/read-partition (:onyx.core/pipeline event) (:onyx.core/partition event))
              applied-partition (map (:onyx.core/fn event) xs)
              checkpoint-file (extensions/write-content (:onyx.core/checkpoint-storage event) applied-partition)]
          (>!! (:onyx.core/outbox-ch event)
               (entry/create-log-entry :complete-partition {:job (:onyx.core/job-id event)
                                                            :task (:onyx.core/task-id event)
                                                            :partition (:onyx.core/partition event)
                                                            :location checkpoint-file})))

        (= :function (:onyx/type (:onyx.core/task-map event)))
        (let [replica @(:onyx.core/replica event)
              location (get-in replica [:completed-partitions (:onyx.core/job-id event)
                                        (get-in replica [:tasks (:onyx.core/job-id event) 0])
                                        (:onyx.core/partition event)])
              xs (extensions/read-content (:onyx.core/checkpoint-storage event) location)
              results (map (:onyx.core/fn event) xs)
              checkpoint-file (extensions/write-content (:onyx.core/checkpoint-storage event) results)]
          (>!! (:onyx.core/outbox-ch event)
               (entry/create-log-entry :complete-partition {:job (:onyx.core/job-id event)
                                                            :task (:onyx.core/task-id event)
                                                            :partition (:onyx.core/partition event)
                                                            :location checkpoint-file})))
        (= :output (:onyx/type (:onyx.core/task-map event)))
        (let [replica @(:onyx.core/replica event)
              location (get-in replica [:completed-partitions (:onyx.core/job-id event)
                                        (get-in replica [:tasks (:onyx.core/job-id event) 1])
                                        (:onyx.core/partition event)])
              xs (extensions/read-content (:onyx.core/checkpoint-storage event) location)
              results (map (:onyx.core/fn event) xs)]
          (p-ext/write-partition (:onyx.core/pipeline event) (:onyx.core/partition event) results)
          (>!! (:onyx.core/outbox-ch event)
               (entry/create-log-entry :complete-partition {:job (:onyx.core/job-id event)
                                                            :task (:onyx.core/task-id event)
                                                            :partition (:onyx.core/partition event)})))))
(defrecord BatchTaskLifeCycle
    [id log messenger-buffer messenger job-id task-id replica peer-replica-view restart-ch
     kill-ch outbox-ch seal-resp-ch completion-ch opts task-kill-ch monitoring partition]
  component/Lifecycle

  (start [component]
    (try
      (let [catalog (extensions/read-chunk log :catalog job-id)
            task (extensions/read-chunk log :task task-id)
            flow-conditions (extensions/read-chunk log :flow-conditions job-id)
            lifecycles (extensions/read-chunk log :lifecycles job-id)
            catalog-entry (find-task catalog (:name task))

            _ (taoensso.timbre/info (format "[%s] Warming up Batch Task LifeCycle for job %s, task %s, partition %s"
                                            id job-id (:name task) partition))

            pipeline-data {:onyx.core/id id
                           :onyx.core/job-id job-id
                           :onyx.core/task-id task-id
                           :onyx.core/task (:name task)
                           :onyx.core/partition partition
                           :onyx.core/catalog catalog
                           :onyx.core/workflow (extensions/read-chunk log :workflow job-id)
                           :onyx.core/flow-conditions flow-conditions
                           :onyx.core/task-map catalog-entry
                           :onyx.core/serialized-task task
                           :onyx.core/params (t/resolve-calling-params catalog-entry opts)
                           :onyx.core/drained-back-off (or (:onyx.peer/drained-back-off opts) 400)
                           :onyx.core/log log
                           :onyx.core/messenger-buffer messenger-buffer
                           :onyx.core/messenger messenger
                           :onyx.core/checkpoint-storage (local-fs-storage "target")
                           :onyx.core/monitoring monitoring
                           :onyx.core/outbox-ch outbox-ch
                           :onyx.core/seal-ch seal-resp-ch
                           :onyx.core/peer-opts opts
                           :onyx.core/max-downstream-links (arg-or-default :onyx.messaging/max-downstream-links opts)
                           :onyx.core/max-acker-links (arg-or-default :onyx.messaging/max-acker-links opts)
                           :onyx.core/fn (t/resolve-task-fn catalog-entry)
                           :onyx.core/replica replica
                           :onyx.core/peer-replica-view peer-replica-view
                           :onyx.core/state (atom {})}

            pipeline (t/build-pipeline catalog-entry pipeline-data)
            pipeline-data (assoc pipeline-data :onyx.core/pipeline pipeline)

            ex-f (fn [e] (t/handle-exception (constantly false) e restart-ch outbox-ch job-id))]

;;;;; TODO: << FIX ME >>
;;;;; Huge hack to force the single active peer to partition
;;;;; the data set and do nothing else, unless the data set
;;;;; has already been partitioned.
        (if (and (= :input (:onyx/type catalog-entry))
                 (nil? partition))
          (let [partitions (p-ext/n-partitions pipeline)]
            (>!! outbox-ch (entry/create-log-entry :broadcast-input-partitions {:job job-id :task task-id :n-partitions partitions}))
            component)
          (do (run-task-lifecycle pipeline-data)
              (assoc component 
                :pipeline pipeline
                :pipeline-data pipeline-data
                :seal-ch seal-resp-ch
                :task-kill-ch task-kill-ch))))
      (catch Throwable e
        (t/handle-exception (constantly false) e restart-ch outbox-ch job-id)
        component)))

  (stop [component]
    (taoensso.timbre/info (format "[%s] Stopping Task LifeCycle for %s" id (:onyx.core/task (:pipeline-data component))))
    (when-let [event (:pipeline-data component)]
      (close! (:task-kill-ch component)))

    (assoc component :pipeline nil :pipeline-data nil)))

(defn batch-task-lifecycle [args {:keys [id log messenger-buffer messenger job task replica peer-replica-view
                                         restart-ch kill-ch outbox-ch seal-ch completion-ch opts task-kill-ch
                                         monitoring partition]}]
  (map->BatchTaskLifeCycle {:id id :log log :messenger-buffer messenger-buffer
                            :messenger messenger :job-id job :task-id task :restart-ch restart-ch
                            :peer-replica-view peer-replica-view
                            :kill-ch kill-ch :outbox-ch outbox-ch
                            :replica replica :seal-resp-ch seal-ch :completion-ch completion-ch
                            :opts opts :task-kill-ch task-kill-ch :monitoring monitoring
                            :partition partition}))
