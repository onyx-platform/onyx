(ns ^:no-doc onyx.peer.aggregate
    (:require [clojure.core.async :refer [chan go >! <! >!! close! alts!! timeout]]
              [onyx.peer.task-lifecycle-extensions :as l-ext]
              [onyx.peer.pipeline-extensions :as p-ext]
              [onyx.peer.operation :as operation]
              [onyx.extensions :as extensions]
              [onyx.queue.hornetq :refer [take-segments]]
              [onyx.peer.function :as function]
              [taoensso.timbre :refer [debug fatal]]
              [dire.core :refer [with-post-hook!]]))

(defn reader-thread [event queue reader-ch input consumer]
  (future
    (try
      (loop []
        (let [segment (extensions/consume-message queue consumer)]
          (when segment
            (>!! reader-ch {:input input :message segment})
            (recur))))
      (finally
       (close! reader-ch)))))

(defn consumer-loop [event session input consumer halting-ch session-ch]
  (let [task (:onyx.core/task-map event)
        capacity (:onyx/batch-size task)
        timeout-ms (or (:onyx/batch-timeout task) 1000)]
    (go
     (loop []
       (let [reader-ch (chan capacity)
             fut (reader-thread event (:onyx.core/queue event) reader-ch input consumer)]
         (let [read-f #(first (alts!! (vector reader-ch (timeout timeout-ms))))
               msgs (doall (take-segments read-f capacity))]
           (future-cancel fut)
           (>! session-ch {:session session :halting-ch halting-ch :msgs msgs}))
         (when (<! halting-ch)
           (recur)))))))

(defn inject-pipeline-resource-shim
  [{:keys [onyx.core/queue onyx.core/ingress-queues onyx.core/task-map] :as event}]
  (let [consumers (map (fn [[task queue-name]]
                         (let [session (extensions/bind-active-session queue queue-name)]
                           {:input task
                            :session session
                            :consumer (extensions/create-consumer queue session queue-name)}))
                       ingress-queues)
        halting-ch (chan 0)
        read-ch (chan 1)
        rets
        {:onyx.aggregate/queue
         {:consumers consumers
          :halting-ch halting-ch}
         :onyx.aggregate/read-ch read-ch
         :onyx.core/reserve? true
         :onyx.function/fn (operation/resolve-fn task-map)}]
    (doseq [c consumers]
      (consumer-loop event (:session c) (:input c) (:consumer c) halting-ch read-ch))
    (merge event rets)))

(defn inject-temporal-resource-shim
  [event]
  ;;; To make HornetQ clustered grouping work for Onyx's semantics,
  ;;; only one session can be alive. Open a fake and redefine it later
  ;;; in the pipeline.
  {:onyx.core/session :placeholder})

(defn read-batch-shim [{:keys [onyx.core/queue onyx.core/task-map] :as event}]
  (let [ms (or (:onyx/batch-timeout task-map) 1000)
        v (first (alts!! [(:onyx.aggregate/read-ch event) (timeout ms)]))
        ;; Grab any session if there is none. Nothing will commit.
        default-session (:session (first (:consumers (:onyx.aggregate/queue event))))]
    (merge event
           {:onyx.core/session (or (:session v) default-session)
            :onyx.core/batch (or (:msgs v) [])
            :onyx.aggregate/halting-ch (or (:halting-ch v) (chan 1))})))

(defn write-batch-shim [event]
  (let [results (function/write-batch-shim event)]
    (doseq [msg (:onyx.core/batch results)]
      (extensions/ack-message (:onyx.core/queue event) (:message msg)))
    results))

(defn close-temporal-resources-shim [event]
  (>!! (:onyx.aggregate/halting-ch event) true)
  event)

(defn close-pipeline-resources-shim [{:keys [onyx.core/queue] :as event}]
  (close! (:onyx.aggregate/read-ch event))
  (close! (:halting-ch (:onyx.aggregate/queue event)))

  (doseq [c (:consumers (:onyx.aggregate/queue event))]
    (extensions/close-resource queue (:consumer c))
    (extensions/close-resource queue (:session c)))
  event)

(defmethod l-ext/start-lifecycle? :aggregator
  [_ event]
  {:onyx.core/start-lifecycle? (operation/start-lifecycle? event)})

(defmethod l-ext/inject-lifecycle-resources :aggregator
  [_ event]
  (inject-pipeline-resource-shim event))

(defmethod l-ext/inject-temporal-resources :aggregator
  [_ event]
  (inject-temporal-resource-shim event))

(defmethod p-ext/read-batch [:aggregator nil]
  [event]
  (read-batch-shim event))

(defmethod p-ext/write-batch [:aggregator nil]
  [event] (write-batch-shim event))

(defmethod l-ext/close-temporal-resources :aggregator
  [_ event]
  (close-temporal-resources-shim event)
  {})

(defmethod l-ext/close-lifecycle-resources :aggregator
  [_ event]
  (close-pipeline-resources-shim event)
  {})

