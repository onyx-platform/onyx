(ns ^:no-doc onyx.peer.function
  (:require [clojure.core.async :refer [chan >! go alts!! close! timeout]]
            [onyx.static.planning :refer [find-task]]
            [onyx.messaging.acking-daemon :as acker]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.peer.operation :as operation]
            [onyx.extensions :as extensions]
            [clj-tuple :as t]
            [onyx.log.commands.peer-replica-view :refer [peer-site]]
            [taoensso.timbre :as timbre :refer [debug info]])
  (:import [java.util UUID]))

(defn read-batch
  ([event]
   (read-batch event (:onyx.core/messenger event)))
  ([event messenger]
   {:onyx.core/batch (onyx.extensions/receive-messages messenger event)}))

(defn write-batch
  ([{:keys [onyx.core/results onyx.core/messenger onyx.core/state
            onyx.core/replica onyx.core/peer-replica-view onyx.core/serialized-task] :as event}]
   (write-batch event replica peer-replica-view state messenger (:egress-ids serialized-task)))
  ([event replica peer-replica-view state messenger egress-tasks]
   (let [segments (:segments (:onyx.core/results event))]
     (when-not (empty? segments)
       (let [replica-val @replica
             pick-peer-fns (:pick-peer-fns @peer-replica-view)
             grouped (group-by #(t/vector (:route %) (:hash-group %)) segments)]
         (run! (fn [[[route hash-group] segs]]
                 (when-let [pick-peer-fn (get pick-peer-fns (get egress-tasks route))]
                   (when-let [target (pick-peer-fn hash-group)]
                     (when-let [site (peer-site peer-replica-view target)]
                       (onyx.extensions/send-messages messenger site segs)))))
               grouped))))
   {}))

(defrecord Function [replica peer-replica-view state messenger egress-tasks]

  p-ext/PipelineInput
  p-ext/Pipeline
  
  (read-batch
    [_ event]
    (read-batch event messenger))

  (write-batch
    [_ event]
    (write-batch event replica peer-replica-view state messenger egress-tasks))

  (seal-resource [_ _]
    nil))

(defn function [{:keys [onyx.core/replica
                        onyx.core/peer-replica-view
                        onyx.core/state
                        onyx.core/messenger
                        onyx.core/serialized-task] :as pipeline-data}]
  (->Function replica
              peer-replica-view
              state
              messenger
              (:egress-ids serialized-task)))
