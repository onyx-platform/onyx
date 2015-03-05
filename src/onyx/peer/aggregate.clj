(ns ^:no-doc onyx.peer.aggregate
    (:require [clojure.core.async :refer [chan go >! <! >!! close! alts!! timeout]]
              [onyx.peer.task-lifecycle-extensions :as l-ext]
              [onyx.peer.pipeline-extensions :as p-ext]
              [onyx.peer.operation :as operation]
              [onyx.extensions :as extensions]
              [onyx.peer.function :as function]
              [taoensso.timbre :refer [debug fatal]]
              [dire.core :refer [with-post-hook!]]))

(defn inject-pipeline-resource-shim
  [{:keys [onyx.core/queue onyx.core/ingress-queues onyx.core/task-map] :as event}]
  {})

(defn inject-batch-resource-shim
  [event]
  {})

(defn read-batch-shim [{:keys [onyx.core/queue onyx.core/task-map] :as event}]
  {})

(defn write-batch-shim [event]
  {})

(defn close-batch-resources-shim [event]
  {})

(defn close-pipeline-resources-shim [{:keys [onyx.core/queue] :as event}]
  {})

(defmethod l-ext/start-lifecycle? :aggregator
  [_ event]
  {:onyx.core/start-lifecycle? (operation/start-lifecycle? event)})

(defmethod l-ext/inject-lifecycle-resources :aggregator
  [_ event]
  {})

(defmethod l-ext/inject-batch-resources :aggregator
  [_ event]
  {})

(defmethod p-ext/read-batch [:aggregator nil]
  [event]
  {})

(defmethod p-ext/write-batch [:aggregator nil]
  [event]
  {})

(defmethod l-ext/close-batch-resources :aggregator
  [_ event]
  {})

(defmethod l-ext/close-lifecycle-resources :aggregator
  [_ event]
  {})

