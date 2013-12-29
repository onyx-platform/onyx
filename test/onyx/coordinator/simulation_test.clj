(ns onyx.coordinator.simulation-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [>!!]]
            [onyx.coordinator.async :as async]
            [onyx.coordinator.extensions :as extensions]
            [onyx.coordinator.sync.zookeeper :as ozk]
            [zookeeper :as zk]))

(def schema
  [{:onyx/name :stub-in
    :onyx/type :queue
    :onyx/medium :hornetq
    :hornetq/queue-name "stub-in"}
   {:onyx/name :stub-fn
    :onyx/type :transformer
    :onyx/fn :my.ns/stub-fn
    :onyx/consumation :sequential}
   {:onyx-name :stub-out
    :onyx/type :queue
    :onyx/medium :hornetq
    :hornetq/queue-name "stub-out"}])

(def workflow {:stub-in {:stub-fn :stub-out}})

(def peer (extensions/create :zookeeper :peer))

(>!! async/born-peer-ch peer)

