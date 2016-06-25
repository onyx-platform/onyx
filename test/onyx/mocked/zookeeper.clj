(ns onyx.mocked.zookeeper
  (:require [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info error warn fatal]]
            [onyx.log.replica :refer [base-replica]]
            [onyx.extensions :as extensions]))

(defrecord FakeZooKeeper [config]
  component/Lifecycle
  (start [component] component)
  (stop [component] component))

(defn fake-zookeeper [entries config]
  (map->FakeZooKeeper {:entries entries
                       :entry-num 0
                       :config config}))

(defmethod extensions/write-log-entry FakeZooKeeper
  [log data]
  (swap! (:entries log) (fn [entries] 
                          (conj (vec entries)
                                (assoc data :message-id (count entries)))))
  log)

(defmethod extensions/read-log-entry FakeZooKeeper
  [{:keys [entries]} n]
  (get @entries n))

(defmethod extensions/register-pulse FakeZooKeeper
  [& all])

(defmethod extensions/on-delete FakeZooKeeper
  [& all])

(defmethod extensions/group-exists? FakeZooKeeper
  [& all]
  ;; Always show true - we will always manually leave
  true)

(defmethod extensions/subscribe-to-log FakeZooKeeper
  [& all]
  base-replica)

(defmethod extensions/write-chunk :default
  [& all]
  (println "WRITE ALL " all))

(defmethod extensions/read-chunk :default
  [& all]
  (println "READ ALL " all)
  )
