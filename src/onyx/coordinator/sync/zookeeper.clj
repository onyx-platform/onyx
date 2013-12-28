(ns onyx.coordinator.sync.zookeeper
  (:require [zookeeper :as zk]
            [onyx.coordinator.extensions :as extensions])
  (:import [java.util UUID]))

(defn zk-addr []
  (let [resource (clojure.java.io/resource "config.edn")
        config (read-string (slurp resource))]
    (str (:zk/host config) ":" (:zk/port config))))

(def client (zk/connect (zk-addr)))

(defmethod extensions/create [:zookeeper :peer]
  [_ _]
  (let [path (str "/onyx/peers/" (UUID/randomUUID))]
    (zk/create client path :ephemeral? true)
    path))

(defmethod extensions/create [:zookeeper :payload]
  [_ _]
  (let [path (str "/onyx/payloads/" (UUID/randomUUID))]
    (zk/create client path :persistent? true)
    path))

(defmethod extensions/create [:zookeeper :ack]
  [_ _]
  (let [path (str "/onyx/acks/" (UUID/randomUUID))]
    (zk/create client path :persistent? true)
    path))

(defmethod extensions/create [:zookeeper :completion]
  [_ _]
  (let [path (str "/onyx/completions/" (UUID/randomUUID))]
    (zk/create client path :persistent? true)
    path))

(defmethod extensions/create [:zookeeper :status]
  [_ _]
  (let [path (str "/onyx/status/" (UUID/randomUUID))]
    (zk/create client path :persistent? true)
    path))

(defmethod extensions/delete (fn [sync task] sync))

(defmethod extensions/write (fn [sync] sync))

(defmethod extensions/read (fn [sync node] sync))

(defmethod extensions/on-change (fn [sync node cb] sync))

