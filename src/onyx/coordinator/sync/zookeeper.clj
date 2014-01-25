(ns onyx.coordinator.sync.zookeeper
  (:require [com.stuartsierra.component :as component]
            [onyx.coordinator.extensions :as extensions]
            [onyx.util :as u]
            [zookeeper :as zk])
  (:import [java.util UUID]))

(defrecord ZooKeeper [addr]
  component/Lifecycle

  (start [component]
    (prn "Starting ZooKeeper")
    (let [conn (zk/connect addr)]
      (zk/create conn "/onyx" :persistent? true)
      (zk/create conn "/onyx/peers" :persistent? true)
      (zk/create conn "/onyx/pulse" :persistent? true)
      (zk/create conn "/onyx/payloads"  :persistent? true)
      (zk/create conn "/onyx/acks" :persistent? true)
      (zk/create conn "/onyx/completions" :persistent? true)
      (zk/create conn "/onyx/status"  :persistent? true)
      (assoc component :conn conn)))

  (stop [component]
    (prn "Stopping ZooKeeper")
    (zk/close (:conn component))
    component))

(defn zookeeper [addr]
  (map->ZooKeeper {:addr addr}))

(defn serialize-edn [x]
  (.getBytes (pr-str x)))

(defn deserialize-edn [x]
  (read-string (String. x "UTF-8")))

(defn zk-addr []
  (let [config (u/config)]
    (str (:zk/host config) ":" (:zk/port config))))

(defmethod extensions/create [ZooKeeper :peer]
  [sync _]
  (let [place (str "/onyx/peers/" (UUID/randomUUID))]
    (zk/create (:conn sync) place :persistent? true)
    place))

(defmethod extensions/create [ZooKeeper :pulse]
  [sync _]
  (let [place (str "/onyx/pulse/" (UUID/randomUUID))]
    (zk/create (:conn sync) place :ephemeral? true)
    place))

(defmethod extensions/create [ZooKeeper :payload]
  [sync _]
  (let [place (str "/onyx/payloads/" (UUID/randomUUID))]
    (zk/create (:conn sync) place :persistent? true)
    place))

(defmethod extensions/create [ZooKeeper :ack]
  [sync _]
  (let [place (str "/onyx/acks/" (UUID/randomUUID))]
    (zk/create (:conn sync) place :persistent? true)
    place))

(defmethod extensions/create [ZooKeeper :completion]
  [sync _]
  (let [place (str "/onyx/completions/" (UUID/randomUUID))]
    (zk/create (:conn sync) place :persistent? true)
    place))

(defmethod extensions/create [ZooKeeper :status]
  [sync _]
  (let [place (str "/onyx/status/" (UUID/randomUUID))]
    (zk/create (:conn sync) place :persistent? true)
    place))

(defmethod extensions/delete ZooKeeper
  [sync place] (zk/delete (:conn sync) place))

(defmethod extensions/write-place ZooKeeper
  [sync place contents]
  (let [version (:version (zk/exists (:conn sync) place))]
    (zk/set-data (:conn sync) place (serialize-edn contents) version)))

(defmethod extensions/touch-place ZooKeeper
  [sync place]
  (let [contents (zk/data (:conn sync) place)]
    (zk/set-data (:conn sync) place (:data contents)
                 (:version (:stat contents)))))

(defmethod extensions/read-place ZooKeeper
  [sync place]
  (deserialize-edn (:data (zk/data (:conn sync) place))))

(defmethod extensions/on-change ZooKeeper
  [sync place cb]
  (let [f (fn [event]
            (when (= (:event-type event) :NodeDataChanged)
              (prn "Node changed")
              (cb event)))]
    (zk/exists (:conn sync) place :watcher f)))

(defmethod extensions/on-delete ZooKeeper
  [sync place cb]
  (let [f (fn [event]
            (when (= (:event-type event) :NodeDeleted)
              (prn "Node deleted")
              (cb event)))]
    (zk/exists (:conn sync) place :watcher f)))

