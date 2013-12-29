(ns onyx.coordinator.sync.zookeeper
  (:require [onyx.coordinator.extensions :as extensions]
            [onyx.util :as u]
            [zookeeper :as zk])
  (:import [java.util UUID]))

(defn serialize-edn [x]
  (.getBytes (pr-str x)))

(defn deserialize-edn [x]
  (read-string (String. x "UTF-8")))

(defn zk-addr []
  (let [config (u/config)]
    (str (:zk/host config) ":" (:zk/port config))))

(def client (delay (let [c (zk/connect (zk-addr))]
                     (zk/create c "/onyx" :persistent? true)
                     (zk/create c "/onyx/peers" :persistent? true)
                     (zk/create c "/onyx/payloads"  :persistent? true)
                     (zk/create c "/onyx/acks" :persistent? true)
                     (zk/create c "/onyx/completions" :persistent? true)
                     (zk/create c "/onyx/status"  :persistent? true)
                     c)))

(defmethod extensions/create [:zookeeper :peer]
  [_ _]
  (let [place (str "/onyx/peers/" (UUID/randomUUID))]
    (zk/create @client place :ephemeral? true)
    place))

(defmethod extensions/create [:zookeeper :payload]
  [_ _]
  (let [place (str "/onyx/payloads/" (UUID/randomUUID))]
    (zk/create @client place :persistent? true)
    place))

(defmethod extensions/create [:zookeeper :ack]
  [_ _]
  (let [place (str "/onyx/acks/" (UUID/randomUUID))]
    (zk/create @client place :persistent? true)
    place))

(defmethod extensions/create [:zookeeper :completion]
  [_ _]
  (let [place (str "/onyx/completions/" (UUID/randomUUID))]
    (zk/create @client place :persistent? true)
    place))

(defmethod extensions/create [:zookeeper :status]
  [_ _]
  (let [place (str "/onyx/status/" (UUID/randomUUID))]
    (zk/create @client place :persistent? true)
    place))

(defmethod extensions/delete :zookeeper
  [_ place] (zk/delete @client place))

(defmethod extensions/write-place :zookeeper
  [_ place contents]
  (let [version (:version (zk/exists @client place))]
    (zk/set-data @client place (serialize-edn contents) version)))

(defmethod extensions/read-place :zookeeper
  [_ place] (deserialize-edn (zk/data @client place)))

(defmethod extensions/on-change :zookeeper
  [_ place cb] (zk/exists @client place :watcher cb))

