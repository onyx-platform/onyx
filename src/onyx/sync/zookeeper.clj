(ns onyx.coordinator.sync.zookeeper
  (:require [com.stuartsierra.component :as component]
            [onyx.extensions :as extensions]
            [zookeeper :as zk])
  (:import [java.util UUID]))

(def root-path "/onyx")

(defn prefix-path [prefix]
  (str root-path "/" prefix))

(defn peer-path [prefix]
  (str root-path "/" prefix "/peer"))

(defn pulse-path [prefix]
  (str root-path "/" prefix "/pulse"))

(defn payload-path [prefix]
  (str root-path "/" prefix "/payload"))

(defn ack-path [prefix]
  (str root-path "/" prefix "/ack"))

(defn completion-path [prefix]
  (str root-path "/" prefix "/completion"))

(defn status-path [prefix]
  (str root-path "/" prefix "/status"))

(defn shutdown-path [prefix]
  (str root-path "/" prefix "/shutdown"))

(defrecord ZooKeeper [addr onyx-id]
  component/Lifecycle

  (start [component]
    (prn "Starting ZooKeeper")
    (let [conn (zk/connect addr)
          prefix onyx-id]
      (zk/create conn root-path :persistent? true)
      (zk/create conn (prefix-path prefix) :persistent? true)
      (zk/create conn (peer-path prefix) :persistent? true)
      (zk/create conn (pulse-path prefix) :persistent? true)
      (zk/create conn (payload-path prefix)  :persistent? true)
      (zk/create conn (ack-path prefix) :persistent? true)
      (zk/create conn (completion-path prefix) :persistent? true)
      (zk/create conn (status-path prefix) :persistent? true)
      (zk/create conn (shutdown-path prefix) :persistent? true)
      (assoc component
        :conn conn
        :prefix onyx-id)))

  (stop [component]
    (prn "Stopping ZooKeeper")
    (zk/close (:conn component))
    component))

(defn zookeeper [addr onyx-id]
  (map->ZooKeeper {:addr addr :onyx-id onyx-id}))

(defn serialize-edn [x]
  (.getBytes (pr-str x)))

(defn deserialize-edn [x]
  (read-string (String. x "UTF-8")))

(defmethod extensions/create [ZooKeeper :peer]
  [sync _]
  (let [prefix (:onyx-id sync)
        place (str (peer-path prefix) "/" (UUID/randomUUID))]
    (zk/create (:conn sync) place :persistent? true)
    place))

(defmethod extensions/create [ZooKeeper :pulse]
  [sync _]
  (let [prefix (:onyx-id sync)
        place (str (pulse-path prefix) "/" (UUID/randomUUID))]
    (zk/create (:conn sync) place :ephemeral? true)
    place))

(defmethod extensions/create [ZooKeeper :payload]
  [sync _]
  (let [prefix (:onyx-id sync)
        place (str (payload-path prefix) "/" (UUID/randomUUID))]
    (zk/create (:conn sync) place :persistent? true)
    place))

(defmethod extensions/create [ZooKeeper :ack]
  [sync _]
  (let [prefix (:onyx-id sync)
        place (str (ack-path prefix) "/" (UUID/randomUUID))]
    (zk/create (:conn sync) place :persistent? true)
    place))

(defmethod extensions/create [ZooKeeper :completion]
  [sync _]
  (let [prefix (:onyx-id sync)
        place (str (completion-path prefix) "/" (UUID/randomUUID))]
    (zk/create (:conn sync) place :persistent? true)
    place))

(defmethod extensions/create [ZooKeeper :status]
  [sync _]
  (let [prefix (:onyx-id sync)
        place (str (status-path prefix) "/" (UUID/randomUUID))]
    (zk/create (:conn sync) place :persistent? true)
    place))

(defmethod extensions/create [ZooKeeper :shutdown]
  [sync _]
  (let [prefix (:onyx-id sync)
        place (str (shutdown-path prefix) "/" (UUID/randomUUID))]
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

(defmethod extensions/place-exists? ZooKeeper
  [sync place]
  (boolean (zk/exists (:conn sync) place)))

(defmethod extensions/on-change ZooKeeper
  [sync place cb]
  (let [f (fn [event]
            (when (= (:event-type event) :NodeDataChanged)
              (cb event)))]
    (zk/exists (:conn sync) place :watcher f)))

(defmethod extensions/on-delete ZooKeeper
  [sync place cb]
  (let [f (fn [event]
            (when (= (:event-type event) :NodeDeleted)
              (cb event)))]
    (zk/exists (:conn sync) place :watcher f)))

