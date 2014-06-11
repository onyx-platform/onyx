(ns ^:no-doc onyx.sync.zookeeper
  (:require [clojure.data.fressian :as fressian]
            [com.stuartsierra.component :as component]
            [taoensso.timbre]
            [onyx.extensions :as extensions]
            [zookeeper :as zk]
            [zookeeper.util :as util])
  (:import [java.util UUID]))

(def root-path "/onyx")

(defn prefix-path [prefix]
  (str root-path "/" prefix))

(defn peer-path [prefix]
  (str root-path "/" prefix "/peer"))

(defn peer-state-path [prefix]
  (str root-path "/" prefix "/peer-state"))

(defn pulse-path [prefix]
  (str root-path "/" prefix "/pulse"))

(defn payload-path [prefix]
  (str root-path "/" prefix "/payload"))

(defn ack-path [prefix]
  (str root-path "/" prefix "/ack"))

(defn exhaust-path [prefix]
  (str root-path "/" prefix "/exhaust"))

(defn seal-path [prefix]
  (str root-path "/" prefix "/seal"))

(defn completion-path [prefix]
  (str root-path "/" prefix "/completion"))

(defn status-path [prefix]
  (str root-path "/" prefix "/status"))

(defn catalog-path [prefix]
  (str root-path "/" prefix "/catalog"))

(defn workflow-path [prefix]
  (str root-path "/" prefix "/workflow"))

(defn shutdown-path [prefix]
  (str root-path "/" prefix "/shutdown"))

(defn job-path [prefix]
  (str root-path "/" prefix "/job"))

(defn job-log-path [prefix]
  (str root-path "/" prefix "/job-log"))

(defn task-path [prefix job-id]
  (str root-path "/" prefix "/job/" job-id))

(defrecord ZooKeeper [addr onyx-id]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting ZooKeeper")
    (let [conn (zk/connect addr)
          prefix onyx-id]
      (zk/create conn root-path :persistent? true)
      (zk/create conn (prefix-path prefix) :persistent? true)
      (zk/create conn (peer-path prefix) :persistent? true)
      (zk/create conn (peer-state-path prefix) :persistent? true)
      (zk/create conn (pulse-path prefix) :persistent? true)
      (zk/create conn (payload-path prefix)  :persistent? true)
      (zk/create conn (ack-path prefix) :persistent? true)
      (zk/create conn (exhaust-path prefix) :persistent? true)
      (zk/create conn (seal-path prefix) :persistent? true)
      (zk/create conn (completion-path prefix) :persistent? true)
      (zk/create conn (status-path prefix) :persistent? true)
      (zk/create conn (catalog-path prefix) :persistent? true)
      (zk/create conn (workflow-path prefix) :persistent? true)
      (zk/create conn (shutdown-path prefix) :persistent? true)
      (zk/create conn (job-path prefix) :persistent? true)
      (zk/create conn (job-log-path prefix) :persistent? true)

      (assoc component :conn conn :prefix onyx-id)))

  (stop [component]
    (taoensso.timbre/info "Stopping ZooKeeper")
    (zk/close (:conn component))
    component))

(defn zookeeper [addr onyx-id]
  (map->ZooKeeper {:addr addr :onyx-id onyx-id}))

(defn serialize-edn [x]
  (.array (fressian/write x)))

(defn deserialize-edn [x]
  (fressian/read x))

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
    (zk/create (:conn sync) place :persistent? false)
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

(defmethod extensions/create [ZooKeeper :exhaust]
  [sync _]
  (let [prefix (:onyx-id sync)
        place (str (exhaust-path prefix) "/" (UUID/randomUUID))]
    (zk/create (:conn sync) place :persistent? true)
    place))

(defmethod extensions/create [ZooKeeper :seal]
  [sync _]
  (let [prefix (:onyx-id sync)
        place (str (seal-path prefix) "/" (UUID/randomUUID))]
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

(defmethod extensions/create [ZooKeeper :catalog]
  [sync _]
  (let [prefix (:onyx-id sync)
        place (str (catalog-path prefix) "/" (UUID/randomUUID))]
    (zk/create (:conn sync) place :persistent? true)
    place))

(defmethod extensions/create [ZooKeeper :workflow]
  [sync _]
  (let [prefix (:onyx-id sync)
        place (str (workflow-path prefix) "/" (UUID/randomUUID))]
    (zk/create (:conn sync) place :persistent? true)
    place))

(defmethod extensions/create [ZooKeeper :shutdown]
  [sync _]
  (let [prefix (:onyx-id sync)
        place (str (shutdown-path prefix) "/" (UUID/randomUUID))]
    (zk/create (:conn sync) place :persistent? true)
    place))

(defmethod extensions/create [ZooKeeper :job]
  [sync _]
  (let [job-id (UUID/randomUUID)
        prefix (:onyx-id sync)
        place (str (job-path prefix) "/" job-id)]
    (zk/create (:conn sync) place :persistent? true)
    job-id))

(defmethod extensions/create [ZooKeeper :job-log]
  [sync _]
  (let [prefix (:onyx-id sync)
        place (str (job-log-path prefix) "/offer-")]
    (zk/create (:conn sync) place :persistent? true :sequential? true)
    place))

(defmethod extensions/create-node ZooKeeper
  [sync node]
  (zk/create :conn sync) node :persistent? true)

(defmethod extensions/create-at [ZooKeeper :peer-state]
  [sync _ subpath]
  (let [prefix (:onyx-id sync)
        place (str (peer-state-path prefix) "/" subpath "/state-")]
    (zk/create (:conn sync) place :persistent? true :sequential? true)
    place))

(defmethod extensions/create-at [ZooKeeper :workflow]
  [sync _ subpath]
  (let [prefix (:onyx-id sync)
        place (str (job-path prefix) "/" subpath "/workflow")]
    (zk/create (:conn sync) place :persistent? true)
    place))

(defmethod extensions/create-at [ZooKeeper :catalog]
  [sync _ subpath]
  (let [prefix (:onyx-id sync)
        place (str (job-path prefix) "/" subpath "/catalog")]
    (zk/create (:conn sync) place :persistent? true)
    place))

(defmethod extensions/create-at [ZooKeeper :task]
  [sync _ subpath]
  (let [prefix (:onyx-id sync)
        place (str (task-path prefix subpath) "-")]
    (zk/create (:conn sync) place :persistent? true :sequential? true)
    place))

(defmethod extensions/bucket [ZooKeeper :peer-state]
  [sync _]
  (let [prefix (:onyx-id sync)]
    (zk/children (:conn sync) (peer-state-path prefix))))

(defmethod extensions/bucket [ZooKeeper :job]
  [sync _]
  (let [prefix (:onyx-id sync)]
    (zk/children (:conn sync) (job-path prefix))))

(defmethod extensions/bucket-at [ZooKeeper :task]
  [sync _ subpath]
  (let [prefix (:onyx-id sync)]
    (zk/children (:conn sync) (task-path prefix subpath))))

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

(defmethod extensions/deref-place-at [ZooKeeper :peer-state]
  [sync _ subpath]
  (let [prefix (:onyx-id sync)
        place (str (peer-state-path prefix) "/" subpath)
        children (zk/children (:conn sync) place)
        sorted-children (util/sort-sequential-nodes children)]
    (extensions/read-place sync (last sorted-children))))

(defmethod extensions/deref-place-at [ZooKeeper :job-log]
  [sync _ _]
  (let [prefix (:onyx-id sync)
        place (str (job-log-path prefix))
        children (zk/children (:conn sync) place)
        sorted-children (util/sort-sequential-nodes children)]
    (extensions/read-place sync (last sorted-children))))

(defmethod extensions/place-exists? ZooKeeper
  [sync place]
  (boolean (zk/exists (:conn sync) place)))

(defmethod extensions/place-exists-at? [ZooKeeper :task]
  [sync _ & subpaths]
  (let [prefix (:onyx-id sync)
        job-id (first subpaths)
        task-id (second subpaths)]
    (extensions/place-exists? sync (task-path prefix job-id task-id))))

(defmethod extensions/version ZooKeeper
  [sync place]
  (:version (zk/exists (:conn sync) place)))

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

