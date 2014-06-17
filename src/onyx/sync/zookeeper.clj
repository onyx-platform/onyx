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

(defn task-path [prefix subpath]
  (str root-path "/" prefix "/task/" subpath))

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
        uuid (UUID/randomUUID)
        place (str (peer-path prefix) "/" uuid)]
    (zk/create (:conn sync) place :persistent? true)
    {:node place :uuid uuid}))

(defmethod extensions/create [ZooKeeper :pulse]
  [sync _]
  (let [prefix (:onyx-id sync)
        uuid (UUID/randomUUID)
        place (str (pulse-path prefix) "/" uuid)]
    (zk/create (:conn sync) place :persistent? false)
    {:node place :uuid uuid}))

(defmethod extensions/create [ZooKeeper :payload]
  [sync _]
  (let [prefix (:onyx-id sync)
        uuid (UUID/randomUUID)
        place (str (payload-path prefix) "/" uuid)]
    (zk/create (:conn sync) place :persistent? true)
    {:node place :uuid uuid}))

(defmethod extensions/create [ZooKeeper :ack]
  [sync _]
  (let [prefix (:onyx-id sync)
        uuid (UUID/randomUUID)
        place (str (ack-path prefix) "/" uuid)]
    (zk/create (:conn sync) place :persistent? true)
    {:node place :uuid uuid}))

(defmethod extensions/create [ZooKeeper :exhaust]
  [sync _]
  (let [prefix (:onyx-id sync)
        uuid (UUID/randomUUID)
        place (str (exhaust-path prefix) "/" uuid)]
    (zk/create (:conn sync) place :persistent? true)
    {:node place :uuid uuid}))

(defmethod extensions/create [ZooKeeper :seal]
  [sync _]
  (let [prefix (:onyx-id sync)
        uuid (UUID/randomUUID)
        place (str (seal-path prefix) "/" uuid)]
    (zk/create (:conn sync) place :persistent? true)
    {:node place :uuid uuid}))

(defmethod extensions/create [ZooKeeper :completion]
  [sync _]
  (let [prefix (:onyx-id sync)
        uuid (UUID/randomUUID)
        place (str (completion-path prefix) "/" uuid)]
    (zk/create (:conn sync) place :persistent? true)
    {:node place :uuid uuid}))

(defmethod extensions/create [ZooKeeper :status]
  [sync _]
  (let [prefix (:onyx-id sync)
        uuid (UUID/randomUUID)
        place (str (status-path prefix) "/" uuid)]
    (zk/create (:conn sync) place :persistent? true)
    {:node place :uuid uuid}))

(defmethod extensions/create [ZooKeeper :catalog]
  [sync _]
  (let [prefix (:onyx-id sync)
        uuid (UUID/randomUUID)
        place (str (catalog-path prefix) "/" uuid)]
    (zk/create (:conn sync) place :persistent? true)
    {:node place :uuid uuid}))

(defmethod extensions/create [ZooKeeper :workflow]
  [sync _]
  (let [prefix (:onyx-id sync)
        uuid (UUID/randomUUID)
        place (str (workflow-path prefix) "/" uuid)]
    (zk/create (:conn sync) place :persistent? true)
    {:node place :uuid uuid}))

(defmethod extensions/create [ZooKeeper :shutdown]
  [sync _]
  (let [prefix (:onyx-id sync)
        uuid (UUID/randomUUID)
        place (str (shutdown-path prefix) "/" uuid)]
    (zk/create (:conn sync) place :persistent? true)
    {:node place :uuid uuid}))

(defmethod extensions/create [ZooKeeper :task]
  [sync _]
  (let [prefix (:onyx-id sync)
        uuid (UUID/randomUUID)
        place (str (task-path prefix uuid) "/task-")]
    {:node (zk/create-all (:conn sync) place :persistent? true :sequential? true)
     :uuid uuid}))

(defmethod extensions/create [ZooKeeper :job]
  [sync _ subpath]
  (let [prefix (:onyx-id sync)
        place (str (job-path prefix) "/" subpath)]
    {:node (zk/create (:conn sync) place :persistent? true)}))

(defmethod extensions/create [ZooKeeper :job-log]
  [sync _ content]
  (let [prefix (:onyx-id sync)
        place (str (job-log-path prefix) "/offer-")
        data (serialize-edn content)]
    {:node (zk/create (:conn sync) place :data data :persistent? true :sequential? true)}))

(defmethod extensions/create-node ZooKeeper
  [sync node]
  (zk/create (:conn sync) node :persistent? true))

(defmethod extensions/create-at [ZooKeeper :peer-state]
  [sync _ subpath content]
  (let [prefix (:onyx-id sync)
        place (str (peer-state-path prefix) "/" subpath "/state-")
        data (serialize-edn content)]
    {:node (zk/create-all (:conn sync) place :data data :persistent? true :sequential? true)}))

(defmethod extensions/create-at [ZooKeeper :workflow]
  [sync _ subpath]
  (let [prefix (:onyx-id sync)
        place (str (job-path prefix) "/" subpath "/workflow")]
    {:node (zk/create (:conn sync) place :persistent? true)}))

(defmethod extensions/create-at [ZooKeeper :catalog]
  [sync _ subpath]
  (let [prefix (:onyx-id sync)
        place (str (job-path prefix) "/" subpath "/catalog")]
    {:node (zk/create (:conn sync) place :persistent? true)}))

(defmethod extensions/create-at [ZooKeeper :task]
  [sync _ subpath content]
  (let [prefix (:onyx-id sync)
        data (serialize-edn content)
        place (str (task-path prefix subpath) "/task-")]
    {:node (zk/create-all (:conn sync) place :data data :persistent? true :sequential? true)}))

(defmethod extensions/bucket [ZooKeeper :peer-state]
  [sync _]
  (let [prefix (:onyx-id sync)
        children (or (zk/children (:conn sync) (peer-state-path prefix)) [])]
    (map #(str (peer-state-path prefix) "/" %) children)))

(defmethod extensions/bucket [ZooKeeper :job]
  [sync _]
  (let [prefix (:onyx-id sync)
        children (or (zk/children (:conn sync) (job-path prefix)) [])]
    (map #(str (job-path prefix) "/" %) children)))

(defmethod extensions/bucket [ZooKeeper :job-log]
  [sync _]
  (let [prefix (:onyx-id sync)
        children (or (zk/children (:conn sync) (job-log-path prefix)) [])]
    (map #(str (job-log-path prefix) "/" %) children)))

(defmethod extensions/bucket-at [ZooKeeper :task]
  [sync _ subpath]
  (let [prefix (:onyx-id sync)
        children (or (zk/children (:conn sync) (task-path prefix subpath)) [])]
    (map #(str (task-path prefix subpath) "/" %) children)))

(defmethod extensions/resolve-node [ZooKeeper :peer]
  [sync _ subpath]
  (let [prefix (:onyx-id sync)]
    (str (peer-path prefix) "/" subpath)))

(defmethod extensions/resolve-node [ZooKeeper :peer-state]
  [sync _ subpath & more]
  (let [prefix (:onyx-id sync)]
    (str (peer-state-path prefix) "/" subpath)))

(defmethod extensions/resolve-node [ZooKeeper :job-log]
  [sync _ & more]
  (let [prefix (:onyx-id sync)]
    (job-log-path prefix)))

(defmethod extensions/resolve-node [ZooKeeper :task]
  [sync _ job-node & more]
  (str job-node "/task"))

(defmethod extensions/children ZooKeeper
  [sync node]
  (let [children (or (zk/children (:conn sync) node) [])]
    (map #(str node "/" %) children)))

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

(defmethod extensions/read-place-at [ZooKeeper :task]
  [sync _ & subpaths]
  (let [prefix (:onyx-id sync)
        job-id (first subpaths)
        task-id (second subpaths)]
    (extensions/read-place sync (str (task-path prefix job-id) "/" task-id))))

(defmethod extensions/dereference ZooKeeper
  [sync node]
  (let [prefix (:onyx-id sync)
        children (or (zk/children (:conn sync) node) [])
        sorted-children (util/sort-sequential-nodes children)]
    (when (seq sorted-children)
      (extensions/read-place sync (str node "/" (last sorted-children))))))

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

(defmethod extensions/creation-time ZooKeeper
  [sync node]
  (:ctime (zk/exists (:conn sync) node)))

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

