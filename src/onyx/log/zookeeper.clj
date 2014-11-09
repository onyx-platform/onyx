(ns onyx.log.zookeeper
  (:require [clojure.core.async :refer [chan >!! <!! close! thread]]
            [clojure.data.fressian :as fressian]
            [com.stuartsierra.component :as component]
            [taoensso.timbre]
            [zookeeper :as zk]
            [onyx.extensions :as extensions])
  (:import [org.apache.curator.test TestingServer]))

(def root-path "/onyx")

(defn prefix-path [prefix]
  (str root-path "/" prefix))

(defn pulse-path [prefix]
  (str (prefix-path prefix) "/pulse"))

(defn log-path [prefix]
  (str (prefix-path prefix) "/log"))

(defrecord ZooKeeper [id config]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting ZooKeeper")
    (let [server (when (:zookeeper/server? config) (TestingServer. (:zookeeper.server/port config)))
          conn (zk/connect (:zookeeper/address config))]
      (zk/create conn root-path :persistent? true)
      (zk/create conn (prefix-path id) :persistent? true)
      (zk/create conn (pulse-path id) :persistent? true)
      (zk/create conn (log-path id) :persistent? true)

      (assoc component :server server :conn conn :prefix id)))

  (stop [component]
    (taoensso.timbre/info "Stopping ZooKeeper")
    (zk/close (:conn component))

    (when (:server component)
      (.stop (:server component)))

    component))

(defn zookeeper [id config]
  (map->ZooKeeper {:id id :config config}))

(defn serialize-fressian [x]
  (.array (fressian/write x)))

(defn deserialize-fressian [x]
  (fressian/read x))

(defmethod extensions/write-log-entry ZooKeeper
  [{:keys [conn opts] :as log} data]
  (let [node (str (log-path (:onyx/id opts)) "/entry-")
        bytes (serialize-fressian data)]
    (zk/create conn node :data bytes :persistent? true :sequential? true)))

(defmethod extensions/read-log-entry ZooKeeper
  [{:keys [conn opts] :as log} position]
  (let [node (str (log-path (:onyx/id opts)) "/entry-" position)]
    (deserialize-fressian (:data (zk/data conn node)))))

(defmethod extensions/register-pulse ZooKeeper
  [{:keys [conn opts] :as log} id]
  (let [node (str (pulse-path (:onyx/id opts)) "/" id)]
    (zk/create conn node :persistent? false)))

(defmethod extensions/on-delete ZooKeeper
  [{:keys [conn opts] :as log} node ch]
  (let [f (fn [event]
            (when (= (:event-type event) :NodeDeleted)
              (>!! event)))]
    (zk/exists conn node :watcher f)))

(defmethod extensions/subscribe-to-log ZooKeeper
  [{:keys [conn opts] :as log} starting-position ch]
  (thread
   (loop [position starting-position]
     (let [path (str log-path (:onyx/id opts) "/entry-" position)]
       (if (zk/exists conn path)
         (>!! ch position)
         (let [read-ch (chan 1)]
           (zk/children conn (log-path (:onyx/id opts)) :watcher #(>!! read-ch true))
           ;; Log entry may have been added inbetween initial check and when we
           ;; added the watch.
           (when (zk/exists conn path)
             (>!! read-ch true))
           (<!! read-ch)
           (close! read-ch)
           (>!! ch position))))
     (recur (inc position)))))

