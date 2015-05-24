(ns onyx.log.curator
  (:require [clojure.core.async :refer [chan >!! <!! close! thread]]
            [taoensso.timbre :refer [fatal warn trace]]
            [onyx.static.default-vals :refer [defaults]]
            [zookeeper :as zk]
            [zookeeper.internal :as zi])
  (:import [org.apache.zookeeper CreateMode]
           [org.apache.zookeeper KeeperException$NoNodeException KeeperException$NodeExistsException]
           [org.apache.curator.test TestingServer]
           [org.apache.zookeeper.data Stat]
           [org.apache.curator.framework CuratorFrameworkFactory CuratorFramework]
           [org.apache.curator.framework.api CuratorWatcher PathAndBytesable Versionable]
           [org.apache.curator.framework.state ConnectionStateListener ConnectionState]
           [org.apache.curator.framework.imps CuratorFrameworkState]
           [org.apache.curator RetryPolicy]
           [org.apache.curator.retry BoundedExponentialBackoffRetry]))

(defn ^CuratorFramework connect
  ([connection-string]
   (connect connection-string ""))
  ([connection-string ns]
   (connect connection-string ns 
            (BoundedExponentialBackoffRetry. 
              (:onyx.zookeeper/backoff-base-sleep-time-ms defaults) 
              (:onyx.zookeeper/backoff-max-sleep-time-ms defaults)
              (:onyx.zookeeper/backoff-max-retries defaults))))
  ([connection-string ns ^RetryPolicy retry-policy]
   (doto
     (.. (CuratorFrameworkFactory/builder)
         (namespace ns)
         (connectString connection-string)
         (retryPolicy retry-policy)
         (build))
     .start)))

(defn close
  "Closes the connection to the ZooKeeper server."
  [^CuratorFramework client] 
  (.close client))

(defn create-mode [opts]
  (cond (and (:persistent? opts) (:sequential? opts))
        CreateMode/PERSISTENT_SEQUENTIAL
        (:persistent? opts)
        CreateMode/PERSISTENT
        (:sequential? opts)
        CreateMode/EPHEMERAL_SEQUENTIAL
        :else
        CreateMode/EPHEMERAL))

(defn create
  [^CuratorFramework client path & {:keys [data] :as opts}]
  (try 
    (let [cr (.. client 
                 create 
                 (withMode (create-mode opts)))]
      (if data 
        (.forPath cr path data)
        (.forPath cr path)))
    (catch org.apache.zookeeper.KeeperException$NodeExistsException e
      false)))

(defn create-all
  [^CuratorFramework client path & {:keys [data] :as opts}]
  (try 
    (let [cr (.. client 
                 create 
                 creatingParentsIfNeeded 
                 (withMode (create-mode opts)))]
      (if data 
        (.forPath cr path data)
        (.forPath cr path)))
    (catch org.apache.zookeeper.KeeperException$NodeExistsException e
      false) ))

(defn delete
  "Deletes the given node if it exists. Otherwise returns false."
  [^CuratorFramework client path]
  (try
    (.. client (delete) (forPath path))
    (catch KeeperException$NoNodeException e false)))

(defn children 
  ([^CuratorFramework curator path & {:keys [watcher]}]
   (.forPath 
     (cond-> (.getChildren curator)
       watcher (.usingWatcher (zi/make-watcher watcher))) 
     path)))

(defn data [client path]
  (let [stat (Stat.)
        data (.forPath (.storingStatIn (.getData client) stat) path)] 
    {:data data
     :stat (zi/stat-to-map stat)}))

(defn set-data [client path data version]
  (.. client 
      (setData)
      (withVersion version)
      (forPath path data)))

(defn exists [client path & {:keys [watcher]}]
  (zi/stat-to-map 
    (cond-> (.. client checkExists)
      watcher (.usingWatcher (zi/make-watcher watcher))
      true (.forPath path)))) 
