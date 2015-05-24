(ns onyx.log.zookeeper
  (:require [clojure.core.async :refer [chan >!! <!! close! thread]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal warn trace]]
            [onyx.log.curator :as zk]
            [zookeeper.internal :as zi]
            [onyx.extensions :as extensions]
            [onyx.static.default-vals :refer [defaults]]
            [onyx.compression.nippy :refer [compress decompress]])
  (:import [org.apache.curator.test TestingServer]))

(def root-path "/onyx")

(defn prefix-path [prefix]
  (str root-path "/" prefix))

(defn pulse-path [prefix]
  (str (prefix-path prefix) "/pulse"))

(defn log-path [prefix]
  (str (prefix-path prefix) "/log"))

(defn catalog-path [prefix]
  (str (prefix-path prefix) "/catalog"))

(defn workflow-path [prefix]
  (str (prefix-path prefix) "/workflow"))

(defn flow-path [prefix]
  (str (prefix-path prefix) "/flow"))

(defn lifecycles-path [prefix]
  (str (prefix-path prefix) "/lifecycles"))

(defn task-path [prefix]
  (str (prefix-path prefix) "/task"))

(defn sentinel-path [prefix]
  (str (prefix-path prefix) "/sentinel"))

(defn chunk-path [prefix]
  (str (prefix-path prefix) "/chunk"))

(defn origin-path [prefix]
  (str (prefix-path prefix) "/origin"))

(defn job-scheduler-path [prefix]
  (str (prefix-path prefix) "/job-scheduler"))

(defn messaging-path [prefix]
  (str (prefix-path prefix) "/messaging"))

(defn throw-subscriber-closed []
  (throw (ex-info "Log subscriber closed due to disconnection from ZooKeeper" {})))

(defn clean-up-broken-connections [f]
  (try
    (f)
    (catch org.apache.zookeeper.KeeperException$ConnectionLossException e
      (trace e)
      (throw-subscriber-closed))
    (catch org.apache.zookeeper.KeeperException$SessionExpiredException e
      (trace e)
      (throw-subscriber-closed))))

(defn initialize-origin! [conn config prefix]
  (clean-up-broken-connections
   (fn []
     (let [node (str (origin-path prefix) "/origin")
           bytes (compress {:message-id -1 :replica {}})]
       (zk/create conn node :data bytes :persistent? true)))))

(defrecord ZooKeeper [config]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting ZooKeeper" (if (:zookeeper/server? config) "server" "client connection"))
    (let [onyx-id (:onyx/id config)
          server (when (:zookeeper/server? config) (TestingServer. (int (:zookeeper.server/port config))))
          conn (zk/connect (:zookeeper/address config))]
      (zk/create conn root-path :persistent? true)
      (zk/create conn (prefix-path onyx-id) :persistent? true)
      (zk/create conn (pulse-path onyx-id) :persistent? true)
      (zk/create conn (log-path onyx-id) :persistent? true)
      (zk/create conn (catalog-path onyx-id) :persistent? true)
      (zk/create conn (workflow-path onyx-id) :persistent? true)
      (zk/create conn (flow-path onyx-id) :persistent? true)
      (zk/create conn (lifecycles-path onyx-id) :persistent? true)
      (zk/create conn (task-path onyx-id) :persistent? true)
      (zk/create conn (sentinel-path onyx-id) :persistent? true)
      (zk/create conn (chunk-path onyx-id) :persistent? true)
      (zk/create conn (origin-path onyx-id) :persistent? true)
      (zk/create conn (job-scheduler-path onyx-id) :persistent? true)
      (zk/create conn (messaging-path onyx-id) :persistent? true)

      (initialize-origin! conn config onyx-id)
      (assoc component :server server :conn conn :prefix onyx-id)))

  (stop [component]
    (taoensso.timbre/info "Stopping ZooKeeper" (if (:zookeeper/server? config) "server" "client connection"))
    (zk/close (:conn component))

    (when (:server component)
      (.close ^TestingServer (:server component)))

    component))

(defn zookeeper [config]
  (map->ZooKeeper {:config config}))

(defn pad-sequential-id
  "ZooKeeper sequential IDs are at least 10 digits.
   If this node's id is less, pad it. Otherwise returns the str'ed id"
  [id]
  (let [padding 10
        id-len (count (str id))]
    (if (< id-len padding)
      (str (apply str (take (- padding id-len) (repeat "0"))) id)
      (str id))))

(defmethod extensions/write-log-entry ZooKeeper
  [{:keys [conn opts prefix] :as log} data]
  (clean-up-broken-connections
   (fn []
     (let [node (str (log-path prefix) "/entry-")
           bytes (compress data)]
       (zk/create conn node :data bytes :persistent? true :sequential? true)))))

(defmethod extensions/read-log-entry ZooKeeper
  [{:keys [conn opts prefix] :as log} position]
  (clean-up-broken-connections
   (fn []
     (let [node (str (log-path prefix) "/entry-" (pad-sequential-id position))
           data (zk/data conn node)
           content (decompress (:data data))]
       (assoc content :message-id position :created-at (:ctime (:stat data)))))))

(defmethod extensions/register-pulse ZooKeeper
  [{:keys [conn opts prefix] :as log} id]
  (clean-up-broken-connections
   (fn []
     (let [node (str (pulse-path prefix) "/" id)]
       (zk/create conn node :persistent? false)))))

(defmethod extensions/on-delete ZooKeeper
  [{:keys [conn opts prefix] :as log} id ch]
  (let [f (fn [event]
            (when (= (:event-type event) :NodeDeleted)
              (>!! ch true)))]
    (try
      (when-not (zk/exists conn (str (pulse-path prefix) "/" id) :watcher f)
        (>!! ch true))
      (catch Throwable e
        (trace e)
        ;; Node doesn't exist.
        (>!! ch true)))))

(defn find-job-scheduler [log]
  (loop []
    (if-let [chunk
             (try
               (extensions/read-chunk log :job-scheduler nil)
               (catch Throwable e
                 (warn e)
                 (warn "Job scheduler couldn't be discovered. Backing off 500ms and trying again...")
                 nil))]
      (:job-scheduler chunk)
      (do (Thread/sleep 500)
          (recur)))))

(defn find-messaging [log]
  (loop []
    (if-let [chunk
             (try
               (extensions/read-chunk log :messaging nil)
               (catch Throwable e
                 (warn e)
                 (warn "Messaging couldn't be discovered. Backing off 500ms and trying again...")
                 nil))]
      (:messaging chunk)
      (do (Thread/sleep 500)
          (recur)))))

(defmethod extensions/subscribe-to-log ZooKeeper
  [{:keys [conn opts prefix] :as log} ch]
  (let [rets (chan)]
    (thread
     (try
       (let [job-scheduler (find-job-scheduler log)
             messaging (find-messaging log)
             origin (extensions/read-chunk log :origin nil)
             starting-position (inc (:message-id origin))]
         (>!! rets (assoc (:replica origin) 
                          :job-scheduler job-scheduler
                          :messaging messaging))
         (close! rets)
         (loop [position starting-position]
           (let [path (str (log-path prefix) "/entry-" (pad-sequential-id position))]
             (if (zk/exists conn path)
               (>!! ch position)
               (loop []
                 (let [read-ch (chan 2)]
                   (zk/children conn (log-path prefix) :watcher (fn [_] (>!! read-ch true)))
                   ;; Log entry may have been added in between initial check and when we
                   ;; added the watch.
                   (when (zk/exists conn path)
                     (>!! read-ch true))
                   (<!! read-ch)
                   (close! read-ch)
                   ;; Requires one more check. Watch may have been triggered by a delete
                   ;; from a GC call.
                   (if (zk/exists conn path)
                     (>!! ch position)
                     (recur)))))
             (recur (inc position)))))
       (catch java.lang.IllegalStateException e
         (trace e)
         ;; Curator client has been shutdown, close the subscriber cleanly.
         (close! ch))
       (catch org.apache.zookeeper.KeeperException$ConnectionLossException e
         (trace e)
         ;; ZooKeeper has been shutdown, close the subscriber cleanly.
         (close! ch))
       (catch org.apache.zookeeper.KeeperException$SessionExpiredException e
         (trace e)
         (close! ch))
       (catch Throwable e
         (fatal e))))
    (<!! rets)))

(defmethod extensions/write-chunk [ZooKeeper :catalog]
  [{:keys [conn opts prefix] :as log} kw chunk id]
  (clean-up-broken-connections
   (fn []
     (let [node (str (catalog-path prefix) "/" id)
           bytes (compress chunk)]
       (zk/create conn node :persistent? true :data bytes)))))

(defmethod extensions/write-chunk [ZooKeeper :workflow]
  [{:keys [conn opts prefix] :as log} kw chunk id]
  (clean-up-broken-connections
   (fn []
     (let [node (str (workflow-path prefix) "/" id)
           bytes (compress chunk)]
       (zk/create conn node :persistent? true :data bytes)))))

(defmethod extensions/write-chunk [ZooKeeper :flow-conditions]
  [{:keys [conn opts prefix] :as log} kw chunk id]
  (clean-up-broken-connections
   (fn []
     (let [node (str (flow-path prefix) "/" id)
           bytes (compress chunk)]
       (zk/create conn node :persistent? true :data bytes)))))

(defmethod extensions/write-chunk [ZooKeeper :lifecycles]
  [{:keys [conn opts prefix] :as log} kw chunk id]
  (clean-up-broken-connections
   (fn []
     (let [node (str (lifecycles-path prefix) "/" id)
           bytes (compress chunk)]
       (zk/create conn node :persistent? true :data bytes)))))

(defmethod extensions/write-chunk [ZooKeeper :task]
  [{:keys [conn opts prefix] :as log} kw chunk id]
  (clean-up-broken-connections
   (fn []
     (let [node (str (task-path prefix) "/" (:id chunk))
           bytes (compress chunk)]
       (zk/create conn node :persistent? true :data bytes)))))

(defmethod extensions/write-chunk [ZooKeeper :sentinel]
  [{:keys [conn opts prefix] :as log} kw chunk id]
  (clean-up-broken-connections
   (fn []
     (let [node (str (sentinel-path prefix) "/" id)
           bytes (compress chunk)]
       (zk/create conn node :persistent? true :data bytes)))))

(defmethod extensions/write-chunk [ZooKeeper :chunk]
  [{:keys [conn opts prefix] :as log} kw chunk id]
  (clean-up-broken-connections
   (fn []
     (let [node (str (chunk-path prefix) "/" id "/chunk")
           bytes (compress chunk)]
       (zk/create-all conn node :persistent? true :data bytes)
       id))))

(defmethod extensions/write-chunk [ZooKeeper :job-scheduler]
  [{:keys [conn opts prefix] :as log} kw chunk id]
  (clean-up-broken-connections
   (fn []
     (let [node (str (job-scheduler-path prefix) "/scheduler")
           bytes (compress chunk)]
       (zk/create conn node :persistent? true :data bytes)))))

(defmethod extensions/write-chunk [ZooKeeper :messaging]
  [{:keys [conn opts prefix] :as log} kw chunk id]
  (clean-up-broken-connections
   (fn []
     (let [node (str (messaging-path prefix) "/messaging")
           bytes (compress chunk)]
       (zk/create conn node :persistent? true :data bytes)))))

(defmethod extensions/force-write-chunk [ZooKeeper :chunk]
  [{:keys [conn opts prefix] :as log} kw chunk id]
  (clean-up-broken-connections
   (fn []
     (let [node (str (chunk-path prefix) "/" id "/chunk")
           version (:version (zk/exists conn node))
           bytes (compress chunk)]
       (zk/set-data conn node bytes version)))))

(defmethod extensions/read-chunk [ZooKeeper :catalog]
  [{:keys [conn opts prefix] :as log} kw id & _]
  (clean-up-broken-connections
   (fn []
     (let [node (str (catalog-path prefix) "/" id)]
       (decompress (:data (zk/data conn node)))))))

(defmethod extensions/read-chunk [ZooKeeper :workflow]
  [{:keys [conn opts prefix] :as log} kw id & _]
  (clean-up-broken-connections
   (fn []
     (let [node (str (workflow-path prefix) "/" id)]
       (decompress (:data (zk/data conn node)))))))

(defmethod extensions/read-chunk [ZooKeeper :flow-conditions]
  [{:keys [conn opts prefix] :as log} kw id & _]
  (clean-up-broken-connections
   (fn []
     (let [node (str (flow-path prefix) "/" id)]
       (decompress (:data (zk/data conn node)))))))

(defmethod extensions/read-chunk [ZooKeeper :lifecycles]
  [{:keys [conn opts prefix] :as log} kw id & _]
  (clean-up-broken-connections
   (fn []
     (let [node (str (lifecycles-path prefix) "/" id)]
       (decompress (:data (zk/data conn node)))))))

(defmethod extensions/read-chunk [ZooKeeper :task]
  [{:keys [conn opts prefix] :as log} kw id & _]
  (clean-up-broken-connections
   (fn []
     (let [node (str (task-path prefix) "/" id)]
       (decompress (:data (zk/data conn node)))))))

(defmethod extensions/read-chunk [ZooKeeper :sentinel]
  [{:keys [conn opts prefix] :as log} kw id & _]
  (clean-up-broken-connections
   (fn []
     (let [node (str (sentinel-path prefix) "/" id)]
       (decompress (:data (zk/data conn node)))))))

(defmethod extensions/read-chunk [ZooKeeper :chunk]
  [{:keys [conn opts prefix] :as log} kw id & _]
  (clean-up-broken-connections
   (fn []
     (let [node (str (chunk-path prefix) "/" id "/chunk")]
       (decompress (:data (zk/data conn node)))))))

(defmethod extensions/read-chunk [ZooKeeper :origin]
  [{:keys [conn opts prefix] :as log} kw id & _]
  (clean-up-broken-connections
   (fn []
     (let [node (str (origin-path prefix) "/origin")]
       (decompress (:data (zk/data conn node)))))))

(defmethod extensions/read-chunk [ZooKeeper :job-scheduler]
  [{:keys [conn opts prefix] :as log} kw id & _]
  (clean-up-broken-connections
   (fn []
     (let [node (str (job-scheduler-path prefix) "/scheduler")]
       (decompress (:data (zk/data conn node)))))))

(defmethod extensions/read-chunk [ZooKeeper :messaging]
  [{:keys [conn opts prefix] :as log} kw id & _]
  (clean-up-broken-connections
   (fn []
     (let [node (str (messaging-path prefix) "/messaging")]
       (decompress (:data (zk/data conn node)))))))

(defmethod extensions/update-origin! ZooKeeper
  [{:keys [conn opts prefix] :as log} replica message-id]
  (clean-up-broken-connections
   (fn []
     (let [node (str (origin-path prefix) "/origin")
           version (:version (zk/exists conn node))
           content (decompress (:data (zk/data conn node)))]
       (when (< (:message-id content) message-id)
         (let [new-content {:message-id message-id :replica replica}]
           (zk/set-data conn node (compress new-content) version)))))))

(defmethod extensions/gc-log-entry ZooKeeper
  [{:keys [conn opts prefix] :as log} position]
  (clean-up-broken-connections
   (fn []
     (let [node (str (log-path prefix) "/entry-" (pad-sequential-id position))]
       (zk/delete conn node)))))
