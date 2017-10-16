(ns onyx.log.zookeeper
  (:require [clojure.core.async :refer [chan >!! <!! close! thread alts!! offer!]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal warn info trace]]
            [onyx.log.curator :as zk]
            [onyx.extensions :as extensions]
            [onyx.checkpoint :as checkpoint]
            [onyx.compression.nippy :refer [zookeeper-compress zookeeper-decompress]]
            [onyx.log.replica :as replica]
            [onyx.peer.log-version]
            [onyx.monitoring.measurements :refer [measure-latency]]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.schema :as os]
            [schema.core :as s])
  (:import [org.apache.curator.test TestingServer]
           [org.apache.log4j BasicConfigurator]
           [org.apache.curator.framework CuratorFramework]
           [org.apache.zookeeper KeeperException$NoNodeException 
            KeeperException$NodeExistsException KeeperException$BadVersionException]))

(def root-path "/onyx")
(def savepoint-path "/onyx-savepoints")

(defn prefix-path [prefix]
  (when (nil? prefix)
    (throw (Exception. ":onyx/tenancy-id must not be empty")))
  (str root-path "/" prefix))

(defn latest-checkpoint-path [prefix job-id]
  (str savepoint-path "/latest/" prefix "/" job-id))

(defn pulse-path [prefix]
  (str (prefix-path prefix) "/pulse"))

(defn log-path [prefix]
  (str (prefix-path prefix) "/log"))

(defn job-hash-path [prefix]
  (str (prefix-path prefix) "/job-hash"))

(defn catalog-path [prefix]
  (str (prefix-path prefix) "/catalog"))

(defn workflow-path [prefix]
  (str (prefix-path prefix) "/workflow"))

(defn flow-path [prefix]
  (str (prefix-path prefix) "/flow"))

(defn lifecycles-path [prefix]
  (str (prefix-path prefix) "/lifecycles"))

(defn windows-path [prefix]
  (str (prefix-path prefix) "/windows"))

(defn triggers-path [prefix]
  (str (prefix-path prefix) "/triggers"))

(defn job-metadata-path [prefix]
  (str (prefix-path prefix) "/job-metadata"))

(defn resume-point-path [prefix]
  (str (prefix-path prefix) "/resume-point"))

(defn task-path [prefix]
  (str (prefix-path prefix) "/task"))

(defn sentinel-path [prefix]
  (str (prefix-path prefix) "/sentinel"))

(defn chunk-path [prefix]
  (str (prefix-path prefix) "/chunk"))

(defn origin-path [prefix]
  (str (prefix-path prefix) "/origin"))

(defn log-parameters-path [prefix]
  (str (prefix-path prefix) "/log-parameters"))

(defn exception-path [prefix]
  (str (prefix-path prefix) "/exception"))

(defn checkpoint-path [prefix]
  (str (prefix-path prefix) "/checkpoint"))

(defn checkpoint-path-version [prefix job-id replica-version epoch]
  (str (checkpoint-path prefix) "/" job-id "/" replica-version "-" epoch))

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
           bytes (zookeeper-compress {:message-id -1 :replica replica/base-replica})]
       (zk/create conn node :data bytes :persistent? true)))))

(defrecord ZooKeeper [config]
  component/Lifecycle

  (start [component]
    (s/validate os/PeerClientConfig config)
    (taoensso.timbre/info "Starting ZooKeeper" (if (:zookeeper/server? config) "server" "client connection. If Onyx hangs here it may indicate a difficulty connecting to ZooKeeper."))
    (BasicConfigurator/configure)
    (let [onyx-id (:onyx/tenancy-id config)
          server (when (:zookeeper/server? config) (TestingServer. (int (:zookeeper.server/port config))))
          conn (zk/connect (:zookeeper/address config))
          kill-ch (chan)]
      (zk/create conn root-path :persistent? true)
      (zk/create conn (prefix-path onyx-id) :persistent? true)
      (zk/create conn (pulse-path onyx-id) :persistent? true)
      (zk/create conn (log-path onyx-id) :persistent? true)
      (zk/create conn (job-hash-path onyx-id) :persistent? true)
      (zk/create conn (catalog-path onyx-id) :persistent? true)
      (zk/create conn (workflow-path onyx-id) :persistent? true)
      (zk/create conn (flow-path onyx-id) :persistent? true)
      (zk/create conn (lifecycles-path onyx-id) :persistent? true)
      (zk/create conn (windows-path onyx-id) :persistent? true)
      (zk/create conn (triggers-path onyx-id) :persistent? true)
      (zk/create conn (job-metadata-path onyx-id) :persistent? true)
      (zk/create conn (task-path onyx-id) :persistent? true)
      (zk/create conn (sentinel-path onyx-id) :persistent? true)
      (zk/create conn (chunk-path onyx-id) :persistent? true)
      (zk/create conn (origin-path onyx-id) :persistent? true)
      (zk/create conn (log-parameters-path onyx-id) :persistent? true)
      (zk/create conn (exception-path onyx-id) :persistent? true)
      (zk/create conn (checkpoint-path onyx-id) :persistent? true)
      (zk/create conn (resume-point-path onyx-id) :persistent? true)

      (initialize-origin! conn config onyx-id)
      (assoc component :server server :conn conn :prefix onyx-id :kill-ch kill-ch)))

  (stop [component]
    (taoensso.timbre/info "Stopping ZooKeeper" (if (:zookeeper/server? config) "server" "client connection"))
    (zk/close (:conn component))
    (close! (:kill-ch component))

    (when (:server component)
      (.close ^TestingServer (:server component)))

    component))

(defmethod clojure.core/print-method ZooKeeper
  [system ^java.io.Writer writer]
  (.write writer "#<ZooKeeper Component>"))

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
  [{:keys [conn opts prefix monitoring] :as log} data]
  (let [bytes (zookeeper-compress data)]
    (measure-latency
     #(clean-up-broken-connections
       (fn []
         (let [node (str (log-path prefix) "/entry-")]
           (zk/create conn node :data bytes :persistent? true :sequential? true))))
     #(let [args {:event :zookeeper-write-log-entry
                  :latency % :bytes (count bytes)}]
        (extensions/emit monitoring args)))))

(defmethod extensions/read-log-entry ZooKeeper
  [{:keys [conn opts prefix monitoring] :as log} position]
  (measure-latency
   #(clean-up-broken-connections
     (fn []
       (let [node (str (log-path prefix) "/entry-" (pad-sequential-id position))
             data (zk/data conn node)
             content (zookeeper-decompress (:data data))]
         (assoc content :message-id position :created-at (:ctime (:stat data))))))
   #(let [args {:event :zookeeper-read-log-entry :position position :latency %}]
      (extensions/emit monitoring args))))

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
              (offer! ch true)))]
    (try
      (when-not (zk/exists conn (str (pulse-path prefix) "/" id) :watcher f)
        (offer! ch true))
      (catch Throwable e
        (trace e)
        ;; Node doesn't exist.
        (offer! ch true)))))

(defmethod extensions/group-exists? ZooKeeper
  [{:keys [conn opts prefix] :as log} id]
  (zk/exists conn (str (pulse-path prefix) "/" id)))

(defmethod extensions/connected? ZooKeeper
  [{:keys [conn opts prefix] :as log}]
  (.isConnected (.getZookeeperClient ^CuratorFramework conn)))

(defn find-log-parameters [log]
  (loop []
    (if-let [chunk
             (try
               (extensions/read-chunk log :log-parameters nil)
               (catch Throwable e
                 (warn e)
                 (warn (str "Log parameters have yet to be written to ZooKeeper by a peer. "
                            "Backing off 500ms and trying again..."))
                 nil))]
      chunk
      (do (Thread/sleep 500)
          (recur)))))

(defn seek-to-new-origin! [log ch]
  (let [origin (extensions/read-chunk log :origin nil)
        starting-position (inc (:message-id origin))
        entry (create-log-entry :set-replica! {:replica origin})]
    (>!! ch entry)
    starting-position))

(defn seek-and-put-entry! [log position ch]
  (try
    (let [entry (extensions/read-log-entry log position)]
      (assert entry)
      (>!! ch entry)
      (inc position))
    (catch KeeperException$NoNodeException e
      (seek-to-new-origin! log ch))
    (catch KeeperException$NodeExistsException e
      (seek-to-new-origin! log ch))))

(defn await-entry! [{:keys [conn opts prefix kill-ch] :as log} ch path position]
  (loop []
    (let [read-ch (chan 2)]
      (zk/children conn (log-path prefix) :watcher (fn [_] (offer! read-ch true)))
      ;; Log entry may have been added in between initial check and when we
      ;; added the watch.
      (when (zk/exists conn path)
        (offer! read-ch true))
      (let [[_ active-ch] (alts!! [read-ch kill-ch])]
        (cond (= active-ch kill-ch)
              :killed
              (= active-ch read-ch)
              (do 
               (close! read-ch)
               ;; Requires one more check. Watch may have been triggered by a delete
               ;; from a GC call.
               (if (zk/exists conn path)
                 (seek-and-put-entry! log position ch)
                 (recur))))))))

(defmethod extensions/subscribe-to-log ZooKeeper
  [{:keys [conn opts prefix kill-ch] :as log} ch]
  (let [rets (chan)]
    (thread
     (try
       (let [log-parameters (find-log-parameters log)
             origin (extensions/read-chunk log :origin nil)
             starting-position (inc (:message-id origin))]
         (>!! rets (merge (:replica origin) log-parameters))
         (close! rets)
         (loop [position starting-position]
           (let [path (str (log-path prefix) "/entry-" (pad-sequential-id position))
                 new-position (if (zk/exists conn path)
                                (seek-and-put-entry! log position ch)
                                (await-entry! log ch path position))]
             (when-not (= new-position :killed)
               (assert (integer? new-position) new-position)
               (recur new-position)))))
       (catch java.lang.IllegalStateException e
         (trace e)
         ;; Curator client has been shutdown, pass exception along
         (>!! ch e))
       (catch org.apache.zookeeper.KeeperException$ConnectionLossException e
         ;; ZooKeeper has been shutdown, pass exception along
         (trace e)
         (>!! ch e))
       (catch org.apache.zookeeper.KeeperException$SessionExpiredException e
         (trace e)
         (>!! ch e))
       (catch Throwable e
         (fatal "extensions/subscribe-to-log threw exception." e)
         (>!! ch e))))
    (<!! rets)))

(defmethod extensions/write-chunk [ZooKeeper :job-hash]
  [{:keys [conn opts prefix monitoring] :as log} kw chunk id]
  (let [bytes (zookeeper-compress chunk)]
    (measure-latency
     #(clean-up-broken-connections
       (fn []
         (let [node (str (job-hash-path prefix) "/" id)]
           (zk/create conn node :persistent? true :data bytes))))
     #(let [args {:event :zookeeper-write-job-hash :id id
                  :latency % :bytes (count bytes)}]
        (extensions/emit monitoring args)))))

(defmethod extensions/write-chunk [ZooKeeper :catalog]
  [{:keys [conn opts prefix monitoring] :as log} kw chunk id]
  (let [bytes (zookeeper-compress chunk)]
    (measure-latency
     #(clean-up-broken-connections
       (fn []
         (let [node (str (catalog-path prefix) "/" id)]
           (zk/create conn node :persistent? true :data bytes))))
     #(let [args {:event :zookeeper-write-catalog :id id
                  :latency % :bytes (count bytes)}]
        (extensions/emit monitoring args)))))

(defmethod extensions/write-chunk [ZooKeeper :workflow]
  [{:keys [conn opts prefix monitoring] :as log} kw chunk id]
  (let [bytes (zookeeper-compress chunk)]
    (measure-latency
     #(clean-up-broken-connections
       (fn []
         (let [node (str (workflow-path prefix) "/" id)]
           (zk/create conn node :persistent? true :data bytes))))
     #(let [args {:event :zookeeper-write-workflow :id id
                  :latency % :bytes (count bytes)}]
        (extensions/emit monitoring args)))))

(defmethod extensions/write-chunk [ZooKeeper :flow-conditions]
  [{:keys [conn opts prefix monitoring] :as log} kw chunk id]
  (let [bytes (zookeeper-compress chunk)]
    (measure-latency
     #(clean-up-broken-connections
       (fn []
         (let [node (str (flow-path prefix) "/" id)]
           (zk/create conn node :persistent? true :data bytes))))
     #(let [args {:event :zookeeper-write-flow-conditions :id id
                  :latency % :bytes (count bytes)}]
        (extensions/emit monitoring args)))))

(defmethod extensions/write-chunk [ZooKeeper :lifecycles]
  [{:keys [conn opts prefix monitoring] :as log} kw chunk id]
  (let [bytes (zookeeper-compress chunk)]
    (measure-latency
     #(clean-up-broken-connections
       (fn []
         (let [node (str (lifecycles-path prefix) "/" id)]
           (zk/create conn node :persistent? true :data bytes))))
     #(let [args {:event :zookeeper-write-lifecycles :id id
                  :latency % :bytes (count bytes)}]
        (extensions/emit monitoring args)))))

(defmethod extensions/write-chunk [ZooKeeper :windows]
  [{:keys [conn opts prefix monitoring] :as log} kw chunk id]
  (let [bytes (zookeeper-compress chunk)]
    (measure-latency
     #(clean-up-broken-connections
       (fn []
         (let [node (str (windows-path prefix) "/" id)]
           (zk/create conn node :persistent? true :data bytes))))
     #(let [args {:event :zookeeper-write-windows :id id
                  :latency % :bytes (count bytes)}]
        (extensions/emit monitoring args)))))

(defmethod extensions/write-chunk [ZooKeeper :triggers]
  [{:keys [conn opts prefix monitoring] :as log} kw chunk id]
  (let [bytes (zookeeper-compress chunk)]
    (measure-latency
     #(clean-up-broken-connections
       (fn []
         (let [node (str (triggers-path prefix) "/" id)]
           (zk/create conn node :persistent? true :data bytes))))
     #(let [args {:event :zookeeper-write-triggers :id id
                  :latency % :bytes (count bytes)}]
        (extensions/emit monitoring args)))))

(defmethod extensions/write-chunk [ZooKeeper :job-metadata]
  [{:keys [conn opts prefix monitoring] :as log} kw chunk id]
  (let [bytes (zookeeper-compress chunk)]
    (measure-latency
     #(clean-up-broken-connections
       (fn []
         (let [node (str (job-metadata-path prefix) "/" id)]
           (zk/create conn node :persistent? true :data bytes))))
     #(let [args {:event :zookeeper-write-job-metadata :id id
                  :latency % :bytes (count bytes)}]
        (extensions/emit monitoring args)))))

(defmethod extensions/write-chunk [ZooKeeper :resume-point]
  [{:keys [conn opts prefix monitoring] :as log} kw [task-id chunk] job-id]
  (let [bytes (zookeeper-compress chunk)]
    (measure-latency
     #(clean-up-broken-connections
       (fn []
         (let [node (str (resume-point-path prefix) "/" job-id "/" task-id)]
           (zk/create-all conn node :persistent? true :data bytes))))
     #(let [args {:event :zookeeper-write-resume-point :id job-id
                  :latency % :bytes (count bytes)}]
        (extensions/emit monitoring args)))))

(defmethod extensions/write-chunk [ZooKeeper :task]
  [{:keys [conn opts prefix monitoring] :as log} kw chunk id]
  (let [bytes (zookeeper-compress chunk)]
    (measure-latency
     #(clean-up-broken-connections
       (fn []
         (let [node (str (task-path prefix) "/" id "/" (:id chunk))]
           (zk/create-all conn node :persistent? true :data bytes))))
     #(let [args {:event :zookeeper-write-task :id (:id chunk)
                  :latency % :bytes (count bytes)}]
        (extensions/emit monitoring args)))))

(defmethod extensions/write-chunk [ZooKeeper :chunk]
  [{:keys [conn opts prefix monitoring] :as log} kw chunk id]
  (let [bytes (zookeeper-compress chunk)]
    (measure-latency
     #(clean-up-broken-connections
       (fn []
         (let [node (str (chunk-path prefix) "/" id "/chunk")]
           (zk/create-all conn node :persistent? true :data bytes)
           id)))
     #(let [args {:event :zookeeper-write-chunk :id id
                  :latency % :bytes (count bytes)}]
        (extensions/emit monitoring args)))))

(defmethod extensions/write-chunk [ZooKeeper :log-parameters]
  [{:keys [conn opts prefix monitoring] :as log} kw chunk id]
  (let [bytes (zookeeper-compress chunk)]
    (measure-latency
     #(clean-up-broken-connections
       (fn []
         (let [node (str (log-parameters-path prefix) "/log-parameters")]
           (zk/create conn node :persistent? true :data bytes))))
     #(let [args {:event :zookeeper-write-log-parameters :id id
                  :latency % :bytes (count bytes)}]
        (extensions/emit monitoring args)))))

(defmethod extensions/write-chunk [ZooKeeper :exception]
  [{:keys [conn opts prefix monitoring] :as log} kw chunk id]
  (let [bytes (zookeeper-compress chunk)]
    (measure-latency
     #(clean-up-broken-connections
       (fn []
         (let [node (str (exception-path prefix) "/" id)]
           (zk/create-all conn node :persistent? true :data bytes)
           id)))
     #(let [args {:event :zookeeper-write-exception :id id
                  :latency % :bytes (count bytes)}]
        (extensions/emit monitoring args)))))

(defmethod extensions/force-write-chunk [ZooKeeper :chunk]
  [{:keys [conn opts prefix monitoring] :as log} kw chunk id]
  (let [bytes (zookeeper-compress chunk)]
    (measure-latency
     #(clean-up-broken-connections
       (fn []
         (let [node (str (chunk-path prefix) "/" id "/chunk")
               version (:version (zk/exists conn node))]
           (if (nil? version)
             (zk/create-all conn node :persistent? true :data bytes)
             (zk/set-data conn node bytes version)))))
     #(let [args {:event :zookeeper-force-write-chunk :id id
                  :latency % :bytes (count bytes)}]
        (extensions/emit monitoring args)))))

(defmethod extensions/read-chunk [ZooKeeper :job-hash]
  [{:keys [conn opts prefix monitoring] :as log} kw id & _]
  (measure-latency
   #(clean-up-broken-connections
     (fn []
       (let [node (str (job-hash-path prefix) "/" id)]
         (zookeeper-decompress (:data (zk/data conn node))))))
   #(let [args {:event :zookeeper-read-job-hash :id id :latency %}]
      (extensions/emit monitoring args))))

(defmethod extensions/read-chunk [ZooKeeper :catalog]
  [{:keys [conn opts prefix monitoring] :as log} kw id & _]
  (measure-latency
   #(clean-up-broken-connections
     (fn []
       (let [node (str (catalog-path prefix) "/" id)]
         (zookeeper-decompress (:data (zk/data conn node))))))
   #(let [args {:event :zookeeper-read-catalog :id id :latency %}]
      (extensions/emit monitoring args))))

(defmethod extensions/read-chunk [ZooKeeper :workflow]
  [{:keys [conn opts prefix monitoring] :as log} kw id & _]
  (measure-latency
   #(clean-up-broken-connections
     (fn []
       (let [node (str (workflow-path prefix) "/" id)]
         (zookeeper-decompress (:data (zk/data conn node))))))
   #(let [args {:event :zookeeper-read-workflow :id id :latency %}]
      (extensions/emit monitoring args))))

(defmethod extensions/read-chunk [ZooKeeper :flow-conditions]
  [{:keys [conn opts prefix monitoring] :as log} kw id & _]
  (measure-latency
   #(clean-up-broken-connections
     (fn []
       (let [node (str (flow-path prefix) "/" id)]
         (zookeeper-decompress (:data (zk/data conn node))))))
   #(let [args {:event :zookeeper-read-flow-conditions :id id :latency %}]
      (extensions/emit monitoring args))))

(defmethod extensions/read-chunk [ZooKeeper :lifecycles]
  [{:keys [conn opts prefix monitoring] :as log} kw id & _]
  (measure-latency
   #(clean-up-broken-connections
     (fn []
       (let [node (str (lifecycles-path prefix) "/" id)]
         (zookeeper-decompress (:data (zk/data conn node))))))
   #(let [args {:event :zookeeper-read-lifecycles :id id :latency %}]
      (extensions/emit monitoring args))))

(defmethod extensions/read-chunk [ZooKeeper :windows]
  [{:keys [conn opts prefix monitoring] :as log} kw id & _]
  (measure-latency
   #(clean-up-broken-connections
     (fn []
       (let [node (str (windows-path prefix) "/" id)]
         (zookeeper-decompress (:data (zk/data conn node))))))
   #(let [args {:event :zookeeper-read-windows :id id :latency %}]
      (extensions/emit monitoring args))))

(defmethod extensions/read-chunk [ZooKeeper :triggers]
  [{:keys [conn opts prefix monitoring] :as log} kw id & _]
  (measure-latency
   #(clean-up-broken-connections
     (fn []
       (let [node (str (triggers-path prefix) "/" id)]
         (zookeeper-decompress (:data (zk/data conn node))))))
   #(let [args {:event :zookeeper-read-triggers :id id :latency %}]
      (extensions/emit monitoring args))))

(defmethod extensions/read-chunk [ZooKeeper :job-metadata]
  [{:keys [conn opts prefix monitoring] :as log} kw id & _]
  (measure-latency
   #(clean-up-broken-connections
     (fn []
       (let [node (str (job-metadata-path prefix) "/" id)]
         (zookeeper-decompress (:data (zk/data conn node))))))
   #(let [args {:event :zookeeper-read-job-metadata :id id :latency %}]
      (extensions/emit monitoring args))))

(defmethod extensions/read-chunk [ZooKeeper :resume-point]
  [{:keys [conn opts prefix monitoring] :as log} kw job-id id & _]
  (measure-latency
   #(clean-up-broken-connections
     (fn []
       (let [node (str (resume-point-path prefix) "/" job-id "/" id)]
         (if (zk/exists conn node)
           (zookeeper-decompress (:data (zk/data conn node)))))))
   #(let [args {:event :zookeeper-read-resume-point :id id :latency %}]
      (extensions/emit monitoring args))))

(defmethod extensions/read-chunk [ZooKeeper :task]
  [{:keys [conn opts prefix monitoring] :as log} kw job-id id & _]
  (measure-latency
   #(clean-up-broken-connections
     (fn []
       (let [node (str (task-path prefix) "/" job-id "/" id)]
         (zookeeper-decompress (:data (zk/data conn node))))))
   #(let [args {:event :zookeeper-read-task :id id :latency %}]
      (extensions/emit monitoring args))))

(defmethod extensions/read-chunk [ZooKeeper :chunk]
  [{:keys [conn opts prefix monitoring] :as log} kw id & _]
  (measure-latency
   #(clean-up-broken-connections
     (fn []
       (let [node (str (chunk-path prefix) "/" id "/chunk")]
         (zookeeper-decompress (:data (zk/data conn node))))))
   #(let [args {:event :zookeeper-read-chunk :id id :latency %}]
      (extensions/emit monitoring args))))

(defmethod extensions/read-chunk [ZooKeeper :origin]
  [{:keys [conn opts prefix monitoring] :as log} kw id & _]
  (measure-latency
   #(clean-up-broken-connections
     (fn []
       (let [node (str (origin-path prefix) "/origin")]
         (zookeeper-decompress (:data (zk/data conn node))))))
   #(let [args {:event :zookeeper-read-origin :id id :latency %}]
      (extensions/emit monitoring args))))

(defmethod extensions/read-chunk [ZooKeeper :log-parameters]
  [{:keys [conn opts prefix monitoring] :as log} kw id & _]
  (measure-latency
   #(clean-up-broken-connections
     (fn []
       (let [node (str (log-parameters-path prefix) "/log-parameters")]
         (zookeeper-decompress (:data (zk/data conn node))))))
   #(let [args {:event :zookeeper-read-log-parameters :id id :latency %}]
      (extensions/emit monitoring args))))

(defmethod extensions/read-chunk [ZooKeeper :exception]
  [{:keys [conn opts prefix monitoring] :as log} kw id & _]
  (measure-latency
   #(clean-up-broken-connections
     (fn []
       (let [node (str (exception-path prefix) "/" id)]
         (zookeeper-decompress (:data (zk/data conn node))))))
   #(let [args {:event :zookeeper-read-exception :id id :latency %}]
      (extensions/emit monitoring args))))

(defmethod extensions/update-origin! ZooKeeper
  [{:keys [conn opts prefix monitoring] :as log} replica message-id]
  (measure-latency
   #(clean-up-broken-connections
     (fn []
       (let [node (str (origin-path prefix) "/origin")
             version (:version (zk/exists conn node))
             content (zookeeper-decompress (:data (zk/data conn node)))]
         (when (< (:message-id content) message-id)
           (let [new-content {:message-id message-id :replica replica}]
             (zk/set-data conn node (zookeeper-compress new-content) version))))))
   #(let [args {:event :zookeeper-write-origin :message-id message-id :latency %}]
      (extensions/emit monitoring args))))

(defmethod extensions/gc-log-entry ZooKeeper
  [{:keys [conn opts prefix monitoring] :as log} position]
  (measure-latency
   #(clean-up-broken-connections
     (fn []
       (let [node (str (log-path prefix) "/entry-" (pad-sequential-id position))]
         (zk/delete conn node))))
   #(let [args {:event :zookeeper-gc-log-entry :position position :latency %}]
      (extensions/emit monitoring args))))

(defn checkpoint-task-key [task-id slot-id checkpoint-type]
  (str (name task-id) "##" slot-id "##" (name checkpoint-type)))

(defn parse-task-key [s]
  (let [[_ task-id slot-id checkpoint-type] (re-matches #"([^\#]+)##(.*)##(.*)" s)]
    {:task-id (keyword task-id)  
     :slot-id (Integer/parseInt slot-id) 
     :checkpoint-type (keyword checkpoint-type)}))

(defmethod checkpoint/write-checkpoint ZooKeeper
  [{:keys [conn opts monitoring]} tenancy-id job-id replica-version epoch 
   task-id slot-id checkpoint-type checkpoint-bytes]
  (measure-latency
   #(clean-up-broken-connections
     (fn []
       (let [node (str (checkpoint-path-version tenancy-id job-id replica-version epoch)
                       "/" (checkpoint-task-key task-id slot-id checkpoint-type))]
         (zk/create-all conn node :persistent? true :data checkpoint-bytes))))
   #(let [args {:event :zookeeper-write-checkpoint :latency %}]
      (extensions/emit monitoring args))))

(defmethod checkpoint/complete? ZooKeeper
  [_]
  ;; synchronous write means it's already completed
  true)

(defmethod checkpoint/cancel! ZooKeeper
  [_])

(defmethod checkpoint/stop ZooKeeper
  [log] 
  ;; zookeeper connection is shared with peer group, so we don't want to stop it
  log)

(defmethod checkpoint/read-checkpoint ZooKeeper
  [{:keys [conn opts prefix monitoring] :as log} tenancy-id job-id 
   replica-version epoch task-id slot-id checkpoint-type]
  (measure-latency
   #(clean-up-broken-connections
     (fn []
       (let [node (str (checkpoint-path-version tenancy-id job-id replica-version epoch)
                       "/" (checkpoint-task-key task-id slot-id checkpoint-type))]
         (:data (zk/data conn node)))))
   #(let [args {:event :zookeeper-read-checkpoint :latency %}]
      (extensions/emit monitoring args))))

(defmethod checkpoint/write-checkpoint-coordinate ZooKeeper
  [{:keys [conn opts monitoring] :as log} tenancy-id job-id coordinate version] 
  (let [bytes (zookeeper-compress coordinate)]
    (measure-latency
     #(clean-up-broken-connections
       (fn []
         (let [node (latest-checkpoint-path tenancy-id job-id)]
           (zk/set-data conn node bytes version))))
     #(let [args {:event :zookeeper-write-checkpoint-coordinate :id job-id
                  :latency % :bytes (count bytes)}]
        (extensions/emit monitoring args)))))

(defmethod checkpoint/watch-checkpoint-coordinate ZooKeeper
  [{:keys [conn opts monitoring] :as log} tenancy-id job-id watch-fn] 
  ;; TODO, upgrade to latest curator and ZooKeeper so that we can remove the watch
  (zk/exists conn (latest-checkpoint-path tenancy-id job-id) :watcher watch-fn))

(defmethod checkpoint/read-checkpoint-coordinate ZooKeeper
  [{:keys [conn opts monitoring] :as log} tenancy-id job-id]
   (measure-latency
    #(clean-up-broken-connections
      (fn []
        (let [node (latest-checkpoint-path tenancy-id job-id)
              data (zk/data conn node)
              coordinate (zookeeper-decompress (:data data))]
          (if coordinate
            (assoc coordinate :created-at (:ctime (:stat data)))))))
    #(let [args {:event :zookeeper-read-checkpoint-coordinate :id job-id :latency %}]
       (extensions/emit monitoring args))))

;; Takes over the checkpoint coordinate node, so that the coordinator
;; will be the only node to be able to write to it
(defmethod checkpoint/assume-checkpoint-coordinate ZooKeeper
  [{:keys [conn opts monitoring] :as log} tenancy-id job-id]
  (measure-latency
   #(clean-up-broken-connections
     (fn []
       ;; keep writing until we own the node
       (loop []
         (let [node (latest-checkpoint-path tenancy-id job-id)]
           (if-let [version (try 
                              (try 
                               (let [{:keys [data stat]} (zk/data conn node)]
                                 ;; rewrite existing data to bump version number to kick
                                 ;; off other writers
                                 (:version (zk/set-data conn node data (:version stat))))
                               (catch org.apache.zookeeper.KeeperException$NoNodeException nne
                                 ;; initialise to nil
                                 (zk/create-all conn node :persistent? true 
                                                :data (zookeeper-compress nil))
                                 (:version (zk/exists conn node))))
                              (catch KeeperException$BadVersionException bve
                                false))]
             version
             (recur))))))
   #(let [args {:event :zookeeper-read-checkpoint-coordinate-version :id job-id :latency %}]
      (extensions/emit monitoring args))))
