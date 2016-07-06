(ns onyx.state.bookkeeper
  (:require [com.stuartsierra.component :as component]
            [onyx.log.curator :as zk]
            [onyx.log.zookeeper :as ozk]
            [onyx.static.default-vals :refer [arg-or-default]]
            [taoensso.timbre :refer [error info warn]])
  (:import [java.io File]
           [org.apache.bookkeeper.bookie Bookie BookieException$InvalidCookieException]
           [org.apache.bookkeeper.conf ServerConfiguration]
           [org.apache.bookkeeper.proto BookieServer]
           [org.apache.commons.io FileUtils]
           [org.apache.zookeeper KeeperException$NodeExistsException]))

(defn cleanup-dir [dir]
  (FileUtils/deleteDirectory (File. ^String dir)))

(defn format-bk-server
  "Delete journal/ledger directories and removes corresponding cookie in Zookeeper"
  [^ServerConfiguration server-conf zk-conn]
  (let [journal-dir (str (.getJournalDirName server-conf))
        ledger-dir (str (first (.getLedgerDirNames server-conf)))
        cookie-path (format "%s/cookies/%s"
                            (.getZkLedgersRootPath server-conf)
                            (Bookie/getBookieAddress server-conf))]
    (info "Deleting Bookie cookie" cookie-path)
    (zk/delete zk-conn cookie-path)
    (info "Deleting Bookie dirs" journal-dir ";" ledger-dir)
    (cleanup-dir journal-dir)
    (cleanup-dir ledger-dir)))

(defrecord BookieComponent [env-config port log]
  component/Lifecycle
  (start [component]
    (try
      (let [onyx-id (:onyx/tenancy-id env-config)
            ledgers-root-path (arg-or-default :onyx.bookkeeper/zk-ledgers-root-path env-config)
            ledgers-available-path (str ledgers-root-path "/available")
            _ (zk/create (:conn log) ledgers-root-path :persistent? true)
            _ (zk/create (:conn log) ledgers-available-path :persistent? true)
            base-journal-dir (arg-or-default :onyx.bookkeeper/base-journal-dir env-config)
            base-ledger-dir (arg-or-default :onyx.bookkeeper/base-ledger-dir env-config)
            ;; allow loopback? only if running a local quorum
            allow-loopback? (boolean (arg-or-default :onyx.bookkeeper/local-quorum? env-config))
            disk-usage-threshold (arg-or-default :onyx.bookkeeper/disk-usage-threshold env-config)
            disk-usage-warn-threshold (arg-or-default :onyx.bookkeeper/disk-usage-warn-threshold env-config)
            server-id (str onyx-id "_" port)
            journal-dir (str base-journal-dir "/" server-id)
            ledger-dir (str base-ledger-dir "/" server-id)
            server-conf (doto (ServerConfiguration.)
                          (.setZkServers (:zookeeper/address env-config))
                          (.setZkLedgersRootPath ledgers-root-path)
                          (.setBookiePort port)
                          (.setJournalDirName journal-dir)
                          (.setLedgerDirNames (into-array String [ledger-dir]))
                          (.setAllowLoopback allow-loopback?)
                          (.setDiskUsageThreshold disk-usage-threshold)
                          (.setDiskUsageWarnThreshold disk-usage-warn-threshold))
            server (BookieServer. server-conf)]
        (when (:onyx.bookkeeper/delete-server-data? env-config)
          (.addShutdownHook (Runtime/getRuntime)
                            (Thread. (fn []
                                       (format-bk-server server-conf (:conn log))))))
        (info "Starting BookKeeper server on port" port)
        (info "Creating Bookie dirs" journal-dir ";" ledger-dir)
        (.start ^BookieServer server)
        (assoc component
               :server server
               :server-conf server-conf
               :port port))
      (catch Exception e
        (if (or (instance? BookieException$InvalidCookieException (.getCause e))
                (instance? KeeperException$NodeExistsException (.getCause e)))
          (throw (ex-info "The Bookie server failed to start because a cookie or
                           ledger already exists for this host. Set
                           :onyx.bookkeeper/delete-server-data? true to format
                           the Bookie environment on component shutdown."))
          (throw e)))))
  (stop [{:keys [server server-conf] :as component}]
    (info "Stopping BookKeeper server:")
    (.shutdown ^BookieServer server)
    (info "Stopped BookKeeper server with exit code:" (.getExitCode ^BookieServer server))
    (when (:onyx.bookkeeper/delete-server-data? env-config)
      (format-bk-server server-conf (:conn log)))
    (assoc component :server nil :port nil :journal-dir nil :ledger-dir nil)))

(defn started? [bookie]
  (if (nil? bookie)
    false
    (.isRunning ^BookieServer (:server bookie))))

(defrecord BookieMonitor [env-config log port]
  component/Lifecycle
  (component/start [component]
    (let [bookie (atom (component/start (->BookieComponent env-config port log)))
          monitor-fut (future
                        (while (not (Thread/interrupted))
                          (when-not (started? @bookie)
                            (warn "BookKeeper server shut itself down or died. Restarting.")
                            (try
                              (reset! bookie (component/start (->BookieComponent env-config port log)))
                              (catch Throwable t
                                (error t "Error starting BookKeeper server:"))))
                          (Thread/sleep 1000)))]
      (info "Starting BookKeeper Monitor service")
      (assoc component :bookie bookie :monitor-fut monitor-fut)))
  (component/stop [component]
    (try
      (info "Stopping BookKeeper Monitor service")
      (future-cancel (:monitor-fut component))
      (when-let [bookie @(:bookie component)]
        (component/stop bookie))
      (catch Throwable t
        (error t "Error stopping BookKeeper Monitor")))
    (assoc component :bookie nil :monitor-fut nil)))

(defn new-bookie-monitor [env-config port]
  (map->BookieMonitor {:env-config env-config :port port}))

(defrecord BookieServers [env-config log]
  component/Lifecycle
  (start [component]
    (if (arg-or-default :onyx.bookkeeper/server? env-config)
      (let [local-quorum? (arg-or-default :onyx.bookkeeper/local-quorum? env-config)
            ports (if local-quorum?
                    (arg-or-default :onyx.bookkeeper/local-quorum-ports env-config)
                    (vector (arg-or-default :onyx.bookkeeper/port env-config)))]
        (assoc component :servers (mapv (fn [port]
                                          (component/start (->BookieMonitor env-config log port)))
                                        ports)))
      component))
  (stop [component]
    (if (arg-or-default :onyx.bookkeeper/server? env-config)
      (doseq [server (:servers component)]
        (component/stop server)))))

(defn multi-bookie-server [env-config]
  (map->BookieServers {:env-config env-config}))

(defmethod clojure.core/print-method BookieServers
  [system ^java.io.Writer writer]
  (.write writer "#<Bookie Servers>"))
