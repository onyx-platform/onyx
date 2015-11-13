(ns onyx.state.bookkeeper
  (:require [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [taoensso.nippy :as nippy]
            [onyx.log.curator :as zk]
            [onyx.log.zookeeper :as ozk]
            [onyx.static.default-vals :refer [defaults arg-or-default]]
            [com.stuartsierra.component :as component])
  (:import [org.apache.bookkeeper.client LedgerHandle BookKeeper BookKeeper$DigestType AsyncCallback$AddCallback]
           [org.apache.bookkeeper.proto BookieServer]
           [org.apache.bookkeeper.conf ServerConfiguration]
           [org.apache.bookkeeper.conf ClientConfiguration]
           [org.apache.bookkeeper.zookeeper ZooKeeperClient]
           [onyx.log.zookeeper]
           [org.apache.commons.io FileUtils]
           [org.apache.curator.framework CuratorFramework CuratorFrameworkFactory]))

(defn cleanup-dirs [servers]
  (doseq [dir (map :ledger-dir servers)]
    (FileUtils/deleteDirectory (java.io.File. ^String dir)))
  (doseq [dir (map :journal-dir servers)]
    (FileUtils/deleteDirectory (java.io.File. ^String dir))))

(defrecord Bookie [env-config log]
  component/Lifecycle
  (component/start [component]
    (if (arg-or-default :onyx.bookkeeper/server? env-config)
      (let [onyx-id (:onyx/id env-config)
            ledgers-root-path (ozk/ledgers-path onyx-id)
            ledgers-available-path (ozk/ledgers-available-path onyx-id)
            _ (zk/create (:conn log) ledgers-root-path :persistent? true) 
            _ (zk/create (:conn log) ledgers-available-path :persistent? true) 
            local-quorum? (arg-or-default :onyx.bookkeeper/local-quorum? env-config)
            ports (if local-quorum?
                    (arg-or-default :onyx.bookkeeper/local-quorum-ports env-config)
                    (vector (arg-or-default :onyx.bookkeeper/port env-config)))
            base-journal-dir (arg-or-default :onyx.bookkeeper/base-journal-dir env-config)
            base-ledger-dir (arg-or-default :onyx.bookkeeper/base-ledger-dir env-config)
            servers (mapv (fn [port]
                            (let [server-id (str onyx-id "_" port)
                                  journal-dir (str base-journal-dir "/" server-id)
                                  ledger-dir (str base-ledger-dir "/" server-id)
                                  server-conf (doto (ServerConfiguration.)
                                                (.setZkServers (:zookeeper/address env-config))
                                                (.setZkLedgersRootPath ledgers-root-path)
                                                (.setBookiePort port)
                                                ;(.setAutoRecoveryDaemonEnabled true)
                                                (.setJournalDirName journal-dir)
                                                (.setLedgerDirNames (into-array String [ledger-dir]))
                                                (.setAllowLoopback true))
                                  server (BookieServer. server-conf)] 
                              (info "Starting BookKeeper server on port" port)
                              (.start server)
                              {:server server 
                               :port port 
                               :journal-dir journal-dir 
                               :ledger-dir ledger-dir}))
                          ports)]
        (when (:onyx.bookkeeper/delete-server-data? env-config) 
          (.addShutdownHook (Runtime/getRuntime) 
                            (Thread. (fn []
                                       (cleanup-dirs servers)))))
        (assoc component :servers servers)) 
      component))
  (component/stop [{:keys [servers] :as component}]
    (doseq [server servers]
      (info "Stopping BookKeeper server")
      (.shutdown ^BookieServer (:server server)))
    (cleanup-dirs servers)
    (assoc component :servers nil)))

(defn new-bookie [env-config]
  (map->Bookie {:env-config env-config}))
