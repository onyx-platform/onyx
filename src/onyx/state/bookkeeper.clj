(ns onyx.state.bookkeeper
  (:require [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [taoensso.nippy :as nippy]
            [onyx.log.curator :as zk]
            [onyx.log.zookeeper :as ozk]
            [onyx.static.default-vals :refer [defaults]]
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
  (doseq [dir (mapcat (juxt :ledger-dir 
                            :journal-dir)
                      servers)]
    (FileUtils/deleteDirectory (java.io.File. dir))))

(defrecord Bookie [env-config log]
  component/Lifecycle
  (component/start [component]
    (let [{:keys [bookkeeper/server? bookkeeper/local-quorum? bookkeeper/base-directory]} env-config]
      (if server?
        (let [ledgers-root-path (ozk/ledgers-path (:onyx/id env-config))
              ledgers-available-path (ozk/ledgers-available-path (:onyx/id env-config))
              _ (zk/create (:conn log) ledgers-root-path :persistent? true) 
              _ (zk/create (:conn log) ledgers-available-path :persistent? true) 
              server-count (if local-quorum? 3 1)
              base-port (:onyx.bookkeeper/starting-port defaults)
              ports (range base-port (+ base-port server-count))
              base-dir (or (:onyx.bookkeeper/base-dir env-config) 
                           (:onyx.bookkeeper/base-dir defaults))
              servers (mapv (fn [port]
                              (let [server-id (java.util.UUID/randomUUID)
                                    journal-dir (str base-dir port "_jrnl_" server-id)
                                    ledger-dir (str base-dir port "_ldgr_" server-id)
                                    server-conf (doto (ServerConfiguration.)
                                                  (.setZkServers (:zookeeper/address env-config))
                                                  (.setZkLedgersRootPath ledgers-root-path)
                                                  (.setBookiePort port)
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
          (.addShutdownHook (Runtime/getRuntime) 
                            (Thread. (fn []
                                       (cleanup-dirs servers))))
          (assoc component :servers servers)) 
        component)))
  (component/stop [{:keys [servers] :as component}]
    (doseq [server servers]
      (info "Stopping BookKeeper server")
      (.shutdown (:server server)))
    (cleanup-dirs servers)
    (assoc component :servers nil)))

(defn new-bookie [env-config]
  (map->Bookie {:env-config env-config}))
