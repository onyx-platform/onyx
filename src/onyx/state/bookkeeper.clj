(ns onyx.state.bookkeeper
  (:require [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [taoensso.nippy :as nippy]
            [onyx.log.curator :as zk]
            [onyx.log.zookeeper :as ozk]
            [com.stuartsierra.component :as component])
  (:import [org.apache.bookkeeper.client LedgerHandle BookKeeper BookKeeper$DigestType AsyncCallback$AddCallback]
           [org.apache.bookkeeper.proto BookieServer]
           [org.apache.bookkeeper.conf ServerConfiguration]
           [org.apache.bookkeeper.conf ClientConfiguration]
           [org.apache.bookkeeper.zookeeper ZooKeeperClient]
           [onyx.log.zookeeper]
           [org.apache.curator.framework CuratorFramework CuratorFrameworkFactory]))

(def password (.getBytes "somepass"))

(def bookie-server 
  {:zookeeper/address "127.0.0.1:2181"
   :zookeeper/timeout 20000
   :onyx/id (java.util.UUID/randomUUID)
   :bookie/ledger-directories ["/tmp/bk-data996"]
   :bookie/journal-directory "/tmp/bk-txn996"
   :bookie/port 3196})

(defrecord Bookie [conf]
  component/Lifecycle
  (component/start [component]
    (let [ledgers-path (ozk/ledgers-path (:onyx/id conf))
          ledgers-available-path (ozk/ledgers-available-path (:onyx/id conf))
          conn (zk/connect (:zookeeper/address conf))
          _ (zk/create-all conn ledgers-available-path :persistent? true) 
          server-conf (doto (ServerConfiguration.)
                        (.setZkServers (:zookeeper/address conf))
                        (.setZkLedgersRootPath ledgers-path)
                        (.setBookiePort (:bookie/port conf))
                        (.setJournalDirName (:bookie/journal-directory conf))
                        (.setLedgerDirNames (into-array String (:bookie/ledger-directories conf)))
                        (.setAllowLoopback true))
          server (BookieServer. server-conf)] 
      (.start server)
      (assoc component :server server)))
  (component/stop [component]
    (.stop (:server component))
    (assoc component :server nil)))
