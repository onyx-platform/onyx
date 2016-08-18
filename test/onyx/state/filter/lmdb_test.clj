(ns onyx.state.filter.lmdb-test
  (:require [onyx.state.filter.lmdb :as wf]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal error warn trace info]]
            [onyx.state.state-extensions :as se]
            [onyx.compression.nippy :as nippy]
            [clojure.core.async :refer [thread chan]]
            [clojure.test :refer [deftest is testing]]
            [clj-lmdb.simple :as lmdb]))

(def magic-value 
  (doto (byte-array 1)
    (aset 0 (byte 99))))

(deftest lmdb-test
  (let [per-bucket 10
        n-buckets 255 
        lmdb (se/initialize-filter :lmdb {:onyx.core/peer-opts {:onyx.rocksdb.filter/num-ids-per-bucket per-bucket
                                                                        :onyx.rocksdb.filter/num-buckets n-buckets}
                                                  :onyx.core/id (str :peer-id (java.util.UUID/randomUUID))
                                                  :onyx.core/task-id :task-id})
        ]
        (lmdb/with-txn [txn (lmdb/write-txn (:db lmdb))]
          (lmdb/put! (:db lmdb)
                     txn
                     "foo"
                     "bar"))
        (lmdb/with-txn [txn (lmdb/read-txn (:db lmdb))]
          (is (= (lmdb/get! (:db lmdb)
                            txn
                            "foo")
                 "bar")))
        ))  
