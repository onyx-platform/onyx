(ns onyx.state.filter.lmdb-test
  (:require [onyx.state.filter.lmdb :as wf]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal error warn trace info]]
            [onyx.state.state-extensions :as se]
            [onyx.compression.nippy :as nippy]
            [clojure.core.async :refer [thread chan]]
            [clojure.test :refer [deftest is testing]]
            [clj-lmdb.core   :as lmdbc]
            [clj-lmdb.simple :as lmdbs]
            [onyx.compression.nippy :as nippy]))

(def magic-value 
  (doto (byte-array 1)
    (aset 0 (byte 99))))

(deftest lmdb-test
  (let [per-bucket 10
        n-buckets 255
        i1 ^bytes (nippy/localdb-compress (java.util.UUID/randomUUID))
        i2 ^bytes (nippy/localdb-compress 125)
        lmdb (se/initialize-filter :lmdb {:onyx.core/peer-opts {:onyx.rocksdb.filter/num-ids-per-bucket per-bucket
                                                                        :onyx.rocksdb.filter/num-buckets n-buckets}
                                                  :onyx.core/id (str :peer-id (java.util.UUID/randomUUID))
                                                  :onyx.core/task-id :task-id})
        ]
        (lmdbs/with-txn [txn (lmdbc/write-txn (:db lmdb))]
          (lmdbc/put! (:db lmdb)
                      txn
                      i1
                      magic-value))

        (lmdbs/with-txn [txn (lmdbc/read-txn (:db lmdb))]
          (is (not (nil? (lmdbc/get! (:db lmdb)
                                     txn
                                     i1)))))))  
