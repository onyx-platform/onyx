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

(deftest lmdb-filter-test
  (let [per-bucket 10
        n-buckets 255
        id1a (java.util.UUID/randomUUID)
        id1b (java.util.UUID/randomUUID)
        id2a 123456789
        id2b 123456788
        
        lmdb (se/initialize-filter :lmdb {:onyx.core/peer-opts {:onyx.rocksdb.filter/num-ids-per-bucket per-bucket
                                                                        :onyx.rocksdb.filter/num-buckets n-buckets}
                                                  :onyx.core/id (str :peer-id (java.util.UUID/randomUUID))
                                                  :onyx.core/task-id :task-id})
        ]

        (se/apply-filter-id lmdb nil id1a)
        (se/apply-filter-id lmdb nil id2a)

        (is (se/filter? lmdb nil id1a))
        (is (se/filter? lmdb nil id1a))

        (is (not (se/filter? lmdb nil id1b)))
        (is (not (se/filter? lmdb nil id2b)))
        
        (se/close-filter lmdb nil)
        ))  
