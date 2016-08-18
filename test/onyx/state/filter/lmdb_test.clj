(ns onyx.state.filter.lmdb-test
  (:require [onyx.state.filter.lmdb :as lmdb]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal error warn trace info]]
            [onyx.state.state-extensions :as se]
            [onyx.compression.nippy :as nippy]
            [clojure.core.async :refer [thread chan]]
            [clojure.test :refer [deftest is testing]]))

(deftest lmdb-test
  (let [per-bucket 10
        n-buckets 255] 
        (se/initialize-filter :lmdb {:onyx.core/peer-opts {:onyx.rocksdb.filter/num-ids-per-bucket per-bucket
                                                                        :onyx.rocksdb.filter/num-buckets n-buckets}
                                                  :onyx.core/id (str :peer-id (java.util.UUID/randomUUID))
                                                  :onyx.core/task-id :task-id})))  
