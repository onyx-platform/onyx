(ns onyx.state.filter.rocksdb-test
  (:require [onyx.state.filter.rocksdb :as rdb]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal error warn trace info]]
            [onyx.state.state-extensions :as se]
            [onyx.compression.nippy :as nippy]
            [clojure.core.async :refer [thread chan]]
            [clojure.test :refer [deftest is testing]]))

(defn extract-values [db buckets]
  (loop [bucket (first buckets) buckets (rest buckets) vs []] 
    (if bucket 
      (recur (first buckets) 
             (rest buckets)
             (let [iterator (.newIterator db bucket)]
               (.seekToFirst iterator)
               (loop [vs' vs]
                 (if (.isValid iterator)
                   (let [v (nippy/localdb-decompress (.key iterator))]
                     (.next iterator)
                     (recur (conj vs' v)))
                   vs'))))
      vs)))

(defn write-bucket [num-buckets per-bucket f bucket]
  (rdb/rotate-bucket! (:db f) num-buckets (:buckets f) (:bucket f))
  (reduce (fn [f' vr]
            (se/apply-filter-id f' {} (+ vr (* bucket per-bucket))))
          f
          (range per-bucket)))

(deftest rocksdb-filter-test 
  (with-redefs [rdb/start-rotation-thread! (fn [_ _ _ _ _ _] (thread))] 
    (let [per-bucket 10
          n-buckets 255
          rfilter (se/initialize-filter :rocksdb {:onyx.core/peer-opts {:onyx.rocksdb.filter/num-ids-per-bucket per-bucket
                                                                        :onyx.rocksdb.filter/num-buckets n-buckets}
                                                  :onyx.core/id (str :peer-id (java.util.UUID/randomUUID))
                                                  :onyx.core/task-id :task-id})
          filter-range (range (inc n-buckets))]
      (try
        (let [new-rfilter (reduce (partial write-bucket n-buckets per-bucket)
                                  rfilter
                                  (range n-buckets))]


          ;; One bucket is always left cleared, we wrote right up until the point where some data would be cleared
          (is (= (range 2550)
                 (sort (extract-values (:db new-rfilter) @(:buckets new-rfilter)))))

          (write-bucket n-buckets per-bucket new-rfilter 255)

          (testing "Initial bucket rotated, some data deleted and new data entered"
            (is (= (range per-bucket (+ 2550 per-bucket))
                   (sort (extract-values (:db new-rfilter) @(:buckets new-rfilter)))))))
        (finally
          (se/close-filter rfilter {}))))))

(deftest rocksdb-restore-test 
  (with-redefs [rdb/start-rotation-thread! (fn [_ _ _ _ _ _] (thread))] 
    (let [per-bucket 10
          n-buckets 255
          rfilter (se/initialize-filter :rocksdb {:onyx.core/peer-opts {:onyx.rocksdb.filter/num-ids-per-bucket per-bucket
                                                                        :onyx.rocksdb.filter/num-buckets n-buckets}
                                                  :onyx.core/id (str :peer-id (java.util.UUID/randomUUID))
                                                  :onyx.core/task-id :task-id})
          filter-range (range (inc n-buckets))]
      (try
        (let [new-rfilter (reduce (partial write-bucket n-buckets per-bucket)
                                  rfilter
                                  (range n-buckets))

              snapshot @(se/snapshot-filter new-rfilter {})
              restore-filter (-> :rocksdb 
                                 (se/initialize-filter {:onyx.core/peer-opts {:onyx.rocksdb.filter/rotate-filter-bucket-every-n per-bucket}
                                                        :onyx.core/id (str :peer-id (java.util.UUID/randomUUID))
                                                        :onyx.core/task-id :task-id})
                                 (se/restore-filter {} snapshot))]
          (try 
            (is (= (extract-values (:db new-rfilter) @(:buckets new-rfilter))
                   (extract-values (:db restore-filter) @(:buckets restore-filter))))
            (is (= (count @(:buckets new-rfilter))
                   (count @(:buckets restore-filter))))
            (is (= @(:id-counter new-rfilter)
                   @(:id-counter restore-filter)))
            (finally
              (se/close-filter restore-filter {})))) 
        (finally
          (se/close-filter rfilter {}))))))
