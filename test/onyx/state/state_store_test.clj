(ns onyx.state.state-store-test
  (:require [clojure.test :refer [is deftest]]
            [onyx.state.protocol.db :as s]
            [onyx.compression.nippy :refer [statedb-compress statedb-decompress]]
            [onyx.state.lmdb]
            [onyx.state.serializers.windowing-key-encoder :as enc]
            [onyx.state.serializers.windowing-key-decoder :as dec]
            [onyx.state.memory])
  (:import [org.agrona.concurrent UnsafeBuffer]))

;; FIXME copy in stored S3 file, convert to new format
#_(deftest basic-state-store-test
  (let [window-id 0
        db-name (str (java.util.UUID/randomUUID))
        ;store (onyx.state.memory/create-db {} :state-id-1)
        store (onyx.state.lmdb/create-db {} db-name)]
    (try
     (let [group-id-1 (s/group-id store :group1)
           group-id-2 (s/group-id store :group2)]
       ;; actions
       (s/put-extent! store window-id group-id-1 3 :my-value1)
       (s/put-extent! store window-id group-id-2 5 :my-value2)
       (s/put-extent! store window-id group-id-2 6 :my-value3)
       ;; new extent, out of order
       (s/put-extent! store window-id group-id-2 3 :my-value3)
       (is (= [3] (s/group-extents store window-id group-id-1)))
       ; (is (= [3 5 6] (s/group-extents store window-id group-id-2)))

       ; (s/delete-extent! store window-id group-id-2 6)
       ; (is (= [3 5] (s/group-extents store window-id group-id-2)))

       ; ;; results
       ; (is (= :my-value1 (s/get-extent store window-id group-id-1 3)))
       ; (is (= :my-value2 (s/get-extent store window-id group-id-2 5)))
       ; (is (nil? (s/get-extent store window-id group-id-2 6)))
       )
     (finally (s/drop! store)))))

#_(deftest basic-state-export-restore-test
  (let [db-name (str (java.util.UUID/randomUUID))
        window-id 0
        store (onyx.state.lmdb/create-db {} db-name)
        store-mem (onyx.state.memory/create-db {} db-name)
        ]
    (try
     (doseq [s [store store-mem]]
       ;; actions
       (s/put-extent! s window-id :group1 3 :my-value1)
       (s/put-extent! s window-id :group2 5 :my-value2)
       (s/put-extent! s window-id :group2 6 :my-value3))
     (is (= (sort (s/groups store window-id)) 
            (sort (s/groups store-mem window-id))))

     (let [exported (s/export-state store window-id)
           exported-groups (s/export-groups store)
           ;store2 (onyx.state.memory/create-db {} :state-id-2)
           db-name (str (java.util.UUID/randomUUID))
           store2 (onyx.state.lmdb/create-db {} db-name)]
       (try
        (s/restore! store2 exported)
        (s/restore! store2 exported-groups)
        (is (= (s/groups store window-id) 
               (s/groups store2 window-id)))
        (is (= :my-value1 (s/get-extent store2 window-id :group1 3)))
        (is (= :my-value2 (s/get-extent store window-id :group2 5)))
        (is (= :my-value3 (s/get-extent store window-id :group2 6)))
        (is (nil? (s/get-extent store window-id :group2 7)))
        (finally (s/drop! store2))))
     (finally (s/drop! store)))))


#_(deftest serializer-test
  (let [bs1 (byte-array 1000)
        buf1 (UnsafeBuffer. bs1)
        enc1 (enc/wrap buf1 0)
        dec1 (dec/wrap buf1 0)]
    (enc/set-type enc1 (byte 0))
    (enc/set-state-idx enc1 1)
    (enc/set-group enc1 (localdb-compress 2))
    (enc/set-extent enc1 3)

    (is (= (byte 0) (dec/get-type dec1)))
    (is (= 1 (dec/get-state-idx dec1)))
    (is (= 2 (localdb-decompress (dec/get-group dec1))))
    (is (= 3 (dec/get-extent dec1)))


    ))

#_(deftest comparator-test
  (let [bs1 (byte-array 1000)
        buf1 (UnsafeBuffer. bs1)
        bs2 (byte-array 1000)
        buf2 (UnsafeBuffer. bs1)
        enc1 (enc/wrap buf1 0)
        enc2 (enc/wrap buf2 0)
        dec1 (dec/wrap buf1 0)
        dec2 (dec/wrap buf2 0)
        cmp ^java.util.Comparator (onyx.state.lmdb/->Comparator buf1 dec1 buf2 dec2)]
    (enc/set-state-idx enc1 0)
    (enc/set-group enc1 0)
    (enc/set-extent enc1 0)
    (enc/set-state-idx enc2 0)
    (enc/set-group enc2 0)
    (enc/set-extent enc2 0)
    (is (zero? (.compare cmp bs1 bs2)))
    (is (zero? (.compare cmp bs2 bs1)))
    (enc/set-state-idx enc1 1)
    (is (pos? (.compare cmp bs1 bs2)))
    (is (neg? (.compare cmp bs2 bs1)))
    (enc/set-state-idx enc1 0)
    (is (zero? (.compare cmp bs1 bs2)))
    (is (zero? (.compare cmp bs2 bs1)))
    (enc/set-extent enc1 1)
    (is (pos? (.compare cmp bs1 bs2)))
    (is (neg? (.compare cmp bs2 bs1)))
    (enc/set-extent enc1 0)
    (enc/set-group enc1 1)
    (is (pos? (.compare cmp bs1 bs2)))
    (is (neg? (.compare cmp bs2 bs1)))))
