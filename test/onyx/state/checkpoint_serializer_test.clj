(ns onyx.state.checkpoint-serializer-test
  (:require [onyx.state.serializers.checkpoint :as c]
            [onyx.state.serializers.utils :as u]
            [onyx.compression.nippy :refer [statedb-compress statedb-decompress]]
            [clojure.test :refer [deftest is]])
  (:import [org.agrona.concurrent UnsafeBuffer]
           [org.agrona MutableDirectBuffer]
           [org.agrona ExpandableArrayBuffer]))

#_(deftest checkpoint-sz-test
  (let [a (ExpandableArrayBuffer.)
        enc (c/->StateCheckpointEncoder a 0 0)
        metadata-bs (statedb-compress {11 12 1231231 11})
        values (mapv statedb-decompress
                     [1
                      "STOIRSTIERNSTIERNTSRSIETNRENST" 
                      3929923392932
                      {:iesnatar 3838}])]
    (c/set-schema-version enc 9994)
    (c/set-metadata enc metadata-bs)
    (c/set-next-bytes enc (get values 0))
    (c/set-next-bytes enc (get values 1))
    (c/set-next-bytes enc (get values 2))
    (c/set-next-bytes enc (get values 3))
    (let [dec (c/->StateCheckpointDecoder a (c/length enc) 0)]
      (is (= 9994 (c/get-schema-version dec)))
      (is (u/equals metadata-bs (c/get-metadata dec)))
      (is (u/equals (get values 0) (c/get-next-bytes dec)))
      (is (u/equals (get values 1) (c/get-next-bytes dec)))
      (is (u/equals (get values 2) (c/get-next-bytes dec)))
      (is (u/equals (get values 3) (c/get-next-bytes dec)))
      (is (nil? (c/get-next-bytes dec))))))
