(ns onyx.state.state-store-gen-test
  (:require [clojure.test :refer [is deftest]]
            [onyx.state.protocol.db :as s]
            [onyx.compression.nippy :refer [localdb-decompress localdb-compress]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test :refer :all]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [onyx.state.lmdb]
            [onyx.state.serializers.utils :as sz-utils]
            [onyx.state.serializers.checkpoint :as cpenc]
            [onyx.state.serializers.windowing-key-encoder :as enc]
            [onyx.state.serializers.windowing-key-decoder :as dec]
            [onyx.state.memory])
  (:import [org.agrona.concurrent UnsafeBuffer]))

(def gen-group-key (gen/resize 5 gen/int))
(def gen-value (gen/resize 5 gen/any-printable))

(def windowed-grouped-global-indices #{0 2 4 6})
(def windowed-grouped-sliding-indices #{8 10 12 14})
(def windowed-grouped-session-indices #{16 18 20 22})
(def windowed-ungrouped-global-indices #{24 26 28 30})
(def windowed-ungrouped-sliding-indices #{32 34 36 38})
(def windowed-ungrouped-session-indices #{40 42 44 46})

(def gen-extent-long
  (gen/one-of [;; reduced range to test extents being modified often
               (gen/resize 50 (gen/fmap #(Math/abs (long %)) gen/int)) 
               ;; full range to test sorting
               (gen/fmap #(Math/abs (long %)) gen/int)]))

(def gen-extent-global
  (gen/return 1))

(def gen-global-grouped
  (gen/tuple (gen/elements windowed-grouped-global-indices) 
             (gen/return :windowed-grouped-global)
             gen-group-key
             gen-extent-global))

(def gen-sliding-grouped
  (gen/tuple (gen/elements windowed-grouped-sliding-indices) 
             (gen/return :windowed-grouped-sliding)
             gen-group-key
             gen-extent-long))

(def gen-session-grouped
  (gen/tuple (gen/elements windowed-grouped-session-indices) 
             (gen/return :windowed-grouped-session)
             gen-group-key
             (gen/tuple gen-extent-long gen-extent-long)))

(def gen-global-ungrouped
  (gen/tuple (gen/elements windowed-ungrouped-global-indices) 
             (gen/return :windowed-ungrouped-global)
             (gen/return nil)
             gen-extent-global))

(def gen-sliding-ungrouped
  (gen/tuple (gen/elements windowed-ungrouped-sliding-indices) 
             (gen/return :windowed-ungrouped-sliding)
             (gen/return nil)
             gen-extent-long))

(def gen-session-ungrouped
  (gen/tuple (gen/elements windowed-ungrouped-session-indices) 
             (gen/return :windowed-ungrouped-session)
             (gen/return nil)
             (gen/tuple gen-extent-long gen-extent-long)))

(def window-generators
  (gen/one-of [gen-session-grouped gen-global-grouped gen-sliding-grouped 
               gen-global-ungrouped gen-sliding-ungrouped gen-session-ungrouped]))

(def add-windowed-extent
  (gen/tuple (gen/return :add-extent) 
             window-generators
             gen-value))

(def delete-windowed-extent
  (gen/tuple (gen/return :delete-extent) 
             window-generators))

(def triggered-grouped-indices #{1 3 5 7})
(def triggered-ungrouped-indices #{9 11 13 15})

(def all-triggered-indices (into triggered-grouped-indices triggered-ungrouped-indices))

(def gen-trigger-grouped 
  (gen/tuple 
   (gen/elements triggered-grouped-indices)
   (gen/return :trigger-grouped)
   gen-group-key))

(def gen-trigger-ungrouped 
  (gen/tuple 
   (gen/elements triggered-ungrouped-indices)
   (gen/return :trigger-ungrouped)
   (gen/return nil)))

(def gen-trigger
  (gen/one-of [gen-trigger-grouped gen-trigger-ungrouped]))

(def add-trigger-value
  (gen/tuple (gen/return :add-trigger) gen-trigger gen-value))

(def window-serializers 
  (set 
   (concat (map (fn [w] {:idx w 
                         :type :window
                         :grouped? true 
                         :extent :nil}) 
                windowed-grouped-global-indices)
           (map (fn [w] {:idx w 
                         :type :window
                         :grouped? true 
                         :extent :long}) 
                windowed-grouped-sliding-indices)
           (map (fn [w] {:idx w 
                         :type :window
                         :grouped? true 
                         :extent :long-long}) 
                windowed-grouped-session-indices)
           (map (fn [w] {:idx w 
                         :type :window
                         :grouped? false 
                         :extent :nil}) 
                windowed-ungrouped-global-indices)
           (map (fn [w] {:idx w 
                         :type :window
                         :grouped? false 
                         :extent :long}) 
                windowed-ungrouped-sliding-indices)
           (map (fn [w] {:idx w 
                         :type :window
                         :grouped? false 
                         :extent :long-long}) 
                windowed-ungrouped-session-indices))))

(def trigger-serializers 
  (set 
   (concat (map (fn [w] {:idx w 
                         :type :trigger
                         :grouped? true}) 
                triggered-grouped-indices)
           (map (fn [w] {:idx w 
                         :type :trigger
                         :grouped? false}) 
                triggered-ungrouped-indices))))

(deftest state-backend-differences
  (checking "Memory db as oracle for state db"
   (times 600)
   [values (gen/vector (gen/one-of [add-windowed-extent delete-windowed-extent add-trigger-value]))]
   (let [db-name (str (java.util.UUID/randomUUID))
         coders (sz-utils/build-coders window-serializers trigger-serializers)
         mem-store (s/create-db {:onyx.peer/state-store-impl :memory} :state-id-1 coders)
         db-store (s/create-db {:onyx.peer/state-store-impl :lmdb} db-name coders)
         cp-encoder-db (cpenc/empty-checkpoint-encoder)
         cp-encoder-mem (cpenc/empty-checkpoint-encoder)
         db-store-memory->lmdb (s/create-db {:onyx.peer/state-store-impl :lmdb} (str (java.util.UUID/randomUUID)) coders)
         db-store-lmdb->memory (s/create-db {:onyx.peer/state-store-impl :memory} (str (java.util.UUID/randomUUID)) coders)]
     (try
      ;time 
      (doseq [[type [state-idx _ group-key extent] value] values]
        (case type 
          :add-extent (do 
                       (s/put-extent! db-store state-idx (s/group-id db-store group-key) extent value)
                       (s/put-extent! mem-store state-idx (s/group-id mem-store group-key) extent value))
          :delete-extent (do
                          (s/delete-extent! db-store state-idx (s/group-id db-store group-key) extent)
                          (s/delete-extent! mem-store state-idx (s/group-id mem-store group-key) extent))
          :add-trigger (do
                        (s/put-trigger! db-store state-idx (s/group-id db-store group-key) value)
                        (s/put-trigger! mem-store state-idx (s/group-id mem-store group-key) value))))

      (s/export db-store cp-encoder-db)
      (s/export mem-store cp-encoder-mem)

      (is (= (cpenc/length cp-encoder-db)
             (cpenc/length cp-encoder-mem)))

      ;; test restore from mem store to db store
      (let [cp-decoder (cpenc/->StateCheckpointDecoder 
                        (.buffer ^onyx.state.serializers.checkpoint.StateCheckpointEncoder cp-encoder-mem)
                        (cpenc/length cp-encoder-mem) 
                        0)]
        (s/restore! db-store-memory->lmdb cp-decoder identity))

      ;; test restore from db store to mem store
      (let [cp-decoder (cpenc/->StateCheckpointDecoder 
                        (.buffer ^onyx.state.serializers.checkpoint.StateCheckpointEncoder cp-encoder-db)
                        (cpenc/length cp-encoder-db) 
                        0)]
        (s/restore! db-store-lmdb->memory cp-decoder identity))

      (doseq [state-idx (distinct (map (fn [[_ [idx]]] idx) values))]
        (assert (number? state-idx))
        ;; remove nils which are artifact of how ungrouped are stored
        ;; return to this to fix
        (is (= (remove nil? (s/groups db-store state-idx))
               (remove nil? (s/groups mem-store state-idx))
               (remove nil? (s/groups db-store-memory->lmdb state-idx))
               (remove nil? (s/groups db-store-lmdb->memory state-idx)))))

      (doseq [[state-idx group-key] (->> values
                                         (filter (fn [[type]]
                                                   (not= :add-trigger type)))
                                         (map (fn [[_ [state-idx _ group-key]]] 
                                                [state-idx group-key]))
                                         (distinct))]
        (let [serialized-group (some->> group-key (s/group-id db-store))
              serialized-group-mem-store (s/group-id mem-store group-key)]
          (is (= (s/get-trigger mem-store state-idx serialized-group-mem-store)
                 (s/get-trigger db-store state-idx serialized-group)))

          (is (= (s/get-trigger db-store state-idx serialized-group)
                 (s/get-trigger db-store-memory->lmdb state-idx serialized-group)
                 (s/get-trigger db-store-lmdb->memory state-idx serialized-group)))

          (is (= (s/group-extents mem-store state-idx serialized-group-mem-store)
                 (sort (s/group-extents mem-store state-idx serialized-group-mem-store))))

          (is (= (s/group-extents db-store state-idx serialized-group)
                 (s/group-extents mem-store state-idx serialized-group-mem-store)))

          (is (= (s/group-extents db-store state-idx serialized-group)
                 (s/group-extents db-store-memory->lmdb state-idx serialized-group)
                 (s/group-extents db-store-lmdb->memory state-idx serialized-group-mem-store)))

          ;; take only first value of trigger keys as the group key will differ due to serialization
          (is (= (sort (map first (s/trigger-keys db-store state-idx)))
                 (sort (map first (s/trigger-keys mem-store state-idx))))
              [(s/trigger-keys db-store state-idx)
               (s/trigger-keys mem-store state-idx)])))


      (finally 
       (s/drop! db-store)
       (s/close! db-store)
       (s/drop! db-store-memory->lmdb)
       (s/close! db-store-memory->lmdb)
       (s/drop! db-store-lmdb->memory)
       (s/close! db-store-lmdb->memory)
       (s/drop! mem-store)
       (s/close! mem-store))))))
