(ns onyx.state.state-store-gen-test
  (:require [clojure.test :refer [is deftest]]
            [onyx.state.protocol.db :as s]
            [onyx.compression.nippy :refer [statedb-compress statedb-decompress]]
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

(def gen-group-key (gen/fmap (fn [s] (str "gr-")) (gen/resize 5 gen/string)))
(def gen-value (gen/resize 5 gen/any-printable))

(def windowed-grouped-global-indices #{0 2 4 6})
(def windowed-grouped-sliding-indices #{8 10 12 14})
(def windowed-grouped-session-indices #{16 18 20 22})
(def windowed-ungrouped-global-indices #{24 26 28 30})
(def windowed-ungrouped-sliding-indices #{32 34 36 38})
(def windowed-ungrouped-session-indices #{40 42 44 46})

(def gen-pos-int (gen/fmap #(Math/abs (long %)) gen/int))

(def gen-extent-long
  (gen/one-of [;; reduced range to test extents being modified often
               (gen/resize 50 gen-pos-int) 
               ;; full range to test sorting
               gen-pos-int]))

(def gen-extent-global
  (gen/return 1))

(def time-gen gen-pos-int)

(def gen-global-grouped
  (gen/tuple (gen/elements windowed-grouped-global-indices) 
             (gen/return :windowed-grouped-global)
             gen-group-key
             gen-extent-global
             (gen/return 0)))

(def gen-sliding-grouped
  (gen/tuple (gen/elements windowed-grouped-sliding-indices) 
             (gen/return :windowed-grouped-sliding)
             gen-group-key
             gen-extent-long
             time-gen))

(def gen-session-grouped
  (gen/tuple (gen/elements windowed-grouped-session-indices) 
             (gen/return :windowed-grouped-session)
             gen-group-key
             (gen/tuple gen-extent-long gen-extent-long)
             time-gen))

(def gen-global-ungrouped
  (gen/tuple (gen/elements windowed-ungrouped-global-indices) 
             (gen/return :windowed-ungrouped-global)
             (gen/return nil)
             gen-extent-global
             (gen/return 0)))

(def gen-sliding-ungrouped
  (gen/tuple (gen/elements windowed-ungrouped-sliding-indices) 
             (gen/return :windowed-ungrouped-sliding)
             (gen/return nil)
             gen-extent-long
             time-gen))

(def gen-session-ungrouped
  (gen/tuple (gen/elements windowed-ungrouped-session-indices) 
             (gen/return :windowed-ungrouped-session)
             (gen/return nil)
             (gen/tuple gen-extent-long gen-extent-long)
             time-gen))

(def window-generators
  (gen/one-of [gen-session-grouped 
               gen-session-ungrouped
               gen-global-grouped
               gen-global-ungrouped 
               gen-sliding-grouped 
               gen-sliding-ungrouped]))

(def add-windowed-extent
  (gen/tuple (gen/return :add-extent) 
             window-generators
             gen-value))

(def delete-windowed-extent
  (gen/tuple (gen/return :delete-extent) 
             window-generators))

(def time-entry-value-gen gen/int)

(def add-windowed-time-entry
  (gen/tuple (gen/return :add-time-entry) 
             window-generators
             time-entry-value-gen))

(def delete-windowed-time-entries
  (gen/tuple (gen/return :delete-time-entries) 
             window-generators
             time-gen
             time-gen))

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
                         :entry-idx (inc (* w 500))
                         :type :window
                         :grouped? true 
                         :extent :nil}) 
                windowed-grouped-global-indices)
           (map (fn [w] {:idx w 
                         :entry-idx (inc (* w 500))
                         :type :window
                         :grouped? true 
                         :extent :long}) 
                windowed-grouped-sliding-indices)
           (map (fn [w] {:idx w 
                         :entry-idx (inc (* w 500))
                         :type :window
                         :grouped? true 
                         :extent :long-long}) 
                windowed-grouped-session-indices)
           (map (fn [w] {:idx w 
                         :entry-idx (inc (* w 500))
                         :type :window
                         :grouped? false 
                         :extent :nil}) 
                windowed-ungrouped-global-indices)
           (map (fn [w] {:idx w 
                         :entry-idx (inc (* w 500))
                         :type :window
                         :grouped? false 
                         :extent :long}) 
                windowed-ungrouped-sliding-indices)
           (map (fn [w] {:idx w 
                         :entry-idx (inc (* w 500))
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

(def extent-entry-search-range 
  (gen/fmap sort (gen/tuple gen-pos-int gen-pos-int)))

(deftest state-backend-differences
  (checking "Memory db as oracle for state db"
            (times 200)
            [[values [start-range end-range]] 
             (gen/tuple (gen/vector (gen/one-of [add-windowed-extent
                                                 delete-windowed-extent 
                                                 add-trigger-value 
                                                 add-windowed-time-entry
                                                 delete-windowed-time-entries]))
                        extent-entry-search-range)]
            (let [db-name (str (java.util.UUID/randomUUID))
                  coders (sz-utils/build-coders Short/MIN_VALUE (inc Short/MIN_VALUE) window-serializers trigger-serializers)
                  mem-store (s/create-db {:onyx.peer/state-store-impl :memory} :state-id-1 coders)
                  db-store (s/create-db {:onyx.peer/state-store-impl :lmdb} db-name coders)
                  cp-encoder-db (cpenc/empty-checkpoint-encoder)
                  cp-encoder-mem (cpenc/empty-checkpoint-encoder)
                  db-store-memory->lmdb (s/create-db {:onyx.peer/state-store-impl :lmdb} (str (java.util.UUID/randomUUID)) coders)
                  db-store-lmdb->memory (s/create-db {:onyx.peer/state-store-impl :memory} (str (java.util.UUID/randomUUID)) coders)]
              (try
               ;time 
               (doseq [example values]
                 (case (first example) 
                   :add-time-entry
                   (let [[_ [state-idx window-type group-key extent time] value] example]
                     (s/put-state-entry! db-store state-idx (some->> group-key (s/group-id db-store)) time value)
                     (s/put-state-entry! mem-store state-idx (some->> group-key (s/group-id mem-store)) time value))
                   :delete-time-entries
                   (let [[_ [state-idx wt group-key extent] start end] example
                         [start-range end-range] (if (#{:windowed-grouped-global :windowed-ungrouped-global} wt)
                                                   [Double/NEGATIVE_INFINITY Double/POSITIVE_INFINITY]
                                                   [start end])]
                     (s/delete-state-entries! db-store state-idx (when group-key (s/group-id db-store group-key)) start-range end-range)
                     (s/delete-state-entries! mem-store state-idx (when group-key (s/group-id mem-store group-key)) start-range end-range))
                   :add-extent (let [[_ [state-idx _ group-key extent] value] example] 
                                 (s/put-extent! db-store state-idx (when group-key (s/group-id db-store group-key)) extent value)
                                 (s/put-extent! mem-store state-idx (when group-key (s/group-id mem-store group-key)) extent value))
                   :delete-extent (let [[_ [state-idx _ group-key extent] value] example]
                                    (s/delete-extent! db-store state-idx (when group-key (s/group-id db-store group-key)) extent)
                                    (s/delete-extent! mem-store state-idx (when group-key (s/group-id mem-store group-key)) extent))
                   :add-trigger (let [[_ [state-idx _ group-key] value] example]
                                  (s/put-trigger! db-store state-idx (when group-key (s/group-id db-store group-key)) value)
                                  (s/put-trigger! mem-store state-idx (when group-key (s/group-id mem-store group-key)) value))))
               ;; Export and test re-import
               (s/export db-store cp-encoder-db)
               (s/export mem-store cp-encoder-mem)

               (is (= (cpenc/length cp-encoder-db)
                      (cpenc/length cp-encoder-mem)))

               ;; test restore size from mem store to db store
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

               (is (= @(.group-counter ^onyx.state.lmdb.StateBackend db-store)
                      @(.group-counter ^onyx.state.memory.StateBackend mem-store)
                      @(.group-counter ^onyx.state.lmdb.StateBackend db-store-memory->lmdb)
                      @(.group-counter ^onyx.state.memory.StateBackend db-store-lmdb->memory)))

               (is (= @(.entry-counter ^onyx.state.lmdb.StateBackend db-store)
                      @(.entry-counter ^onyx.state.memory.StateBackend mem-store)
                      @(.entry-counter ^onyx.state.lmdb.StateBackend db-store-memory->lmdb)
                      @(.entry-counter ^onyx.state.memory.StateBackend db-store-lmdb->memory)))

               (is (= (sort (map second (s/groups db-store)))
                      (sort (map second (s/groups mem-store)))
                      (sort (map second (s/groups db-store-memory->lmdb)))
                      (sort (map second (s/groups db-store-lmdb->memory)))))

               (doseq [[state-idx group-key] (->> values
                                                  (filter (fn [[type]]
                                                            (not (#{:add-trigger
                                                                    :add-time-entry
                                                                    :delete-time-entries} type))))
                                                  (map (fn [[_ [state-idx _ group-key]]] 
                                                         [state-idx group-key]))
                                                  (distinct))]
                 (let [serialized-group (some->> group-key (s/group-id db-store))
                       serialized-group-mem-store (some->> group-key (s/group-id mem-store))]
                   (is (= (s/get-trigger mem-store state-idx serialized-group-mem-store)
                          (s/get-trigger db-store state-idx serialized-group)))

                   (is (= (s/get-trigger db-store state-idx serialized-group)
                          (s/get-trigger db-store-memory->lmdb state-idx serialized-group)
                          (s/get-trigger db-store-lmdb->memory state-idx serialized-group)))

                   (is (= (s/group-extents mem-store state-idx serialized-group-mem-store)
                          (sort (s/group-extents mem-store state-idx serialized-group-mem-store))))

                   (is (= (s/group-extents db-store state-idx serialized-group)
                          (s/group-extents mem-store state-idx serialized-group-mem-store))
                       [(s/group-extents db-store state-idx serialized-group)
                        (s/group-extents mem-store state-idx serialized-group-mem-store)])

                   (is (= (s/group-extents db-store state-idx serialized-group)
                          (s/group-extents db-store-memory->lmdb state-idx serialized-group)
                          (s/group-extents db-store-lmdb->memory state-idx serialized-group-mem-store))

                       [(s/group-extents db-store state-idx serialized-group)
                        (s/group-extents db-store-memory->lmdb state-idx serialized-group)
                        (s/group-extents db-store-lmdb->memory state-idx serialized-group-mem-store)]
                       
                       )

                   ;; take only first value of trigger keys as the group key will differ due to serialization
                   (is (= (sort (map first (s/trigger-keys db-store state-idx)))
                          (sort (map first (s/trigger-keys mem-store state-idx))))
                       [(s/trigger-keys db-store state-idx)
                        (s/trigger-keys mem-store state-idx)])))

               (doseq [[state-idx group-key wt] (->> values
                                                  (filter (fn [[type]]
                                                            (#{:add-time-entry} type)))
                                                  (map (fn [[_ [state-idx wt group-key]]] 
                                                         [state-idx group-key wt]))
                                                  (distinct))]
                 (let [serialized-group (some->> group-key (s/group-id db-store))
                       serialized-group-mem-store (some->> group-key (s/group-id mem-store))
                       [start-range end-range] (if (#{:windowed-grouped-global :windowed-ungrouped-global} wt)
                                                 [Double/NEGATIVE_INFINITY Double/POSITIVE_INFINITY]
                                                 [start-range end-range])]
                   (is (= (s/get-state-entries db-store state-idx serialized-group start-range end-range)
                          (s/get-state-entries mem-store state-idx serialized-group-mem-store start-range end-range))

                       [(s/get-state-entries db-store state-idx serialized-group start-range end-range)
                        (s/get-state-entries mem-store state-idx serialized-group-mem-store start-range end-range)])
                   
                   (is (= (s/get-state-entries db-store state-idx serialized-group start-range end-range)
                          (s/get-state-entries db-store-memory->lmdb state-idx serialized-group start-range end-range)
                          #_(s/get-state-entries db-store-lmdb->memory state-idx serialized-group-mem-store start-range end-range))
                       "Testing equality of restored state entries")))

               (finally 
                (s/drop! db-store)
                (s/close! db-store)
                (s/drop! db-store-memory->lmdb)
                (s/close! db-store-memory->lmdb)
                (s/drop! db-store-lmdb->memory)
                (s/close! db-store-lmdb->memory)
                (s/drop! mem-store)
                (s/close! mem-store))))))
