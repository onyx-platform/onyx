(ns onyx.windowing.window-watermark-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.test-helper :refer [load-config with-test-env add-test-env-peers!]]
            [onyx.api]))

(def expected-windows
  [[1442113200000 1442113499999 3]
   [1442113500000 1442113799999 5]
   [1442114100000 1442114399999 2]
   [1442114700000 1442114999999 1]
   [1442115000000 1442115299999 1]
   [1442115900000 1442116199999 1]
   [1442116500000 1442116799999 2]])

(def test-state (atom []))

(defn update-atom! [event window trigger {:keys [lower-bound upper-bound event-type] :as opts} extent-state]
  (swap! test-state conj [lower-bound upper-bound extent-state])) 

(def in-chan (atom nil))
(def in-buffer (atom nil))

(def out-chan (atom nil))

(defn inject-in-ch [event lifecycle]
  {:core.async/buffer in-buffer
   :core.async/chan @in-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan @out-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(defn watermark-fn [w] 
  (.getTime ^java.util.Date (:event-time w)))

(deftest window-watermark-test
  (let [id (random-uuid)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id id 
                          :onyx.peer/coordinator-barrier-period-ms 300)
        peer-config (assoc (:peer-config config) :onyx/tenancy-id id)
        batch-size 20
        workflow
        [[:in :identity] [:identity :out]]

        catalog
        [{:onyx/name :in
          :onyx/plugin :onyx.plugin.core-async/input
          :onyx/type :input
          :onyx/assign-watermark-fn ::watermark-fn
          :onyx/medium :core.async
          :onyx/batch-size batch-size
          :onyx/max-peers 1
          :onyx/doc "Reads segments from a core.async channel"}

         {:onyx/name :identity
          :onyx/fn :clojure.core/identity
          :onyx/type :function
          :onyx/max-peers 1
          :onyx/batch-size batch-size}

         {:onyx/name :out
          :onyx/plugin :onyx.plugin.core-async/output
          :onyx/type :output
          :onyx/medium :core.async
          :onyx/batch-size batch-size
          :onyx/max-peers 1
          :onyx/doc "Writes segments to a core.async channel"}]

        windows
        [{:window/id :collect-segments
          :window/task :identity
          :window/type :fixed
          :window/aggregation :onyx.windowing.aggregation/count
          :window/window-key :event-time
          :window/range [5 :minutes]}]

        triggers
        [{:trigger/window-id :collect-segments
          :trigger/id :sync
          :trigger/on :onyx.triggers/watermark
          :trigger/post-evictor [:all]
          :trigger/state-context [:window-state]
          :trigger/sync ::update-atom!}]

        lifecycles
        [{:lifecycle/task :in
          :lifecycle/calls ::in-calls}
         {:lifecycle/task :out
          :lifecycle/calls ::out-calls}]]

    (reset! in-chan (chan 10000))
    (reset! in-buffer {})
    (reset! out-chan (chan 10000))
    (reset! test-state [])

    (with-test-env [test-env [3 env-config peer-config]]
      (let [{:keys [job-id]} (onyx.api/submit-job peer-config
                                                  {:catalog catalog
                                                   :workflow workflow
                                                   :lifecycles lifecycles
                                                   :windows windows
                                                   :triggers triggers
                                                   :task-scheduler :onyx.task-scheduler/balanced})
            ;; TODO, instead of polls, check next epoch or something like that?
            _ (Thread/sleep 2000)
            _ (>!! @in-chan {:id 1, :age 21, :event-time #inst "2015-09-13T03:00:00.829-00:00"})
            _ (>!! @in-chan {:id 11, :age 15, :event-time #inst "2015-09-13T03:03:00.829-00:00"})
            _ (>!! @in-chan {:id 2, :age 12, :event-time #inst "2015-09-13T03:04:00.829-00:00"}) 
            _ (Thread/sleep 2000)
            _ (is (= @test-state []))
            _ (>!! @in-chan {:id 3, :age 3, :event-time #inst "2015-09-13T03:05:00.829-00:00"}) 
            _ (Thread/sleep 2000)
            _ (is (= @test-state [[1442113200000 1442113499999 3]]))
            _ (>!! @in-chan {:id 4, :age 64, :event-time #inst "2015-09-13T03:06:00.829-00:00"}) 
            _ (>!! @in-chan {:id 5, :age 53, :event-time #inst "2015-09-13T03:07:00.829-00:00"}) 
            _ (>!! @in-chan {:id 6, :age 52, :event-time #inst "2015-09-13T03:08:00.829-00:00"})
            _ (>!! @in-chan {:id 7, :age 24, :event-time #inst "2015-09-13T03:09:00.829-00:00"})
            _ (Thread/sleep 2000)
            _ (is (= @test-state [[1442113200000 1442113499999 3]]))
            _ (>!! @in-chan {:id 8, :age 35, :event-time #inst "2015-09-13T03:15:00.829-00:00"})
            _ (Thread/sleep 2000)
            _ (is (= [[1442113200000 1442113499999 3]
                      [1442113500000 1442113799999 5]] 
                     @test-state))
            _ (>!! @in-chan {:id 15, :age 35, :event-time #inst "2015-09-13T03:16:00.829-00:00"})
            _ (>!! @in-chan {:id 9, :age 49, :event-time #inst "2015-09-13T03:25:00.829-00:00"})
            _ (Thread/sleep 2000)
            _ (is (= @test-state [[1442113200000 1442113499999 3]
                                  [1442113500000 1442113799999 5]
                                  [1442114100000 1442114399999 2]]))
            _ (>!! @in-chan {:id 14, :age 60, :event-time #inst "2015-09-13T03:32:00.829-00:00"})
            _ (Thread/sleep 2000)
            _ (is (= @test-state [[1442113200000 1442113499999 3]
                                  [1442113500000 1442113799999 5]
                                  [1442114100000 1442114399999 2]
                                  [1442114700000 1442114999999 1]]))
            _ (>!! @in-chan {:id 10, :age 37, :event-time #inst "2015-09-13T03:45:00.829-00:00"})
            _ (Thread/sleep 2000)
            _ (is (= @test-state [[1442113200000 1442113499999 3]
                                  [1442113500000 1442113799999 5]
                                  [1442114100000 1442114399999 2]
                                  [1442114700000 1442114999999 1]
                                  [1442115000000 1442115299999 1]]))
            _ (>!! @in-chan {:id 12, :age 22, :event-time #inst "2015-09-13T03:56:00.829-00:00"})
            _ (Thread/sleep 2000)
            _ (is (= @test-state [[1442113200000 1442113499999 3]
                                  [1442113500000 1442113799999 5]
                                  [1442114100000 1442114399999 2]
                                  [1442114700000 1442114999999 1]
                                  [1442115000000 1442115299999 1]
                                  [1442115900000 1442116199999 1]]))
            _ (>!! @in-chan {:id 13, :age 83, :event-time #inst "2015-09-13T03:59:00.829-00:00"})
            _ (Thread/sleep 2000)
            ;; Final window will only be flushed by job completion action
            _ (is (= @test-state [[1442113200000 1442113499999 3]
                                  [1442113500000 1442113799999 5]
                                  [1442114100000 1442114399999 2]
                                  [1442114700000 1442114999999 1]
                                  [1442115000000 1442115299999 1]
                                  [1442115900000 1442116199999 1]]))
            _ (close! @in-chan)
            _ (Thread/sleep 2000)
            ;; Final window will only be flushed by job completion action
            _ (is (= @test-state [[1442113200000 1442113499999 3]
                                  [1442113500000 1442113799999 5]
                                  [1442114100000 1442114399999 2]
                                  [1442114700000 1442114999999 1]
                                  [1442115000000 1442115299999 1]
                                  [1442115900000 1442116199999 1]
                                  [1442116500000 1442116799999 2]]))
            _ (onyx.test-helper/feedback-exception! peer-config job-id)
            results (take-segments! @out-chan 50)]
        (is (= expected-windows @test-state))))))
