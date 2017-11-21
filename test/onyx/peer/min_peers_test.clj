(ns onyx.peer.min-peers-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [onyx.peer.task-lifecycle]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.peer.coordinator :as coordinator]
            [org.senatehouse.expect-call :as expect-call :refer [with-expect-call]]
            [onyx.test-helper :refer [load-config with-test-env add-test-env-peers!]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.api]))

(def n-messages 100)

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

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(deftest ^:smoke min-peers-test
  (let [id (random-uuid)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id id)
        peer-config (assoc (:peer-config config) 
                           :onyx/tenancy-id id
                           :onyx.peer/coordinator-barrier-period-ms 50)]
    (with-expect-call [(:do coordinator/periodic-barrier [_])
                       (:do :more coordinator/periodic-barrier [_])]
      (with-test-env [test-env [3 env-config peer-config]]
        (let [batch-size 20
              catalog [{:onyx/name :in
                        :onyx/plugin :onyx.plugin.core-async/input
                        :onyx/type :input
                        :onyx/medium :core.async
                        :onyx/batch-size 10
                        :onyx/max-peers 1
                        :onyx/doc "Reads segments from a core.async channel"}

                       {:onyx/name :inc
                        :onyx/fn :onyx.peer.min-peers-test/my-inc
                        :onyx/batch-size 9
                        :onyx/type :function}

                       {:onyx/name :out
                        :onyx/plugin :onyx.plugin.core-async/output
                        :onyx/type :output
                        :onyx/medium :core.async
                        :onyx/batch-size 8
                        :onyx/max-peers 10
                        :onyx/doc "Writes segments to a core.async channel"}]
              workflow [[:in :inc] [:inc :out]]
              lifecycles [{:lifecycle/task :in
                           :lifecycle/calls ::in-calls}
                          {:lifecycle/task :out
                           :lifecycle/calls ::out-calls}]
              _ (reset! in-chan (chan (inc n-messages)))
              _ (reset! in-buffer {})
              _ (reset! out-chan (chan 100000))
              _ (doseq [n (range n-messages)]
                  (>!! @in-chan {:n n}))
              _ (close! @in-chan)
              {:keys [job-id]} (onyx.api/submit-job peer-config
                                                    {:catalog catalog
                                                     :workflow workflow
                                                     :lifecycles lifecycles
                                                     :task-scheduler :onyx.task-scheduler/balanced
                                                     :metadata {:job-name :click-stream}})
              _ (onyx.test-helper/feedback-exception! peer-config job-id)
              results (take-segments! @out-chan 1)]
          (is (empty? @in-buffer))
          (let [expected (map (fn [x] {:n (inc x)}) (range n-messages))]
            (is (= expected results))))))))
