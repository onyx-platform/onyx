(ns onyx.peer.idempotent-job-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config with-test-env add-test-env-peers!]]
            [onyx.api]))

(def n-messages 100)

(def in-chan (atom nil))

(def out-chan (atom nil))

(defn inject-in-ch [event lifecycle]
  {:core.async/chan @in-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan @out-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(deftest idempotent-job-test
  (let [tenancy-id (java.util.UUID/randomUUID)
        job-id :my-custom-job-id
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id tenancy-id)
        peer-config (assoc (:peer-config config) :onyx/tenancy-id tenancy-id)]
    (with-test-env [test-env [3 env-config peer-config]]
      (let [batch-size 20
            catalog [{:onyx/name :in
                      :onyx/plugin :onyx.plugin.core-async/input
                      :onyx/type :input
                      :onyx/medium :core.async
                      :onyx/batch-size batch-size
                      :onyx/max-peers 1
                      :onyx/doc "Reads segments from a core.async channel"}

                     {:onyx/name :inc
                      :onyx/fn ::my-inc
                      :onyx/type :function
                      :onyx/batch-size batch-size}

                     {:onyx/name :out
                      :onyx/plugin :onyx.plugin.core-async/output
                      :onyx/type :output
                      :onyx/medium :core.async
                      :onyx/batch-size batch-size
                      :onyx/max-peers 1
                      :onyx/doc "Writes segments to a core.async channel"}]
            workflow [[:in :inc] [:inc :out]]
            lifecycles [{:lifecycle/task :in
                         :lifecycle/calls ::in-calls}
                        {:lifecycle/task :in
                         :lifecycle/calls :onyx.plugin.core-async/reader-calls}
                        {:lifecycle/task :out
                         :lifecycle/calls ::out-calls}
                        {:lifecycle/task :out
                         :lifecycle/calls :onyx.plugin.core-async/writer-calls}]
            _ (reset! in-chan (chan (inc n-messages)))
            _ (reset! out-chan (chan (sliding-buffer (inc n-messages))))
            _ (doseq [n (range n-messages)]
                (>!! @in-chan {:n n}))
            _ (>!! @in-chan :done)
            _ (close! @in-chan)
            job {:catalog catalog
                 :workflow workflow
                 :lifecycles lifecycles
                 :task-scheduler :onyx.task-scheduler/balanced
                 :metadata {:job-id job-id}}
            rets (map deref (map (fn [_] (future (onyx.api/submit-job peer-config job))) (range 10)))]
        (is (apply = rets))
        (let [results (take-segments! @out-chan)]
          (let [expected (set (map (fn [x] {:n (inc x)}) (range n-messages)))]
            (is (= expected (set (butlast results))))
            (is (= :done (last results)))))))))
