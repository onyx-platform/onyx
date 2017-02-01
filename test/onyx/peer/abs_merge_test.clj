(ns onyx.peer.abs-merge-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [onyx.plugin.seq]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config with-test-env add-test-env-peers!]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.api]))

(def n-messages 100)

(def out-chan (atom nil))

(defn inject-out-ch [event lifecycle]
  {:core.async/chan @out-chan})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(deftest abs-merge-test
  (let [id (random-uuid)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id id)
        peer-config (assoc (:peer-config config) :onyx/tenancy-id id)]
    (with-test-env [test-env [8 env-config peer-config]]
      (let [batch-size 20
            catalog [{:onyx/name :in-1
                      :onyx/plugin :onyx.plugin.seq/input
                      :onyx/type :input
                      :onyx/medium :seq
                      :onyx/batch-size batch-size
                      :onyx/max-peers 1
                      :onyx/doc "Reads segments from a seq"}

                     {:onyx/name :in-2
                      :onyx/plugin :onyx.plugin.seq/input
                      :onyx/type :input
                      :onyx/medium :seq
                      :onyx/batch-size batch-size
                      :onyx/max-peers 1
                      :onyx/doc "Reads segments from a seq"}

                     {:onyx/name :inc
                      :onyx/fn ::my-inc
                      :onyx/type :function
                      ;:onyx/max-peers 4
                      :onyx/batch-size batch-size}

                     {:onyx/name :out
                      :onyx/plugin :onyx.plugin.core-async/output
                      :onyx/type :output
                      :onyx/medium :core.async
                      :onyx/batch-size batch-size
                      :onyx/max-peers 1
                      :onyx/doc "Writes segments to a core.async channel"}]
            workflow [[:in-1 :inc]
                      [:in-2 :inc]
                      [:inc :out]]
            lifecycles [{:lifecycle/task :in-1
                         :seq/sequential (map (fn [v] {:n v}) (range n-messages))
                         :lifecycle/calls :onyx.plugin.seq/inject-seq-via-lifecycle}

                        {:lifecycle/task :in-2
                         :seq/sequential (map (fn [v] {:n v}) 
                                              (range n-messages (* 2 n-messages)))
                         :lifecycle/calls :onyx.plugin.seq/inject-seq-via-lifecycle}

                        {:lifecycle/task :out
                         :lifecycle/calls ::out-calls}]
            _ (reset! out-chan (chan (sliding-buffer (inc (* 2 n-messages)))))
            {:keys [job-id]} (onyx.api/submit-job peer-config
                                   {:catalog catalog
                                    :workflow workflow
                                    :lifecycles lifecycles
                                    :task-scheduler :onyx.task-scheduler/balanced})
            _ (onyx.test-helper/feedback-exception! peer-config job-id)
            results (take-segments! @out-chan 50)]
        (let [expected (sort-by :n (map (fn [x] {:n (inc x)}) (range (* 2 n-messages))))]
          (is (= expected (sort-by :n results))))))))
