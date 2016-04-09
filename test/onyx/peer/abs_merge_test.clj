(ns onyx.peer.abs-merge-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config with-test-env add-test-env-peers!]]
            [onyx.api]))

(def n-messages 100)

(def in-chan-1 (atom nil))

(def in-chan-2 (atom nil))

(def out-chan (atom nil))

(defn inject-in-ch-1 [event lifecycle]
  {:core.async/chan @in-chan-1})

(defn inject-in-ch-2 [event lifecycle]
  {:core.async/chan @in-chan-2})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan @out-chan})

(def in-calls-1
  {:lifecycle/before-task-start inject-in-ch-1})

(def in-calls-2
  {:lifecycle/before-task-start inject-in-ch-2})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(deftest abs-plugin-test
  (let [id (java.util.UUID/randomUUID)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id id)
        peer-config (assoc (:peer-config config) :onyx/tenancy-id id)]
    (with-test-env [test-env [7 env-config peer-config]]
      (let [batch-size 20
            catalog [{:onyx/name :in-1
                      :onyx/plugin :onyx.plugin.core-async/input
                      :onyx/type :input
                      :onyx/medium :core.async
                      :onyx/batch-size batch-size
                      :onyx/max-peers 1
                      :onyx/doc "Reads segments from a core.async channel"}

                     {:onyx/name :in-2
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
            workflow [[:in-1 :inc]
                      [:in-2 :inc]
                      [:inc :out]]
            lifecycles [{:lifecycle/task :in-1
                         :lifecycle/calls ::in-calls-1}

                        {:lifecycle/task :in-2
                         :lifecycle/calls ::in-calls-2}

                        {:lifecycle/task :out
                         :lifecycle/calls ::out-calls}]
            _ (reset! in-chan-1 (chan (inc n-messages)))
            _ (reset! in-chan-2 (chan (inc n-messages)))
            _ (reset! out-chan (chan (sliding-buffer (inc (* 2 n-messages)))))
            _ (doseq [n (range n-messages)]
                (>!! @in-chan-1 {:n n}))
            _ (doseq [n (range n-messages (* 2 n-messages))]
                (>!! @in-chan-2 {:n n}))
            _ (close! @in-chan-1)
            _ (close! @in-chan-2)
            _ (onyx.api/submit-job peer-config
                                   {:catalog catalog
                                    :workflow workflow
                                    :lifecycles lifecycles
                                    :task-scheduler :onyx.task-scheduler/balanced})
            results (take-segments! @out-chan)]
        (let [expected (sort-by :n (map (fn [x] {:n (inc x)}) (range (* 2 n-messages))))]
          (is (= expected (sort-by :n (butlast results)))))))))
