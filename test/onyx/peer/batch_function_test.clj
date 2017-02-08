(ns onyx.peer.batch-function-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config with-test-env add-test-env-peers!]]
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

(defn my-inc-batch [segments]
  (map (fn [{:keys [n] :as segment}]
         (cond-> segment 
           true (assoc :n (inc n))
           (zero? (rand-int 2)) 
           ;; Sometimes stick it in a vector to make sure we unroll
           (vector)))
       segments))

(deftest ^:smoke batch-function-test
  (let [id (java.util.UUID/randomUUID)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id id)
        peer-config (assoc (:peer-config config) :onyx/tenancy-id id)]
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
                      :onyx/fn ::my-inc-batch
                      :onyx/batch-fn? true
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
            lifecycles [{:lifecycle/task :in :lifecycle/calls ::in-calls}
                        {:lifecycle/task :out :lifecycle/calls ::out-calls}]
            _ (reset! in-chan (chan (inc n-messages)))
            _ (reset! in-buffer {})
            _ (reset! out-chan (chan (sliding-buffer (inc n-messages))))
            _ (doseq [n (range n-messages)]
                (>!! @in-chan {:n n}))
            _ (close! @in-chan)
            _ (->> {:catalog catalog
                    :workflow workflow
                    :lifecycles lifecycles
                    :task-scheduler :onyx.task-scheduler/balanced
                    :metadata {:job-name :click-stream}}
                   (onyx.api/submit-job peer-config)
                   (:job-id)
                   (onyx.test-helper/feedback-exception! peer-config))]
        (let [results (take-segments! @out-chan 50)
              expected (set (map (fn [x] {:n (inc x)}) (range n-messages)))]
          (is (= expected (set results))))))))
