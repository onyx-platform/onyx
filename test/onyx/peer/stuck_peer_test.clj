(ns onyx.peer.stuck-peer-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config with-test-env add-test-env-peers! feedback-exception!]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.job :refer [add-task]]
            [onyx.api]))

(def n-messages 4000)

(def out-chan (atom nil))

(defn inject-out-ch [event lifecycle]
  {:core.async/chan @out-chan})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})


(def publisher-liveness-timeout 3000)

(defn my-inc [{:keys [n] :as segment}]
  (when (zero? (rand-int (/ n-messages 4)))
    (println "Sleeping for " publisher-liveness-timeout "to cause timeout")
    (Thread/sleep (+ 10 publisher-liveness-timeout)))
  (assoc segment :n (inc n)))

(deftest ^:smoke min-peers-test
  (let [id (random-uuid)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id id)
        peer-config (assoc (:peer-config config) 
                           :onyx/tenancy-id id
                           :onyx.peer/publisher-liveness-timeout-ms publisher-liveness-timeout)]
    (with-test-env [test-env [4 env-config peer-config]]
      (let [batch-size 20
            catalog [{:onyx/name :inc
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
            lifecycles [{:lifecycle/task :out
                         :lifecycle/calls ::out-calls}]
            input (map (fn [n]
                         {:n n})
                       (range n-messages))
            job (-> {:catalog catalog
                     :workflow workflow
                     :lifecycles lifecycles
                     :task-scheduler :onyx.task-scheduler/balanced
                     :metadata {:job-name :click-stream}}
                    (add-task (onyx.tasks.seq/input-serialized :in 
                                                               {:onyx/batch-size batch-size
                                                                :onyx/n-peers 1} 
                                                               input)))
            n-out-size 1000000
            _ (reset! out-chan (chan n-out-size))
            job-sub (onyx.api/submit-job peer-config job)
            _ (feedback-exception! peer-config (:job-id job-sub))
            results (take-segments! @out-chan 5000)]
        (let [expected (set (map (fn [x] {:n (inc x)}) (range n-messages)))]
          (is (= expected (set results))))))))
