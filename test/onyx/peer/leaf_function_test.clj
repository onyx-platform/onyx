(ns onyx.peer.leaf-function-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config with-test-env]]
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

(def results (atom []))

(defn add-to-results [segment]
  (swap! results conj segment))

(deftest leaf-function-test
  (let [id (random-uuid)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id id)
        peer-config (assoc (:peer-config config) :onyx/tenancy-id id)
        batch-size 20
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
                  :onyx/fn ::add-to-results
                  :onyx/plugin :onyx.peer.function/function
                  :onyx/medium :function
                  :onyx/type :output
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1}]
        workflow [[:in :inc] [:inc :out]]
        lifecycles [{:lifecycle/task :in
                     :lifecycle/calls ::in-calls}
                    {:lifecycle/task :out
                     :lifecycle/calls ::out-calls}]]
    (reset! in-chan (chan (inc n-messages)))
    (reset! in-buffer {})
    (reset! out-chan (chan (sliding-buffer (inc n-messages))))

    (with-test-env [test-env [3 env-config peer-config]]
        (doseq [n (range n-messages)]
            (>!! @in-chan {:n n}))
        (close! @in-chan)
        (let [{:keys [job-id]} (onyx.api/submit-job peer-config
                                                    {:catalog catalog
                                                     :workflow workflow
                                                     :lifecycles lifecycles
                                                     :task-scheduler :onyx.task-scheduler/balanced})
              _ (while (not= n-messages (count @results))
                  (println "Waiting for enough results" @results)
                  (Thread/sleep 500))
              expected (set (map (fn [x] {:n (inc x)}) (range n-messages)))]
          (is (= expected (set @results))))))) 
