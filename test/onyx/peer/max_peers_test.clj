(ns onyx.peer.max-peers-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config with-test-env]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.api]))

(def n-messages 15000)

(def in-chan (atom nil))
(def in-buffer-1 (atom nil))

(def out-chan (atom nil))

(defn inject-in-ch [event lifecycle]
  {:core.async/buffer in-buffer-1
   :core.async/chan @in-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan @out-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(deftest max-peers-test
  (let [id (random-uuid)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id id)
        peer-config (assoc (:peer-config config) :onyx/tenancy-id id)
        batch-size 40
        catalog [{:onyx/name :in
                  :onyx/plugin :onyx.plugin.core-async/input
                  :onyx/type :input
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1
                  :onyx/doc "Reads segments from a core.async channel"}

                 {:onyx/name :inc
                  :onyx/fn :onyx.peer.max-peers-test/my-inc
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
        workflow [[:in :inc] [:inc :out]]
        lifecycles [{:lifecycle/task :in
                     :lifecycle/calls :onyx.peer.max-peers-test/in-calls}
                    {:lifecycle/task :out
                     :lifecycle/calls :onyx.peer.max-peers-test/out-calls}]]

    (reset! in-chan (chan (inc n-messages)))
    (reset! in-buffer-1 {})
    (reset! out-chan (chan (sliding-buffer (inc n-messages))))    

    (with-test-env [test-env [8 env-config peer-config]]
      (doseq [n (range n-messages)]
          (>!! @in-chan {:n n}))
      (close! @in-chan)

      (->> (onyx.api/submit-job
            peer-config
            {:catalog catalog :workflow workflow
             :lifecycles lifecycles
             :task-scheduler :onyx.task-scheduler/balanced})
           (:job-id)
           (onyx.test-helper/feedback-exception! peer-config) )

      ;;;;;;;;;;;;;;;;;;;; TODO: Verify only 3 peers were actually used ;;;;;;;;;;;;;;;;;;;;;;;;;;;;
      (let [results (take-segments! @out-chan 50)
            expected (set (map (fn [x] {:n (inc x)}) (range n-messages)))]
        (is (= expected (set results)))))))
