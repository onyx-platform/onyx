(ns onyx.peer.lifecycles-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config with-test-env feedback-exception!]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.api]))

(def n-messages 100)

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(def call-log (atom []))
(def started-task-counter (atom 0))

(defn start-task? [event lifecycle]
  (swap! call-log (fn [call-log] (conj call-log :task-started)))
  true)

(defn before-task-start [event lifecycle]
  (swap! call-log (fn [call-log] (conj call-log :task-before)))
  {})

(defn after-task-stop [event lifecycle]
  (swap! call-log (fn [call-log] (conj call-log :task-after)))
  {})

(defn before-batch [event lifecycle]
  (swap! call-log (fn [call-log] (conj call-log :batch-before)))
  {})

(defn after-read-batch [event lifecycle]
  (swap! call-log (fn [call-log] (conj call-log :batch-after-read)))
  {})

(defn after-batch [event lifecycle]
  (swap! call-log (fn [call-log] (conj call-log :batch-after)))
  {})

(defn signal-started [event lifecycle]
  (swap! started-task-counter inc)
  {})

(def in-chan (atom nil))
(def in-buffer-1 (atom nil))

(def out-chan (atom nil))

(defn inject-in-ch [event lifecycle]
  {:core.async/buffer in-buffer-1
   :core.async/chan @in-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan @out-chan})

(def calls
  {:lifecycle/start-task? start-task?
   :lifecycle/before-task-start before-task-start
   :lifecycle/before-batch before-batch
   :lifecycle/after-read-batch after-read-batch
   :lifecycle/after-batch after-batch
   :lifecycle/after-task-stop after-task-stop})

(def all-calls
  {:lifecycle/before-task-start signal-started})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(deftest ^:smoke lifecycles-test
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
                  :onyx/fn :onyx.peer.lifecycles-test/my-inc
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
                     :lifecycle/calls :onyx.peer.lifecycles-test/in-calls}
                    {:lifecycle/task :inc
                     :lifecycle/calls :onyx.peer.lifecycles-test/calls
                     :lifecycle/doc "Test lifecycles that increment a counter in an atom"}
                    {:lifecycle/task :out
                     :lifecycle/calls :onyx.peer.lifecycles-test/out-calls}
                    {:lifecycle/task :all
                     :lifecycle/calls :onyx.peer.lifecycles-test/all-calls}]]

    (reset! in-chan (chan (inc n-messages)))
    (reset! in-buffer-1 {})
    (reset! out-chan (chan (sliding-buffer (inc n-messages))))

    (with-test-env [test-env [3 env-config peer-config]]
      (doseq [n (range n-messages)]
        (>!! @in-chan {:n n}))

      (close! @in-chan)

      (->> {:catalog catalog
            :workflow workflow
            :lifecycles lifecycles
            :task-scheduler :onyx.task-scheduler/balanced}
           (onyx.api/submit-job peer-config)
           :job-id
           (feedback-exception! peer-config))

      (let [results (take-segments! @out-chan 50)
            expected (set (map (fn [x] {:n (inc x)}) (range n-messages)))]
        (is (= expected (set results)))))

    ;; after shutdown-peer ensure peers are fully shutdown so that
    ;; :task-after will have been set

    (let [calls @call-log
          repeated-calls (drop 2 (butlast calls))]
      (is (= [:task-started :task-before] (take 2 calls)))
      (is (= :task-after (last calls)))
      (is (every? (partial = [:batch-before :batch-after-read :batch-after])
                  (partition 3 repeated-calls)))
      (is (= 3 @started-task-counter)))))
