(ns onyx.peer.lifecycles-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config with-test-env]]
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

(defn after-batch [event lifecycle]
  (swap! call-log (fn [call-log] (conj call-log :batch-after)))
  {})

(defn signal-started [event lifecycle]
  (swap! started-task-counter inc)
  {})

(def in-chan (chan (inc n-messages)))

(def out-chan (chan (sliding-buffer (inc n-messages))))

(defn inject-in-ch [event lifecycle]
  {:core.async/chan in-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan out-chan})

(def calls
  {:lifecycle/start-task? start-task?
   :lifecycle/before-task-start before-task-start
   :lifecycle/before-batch before-batch
   :lifecycle/after-batch after-batch
   :lifecycle/after-task-stop after-task-stop})

(def all-calls
  {:lifecycle/before-task-start signal-started})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(deftest lifecycles-test
  (let [id (java.util.UUID/randomUUID)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/id id)
        peer-config (assoc (:peer-config config) :onyx/id id)
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
                    {:lifecycle/task :in
                     :lifecycle/calls :onyx.plugin.core-async/reader-calls}
                    {:lifecycle/task :inc
                     :lifecycle/calls :onyx.peer.lifecycles-test/calls
                     :lifecycle/doc "Test lifecycles that increment a counter in an atom"}
                    {:lifecycle/task :out
                     :lifecycle/calls :onyx.peer.lifecycles-test/out-calls}
                    {:lifecycle/task :out
                     :lifecycle/calls :onyx.plugin.core-async/writer-calls}
                    {:lifecycle/task :all
                     :lifecycle/calls :onyx.peer.lifecycles-test/all-calls}]


    ]


    (with-test-env [test-env [3 env-config peer-config]]
      (onyx.api/submit-job peer-config
                           {:catalog catalog
                            :workflow workflow
                            :lifecycles lifecycles
                            :task-scheduler :onyx.task-scheduler/balanced})

      (doseq [n (range n-messages)]
        (>!! in-chan {:n n}))

      (>!! in-chan :done)
      (close! in-chan)

      (let [results (take-segments! out-chan)
            expected (set (map (fn [x] {:n (inc x)}) (range n-messages)))]
        (is (= expected (set (butlast results))))
        (is (= :done (last results)))))

    ;; after shutdown-peer ensure peers are fully shutdown so that
    ;; :task-after will have been set
    (is (= [:task-started
            :task-before
            :batch-before
            :batch-after ; 1
            :batch-before
            :batch-after ; 2
            :batch-before
            :batch-after ; 3
            :batch-before
            :batch-after ; 4
            :batch-before
            :batch-after ; 5
            :batch-before
            :batch-after
            :task-after] 
           @call-log))
    (is (= 3 @started-task-counter))))
