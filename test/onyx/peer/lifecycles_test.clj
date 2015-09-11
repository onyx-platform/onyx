(ns onyx.peer.lifecycles-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config]]
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
        env (onyx.api/start-env env-config)
        peer-group (onyx.api/start-peer-group peer-config)
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


        v-peers (onyx.api/start-peers 3 peer-group)

        _ (onyx.api/submit-job
            peer-config
            {:catalog catalog
             :workflow workflow
             :lifecycles lifecycles
             :task-scheduler :onyx.task-scheduler/balanced})

        _ (doseq [n (range n-messages)]
            (>!! in-chan {:n n}))

        _ (>!! in-chan :done)
        _ (close! in-chan)

        results (take-segments! out-chan)


        ]


    (let [expected (set (map (fn [x] {:n (inc x)}) (range n-messages)))]
      (is (= (set (butlast results)) expected))
      (is (= (last results) :done)))

    ;; shutdown-peer ensure peers are fully shutdown so that
    ;; :task-after will have been set
    (doseq [v-peer v-peers]
      (onyx.api/shutdown-peer v-peer))

    (is (= @call-log [:task-started
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
                      :task-after]))
    (is (= @started-task-counter 3))

    (onyx.api/shutdown-peer-group peer-group)

    (onyx.api/shutdown-env env)))


