(ns onyx.peer.bulk-function-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config]]
            [onyx.api]))

(def n-messages 100)

(def in-chan (chan (inc n-messages)))

(def out-chan (chan (sliding-buffer (inc n-messages))))

(defn inject-in-ch [event lifecycle]
  {:core.async/chan in-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan out-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(defn my-inc [segments]
  :ignored)

(deftest bulk-functions
  (let [id (java.util.UUID/randomUUID)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/id id)
        peer-config (assoc (:peer-config config) :onyx/id id)
        env (onyx.api/start-env env-config)
        peer-group (onyx.api/start-peer-group peer-config)
        batch-size 5
        catalog
        [{:onyx/name :in
          :onyx/plugin :onyx.plugin.core-async/input
          :onyx/type :input
          :onyx/medium :core.async
          :onyx/batch-size batch-size
          :onyx/max-peers 1
          :onyx/doc "Reads segments from a core.async channel"}

         {:onyx/name :inc
          :onyx/fn :onyx.peer.bulk-function-test/my-inc
          :onyx/type :function
          :onyx/bulk? true
          :onyx/batch-size batch-size}

         {:onyx/name :out
          :onyx/plugin :onyx.plugin.core-async/output
          :onyx/type :output
          :onyx/medium :core.async
          :onyx/batch-size batch-size
          :onyx/max-peers 1
          :onyx/doc "Writes segments to a core.async channel"}]

        workflow [[:in :inc] [:inc :out]]

        lifecycles
        [{:lifecycle/task :in
          :lifecycle/calls :onyx.peer.bulk-function-test/in-calls}
         {:lifecycle/task :in
          :lifecycle/calls :onyx.plugin.core-async/reader-calls}
         {:lifecycle/task :out
          :lifecycle/calls :onyx.peer.bulk-function-test/out-calls}
         {:lifecycle/task :out
          :lifecycle/calls :onyx.plugin.core-async/writer-calls}]

        v-peers (onyx.api/start-peers 3 peer-group)]

    (doseq [n (range n-messages)]
      (>!! in-chan {:n n}))
    (>!! in-chan :done)
    (onyx.api/submit-job
     peer-config
     {:catalog catalog
      :workflow workflow
      :lifecycles lifecycles
      :task-scheduler :onyx.task-scheduler/balanced})

    (let [results (take-segments! out-chan)
          expected (set (map (fn [x] {:n x}) (range n-messages)))]
      (is (= expected (set (butlast results))))
      (is (= :done (last results)))

      (doseq [v-peer v-peers]
        (onyx.api/shutdown-peer v-peer))

      (onyx.api/shutdown-peer-group peer-group)

      (onyx.api/shutdown-env env))))