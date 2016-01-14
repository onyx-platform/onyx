(ns onyx.peer.lifecycle-exception-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config with-test-env feedback-exception!]]
            [onyx.api]))

(def n-messages 100)

(def in-chan (atom nil))

(def out-chan (atom nil))

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(def exception-thrower (atom :start-task?))

(defn start-task? [event lifecycle]
  (when (= @exception-thrower :start-task?)
    (reset! exception-thrower :before-task)
    (throw (ex-info "Threw exception in start-task" {})))
  true)

(defn before-task-start [event lifecycle]
  (when (= @exception-thrower :before-task)
    (reset! exception-thrower :before-batch)
    (throw (ex-info "Threw exception in before-task" {})))
  {})

(defn before-batch [event lifecycle]
  (when (= @exception-thrower :before-batch)
    (reset! exception-thrower :after-read-batch)
    (throw (ex-info "Threw exception in before-batch" {})))
  {})

(defn after-read-batch [event lifecycle]
  (when (= @exception-thrower :after-read-batch)
    (reset! exception-thrower :after-batch)
    (throw (ex-info "Threw exception in after-read-batch" {})))
  {})

(defn after-batch [event lifecycle]
  (when (= @exception-thrower :after-batch)
    (reset! exception-thrower nil)
    (>!! @in-chan :done)
    (close! @in-chan)
    (throw (ex-info "Threw exception in after-batch" {})))
  {})

(defn after-task-stop [event lifecycle]
  {})

(defn handle-exception [event lifecycle phase e]
  :restart)

(defn inject-in-ch [event lifecycle]
  {:core.async/chan @in-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan @out-chan})

(def calls
  {:lifecycle/start-task? start-task?
   :lifecycle/before-task-start before-task-start
   :lifecycle/before-batch before-batch
   :lifecycle/after-read-batch after-read-batch
   :lifecycle/after-batch after-batch
   :lifecycle/after-task-stop after-task-stop
   :lifecycle/handle-exception handle-exception})

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
        lifecycles [{:lifecycle/task :in
                     :lifecycle/calls ::in-calls}
                    {:lifecycle/task :in
                     :lifecycle/calls :onyx.plugin.core-async/reader-calls}
                    {:lifecycle/task :inc
                     :lifecycle/calls ::calls
                     :lifecycle/doc "Test lifecycles that increment a counter in an atom"}
                    {:lifecycle/task :out
                     :lifecycle/calls ::out-calls}
                    {:lifecycle/task :out
                     :lifecycle/calls :onyx.plugin.core-async/writer-calls}]]

    (reset! in-chan (chan (inc n-messages)))
    (reset! out-chan (chan (sliding-buffer (inc n-messages))))

    (with-test-env [test-env [3 env-config peer-config]]
      (let [job-id
            (:job-id
             (onyx.api/submit-job peer-config
                                  {:catalog catalog
                                   :workflow workflow
                                   :lifecycles lifecycles
                                   :task-scheduler :onyx.task-scheduler/balanced}))]

        (doseq [n (range n-messages)]
          (>!! @in-chan {:n n}))

        (feedback-exception! peer-config job-id (:log (:env test-env)))
        ;; Made it to the end with a successful job completion.
        (is true)))))
