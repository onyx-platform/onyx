(ns onyx.peer.automatic-kill-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config with-test-env add-test-env-peers!]]
            [onyx.extensions :as extensions]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.api]))

(def n-messages 5000)

(def in-chan-1 (atom nil))
(def in-buffer-1 (atom nil))
(def in-chan-2 (atom nil))
(def in-buffer-2 (atom nil))

(def out-chan-1 (atom nil))
(def out-chan-2 (atom nil))

(defn inject-in-ch-1 [event lifecycle]
  {:core.async/buffer in-buffer-1
   :core.async/chan @in-chan-1})

(defn inject-out-ch-1 [event lifecycle]
  {:core.async/chan @out-chan-1})

(defn inject-in-ch-2 [event lifecycle]
  {:core.async/buffer in-buffer-2
   :core.async/chan @in-chan-2})

(defn inject-out-ch-2 [event lifecycle]
  {:core.async/chan @out-chan-2})

(def in-calls-1
  {:lifecycle/before-task-start inject-in-ch-1})

(def out-calls-1
  {:lifecycle/before-task-start inject-out-ch-1})

(def in-calls-2
  {:lifecycle/before-task-start inject-in-ch-2})

(def out-calls-2
  {:lifecycle/before-task-start inject-out-ch-2})

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(deftest test-automatic-kill
  (let [id (random-uuid)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id id)
        peer-config (assoc (:peer-config config) :onyx/tenancy-id id)
        batch-size 20
        catalog-1 [{:onyx/name :in-1
                    :onyx/plugin :onyx.plugin.core-async/input
                    :onyx/type :input
                    :onyx/medium :core.async
                    :onyx/batch-size batch-size
                    :onyx/max-peers 1
                    :onyx/doc "Reads segments from a core.async channel"}

                   {:onyx/name :inc
                    :onyx/fn :onyx.peer.automatic-kill-test/my-invalid-fn
                    :onyx/type :function
                    :onyx/batch-size batch-size}

                   {:onyx/name :out-1
                    :onyx/plugin :onyx.plugin.core-async/output
                    :onyx/type :output
                    :onyx/medium :core.async
                    :onyx/batch-size batch-size
                    :onyx/max-peers 1
                    :onyx/doc "Writes segments to a core.async channel"}]

        catalog-2 [{:onyx/name :in-2
                    :onyx/plugin :onyx.plugin.core-async/input
                    :onyx/type :input
                    :onyx/medium :core.async
                    :onyx/batch-size batch-size
                    :onyx/max-peers 1
                    :onyx/doc "Reads segments from a core.async channel"}

                   {:onyx/name :inc
                    :onyx/fn :onyx.peer.automatic-kill-test/my-inc
                    :onyx/type :function
                    :onyx/batch-size batch-size}

                   {:onyx/name :out-2
                    :onyx/plugin :onyx.plugin.core-async/output
                    :onyx/type :output
                    :onyx/medium :core.async
                    :onyx/batch-size batch-size
                    :onyx/max-peers 1
                    :onyx/doc "Writes segments to a core.async channel"}]

        workflow-1 [[:in-1 :inc] [:inc :out-1]]
        workflow-2 [[:in-2 :inc] [:inc :out-2]]

        lifecycles-1 [{:lifecycle/task :in-1
                       :lifecycle/calls :onyx.peer.automatic-kill-test/in-calls-1}
                      {:lifecycle/task :out-1
                       :lifecycle/calls :onyx.peer.automatic-kill-test/out-calls-1}]

        lifecycles-2 [{:lifecycle/task :in-2
                       :lifecycle/calls :onyx.peer.automatic-kill-test/in-calls-2}
                      {:lifecycle/task :out-2
                       :lifecycle/calls :onyx.peer.automatic-kill-test/out-calls-2}]]
    (reset! in-buffer-1 {})
    (reset! in-chan-1 (chan (inc n-messages)))
    (reset! in-buffer-2 {})
    (reset! in-chan-2 (chan (inc n-messages)))
    (reset! out-chan-1 (chan (sliding-buffer (inc n-messages))))
    (reset! out-chan-2 (chan (sliding-buffer (inc n-messages))))

    (with-test-env [test-env [3 env-config peer-config]]
      (doseq [n (range n-messages)]
        ;; Using + 50,000 on the first job to make sure messages don't cross jobs.
        (>!! @in-chan-1 {:n (+ n 50000)})
        (>!! @in-chan-2 {:n n}))

      (close! @in-chan-1)
      (close! @in-chan-2)

      (let [j1 (:job-id (onyx.api/submit-job
                         peer-config
                         {:catalog catalog-1 :workflow workflow-1
                          :lifecycles lifecycles-1
                          :task-scheduler :onyx.task-scheduler/balanced}))
            j2 (:job-id (onyx.api/submit-job
                         peer-config
                         {:catalog catalog-2 :workflow workflow-2
                          :lifecycles lifecycles-2
                          :task-scheduler :onyx.task-scheduler/balanced}))

            ch (chan n-messages)]
        ;; Make sure we find the killed job in the replica, then bail
        (loop [replica (extensions/subscribe-to-log (:log (:env test-env)) ch)]
          (let [entry (<!! ch)
                new-replica (extensions/apply-log-entry entry replica)]
            (when-not (= (:killed-jobs new-replica) [j1])
              (recur new-replica))))
        (onyx.api/await-job-completion peer-config j2)
        (let [results (take-segments! @out-chan-2 50)
              expected (set (map (fn [x] {:n (inc x)}) (range n-messages)))]
          (is (= expected (set results))))
        (close! @in-chan-1)
        (close! @in-chan-2)))))
