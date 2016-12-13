(ns onyx.peer.flow-retry-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [taoensso.timbre :refer [info warn trace fatal] :as timbre]
            [onyx.test-helper :refer [load-config with-test-env]]
            [onyx.api]))

(def in-chan (atom nil))

(def out-chan (atom nil))

(defn inject-in-ch [event lifecycle]
  {:core.async/chan @in-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan @out-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(def retried? (atom false))

(defn retry?
  "Predicate to trigger retry if the number is 10"
  [event old segment all-new]
  (if (and (not @retried?)
           (= 10 (:n segment)))
    (do (swap! retried? not)
        true)
    false))

(defn my-inc [{:keys [n] :as segment}]
  (update-in segment [:n] inc))

(deftest ^:broken flow-retry
  (let [id (random-uuid)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id id)
        peer-config (assoc (:peer-config config) :onyx/tenancy-id id)
        batch-size 10

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

        workflow [[:in :inc]
                  [:inc :out]]

        lifecycles [{:lifecycle/task :in
                     :lifecycle/calls ::in-calls}
                    {:lifecycle/task :out
                     :lifecycle/calls ::out-calls}]

        flow-conditions [{:flow/from :inc
                          :flow/to [:out]
                          :flow/short-circuit? true
                          :flow/action :retry
                          :flow/predicate ::retry?}]]

    (reset! in-chan (chan 100))
    (reset! out-chan (chan (sliding-buffer 100)))

    (with-test-env [test-env [3 env-config peer-config]]
      (doseq [x (range 20)]
        (>!! @in-chan {:n x}))

      (close! @in-chan)

      (onyx.api/submit-job peer-config
                           {:catalog catalog :workflow workflow
                            :flow-conditions flow-conditions
                            :lifecycles lifecycles
                            :task-scheduler :onyx.task-scheduler/balanced})

      (let [results (take-segments! @out-chan)]
        (is @retried?)
        (is (= 21 (count results)))))))
