(ns onyx.windowing.window-exceptions-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [schema.core :as s]
            [onyx.schema :as os]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.test-helper :refer [load-config with-test-env]]
            [onyx.api]))

(def input
  [{:id 1 :event-time 0}])

(defn no-op [& _])

(defn user-function [segment]
  (throw (ex-info "Blow up here" {})))

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

(defn transform-exception [event segment e]
  {:status :threw-exception
   :segment segment})

(def always-true (constantly true))

(deftest window-exceptions-test
  (let [id (random-uuid)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id id)
        peer-config (assoc (:peer-config config) :onyx/tenancy-id id)
        batch-size 20
        workflow
        [[:in :identity] [:identity :out]]

        catalog
        [{:onyx/name :in
          :onyx/plugin :onyx.plugin.core-async/input
          :onyx/type :input
          :onyx/medium :core.async
          :onyx/batch-size batch-size
          :onyx/max-peers 1
          :onyx/doc "Reads segments from a core.async channel"}

         {:onyx/name :identity
          :onyx/fn ::user-function
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

        flow-conditions
        [{:flow/from :identity
          :flow/to [:out]
          :flow/short-circuit? true
          :flow/thrown-exception? true
          :flow/predicate [::always-true]
          :flow/post-transform ::transform-exception}]

        windows
        [{:window/id :collect-segments
          :window/task :identity
          :window/type :fixed
          :window/aggregation :onyx.windowing.aggregation/conj
          :window/window-key :event-time
          :window/range [5 :minutes]}]

        triggers
        [{:trigger/window-id :collect-segments
          :trigger/id :sync
          
          :trigger/fire-all-extents? true
          :trigger/on :onyx.triggers/segment
          :trigger/threshold [5 :elements]
          :trigger/sync ::no-op}]

        lifecycles
        [{:lifecycle/task :in
          :lifecycle/calls ::in-calls}
         {:lifecycle/task :out
          :lifecycle/calls ::out-calls}]]

    (reset! in-chan (chan (inc (count input))))
    (reset! in-buffer {})
    (reset! out-chan (chan (sliding-buffer (inc (count input)))))

    (with-test-env [test-env [3 env-config peer-config]]
      (doseq [i input]
        (>!! @in-chan i))
        (close! @in-chan)
        (let [job (onyx.api/submit-job
                   peer-config
                   {:catalog catalog
                    :workflow workflow
                    :lifecycles lifecycles
                    :flow-conditions flow-conditions
                    :windows windows
                    :triggers triggers
                    :task-scheduler :onyx.task-scheduler/balanced})
              _ (onyx.test-helper/feedback-exception! peer-config (:job-id job))
              results (take-segments! @out-chan 50)]
          (is (= #{{:status :threw-exception :segment {:id 1 :event-time 0}}}
                 (into #{} results)))))))
