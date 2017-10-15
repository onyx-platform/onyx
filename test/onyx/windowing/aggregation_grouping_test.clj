(ns onyx.windowing.aggregation-grouping-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [taoensso.timbre :refer [info]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.test-helper :refer [load-config with-test-env]]
            [onyx.api]))

(def input
  [{:id 1  :age 21 :event-time #inst "2015-09-13T03:00:00.829-00:00"}
   {:id 2  :age 21 :event-time #inst "2015-09-13T03:04:00.829-00:00"}
   {:id 3  :age 21 :event-time #inst "2015-09-13T03:05:00.829-00:00"}
   {:id 4  :age 64 :event-time #inst "2015-09-13T03:06:00.829-00:00"}
   {:id 5  :age 37 :event-time #inst "2015-09-13T03:07:00.829-00:00"}
   {:id 6  :age 52 :event-time #inst "2015-09-13T03:08:00.829-00:00"}
   {:id 7  :age 24 :event-time #inst "2015-09-13T03:09:00.829-00:00"}
   {:id 8  :age 35 :event-time #inst "2015-09-13T03:15:00.829-00:00"}
   {:id 9  :age 24 :event-time #inst "2015-09-13T03:25:00.829-00:00"}
   {:id 10 :age 37 :event-time #inst "2015-09-13T03:45:00.829-00:00"}
   {:id 11 :age 15 :event-time #inst "2015-09-13T03:03:00.829-00:00"}
   {:id 12 :age 22 :event-time #inst "2015-09-13T03:56:00.829-00:00"}
   {:id 13 :age 83 :event-time #inst "2015-09-13T03:59:00.829-00:00"}
   {:id 14 :age 83 :event-time #inst "2015-09-13T03:32:00.829-00:00"}
   {:id 15 :age 15 :event-time #inst "2015-09-13T03:16:00.829-00:00"}])

(def expected-windows
  (reduce-kv
   (fn [all k v] (assoc all k (count v)))
   {}
   (group-by :age input)))

(def test-state (atom {}))

(def fire-count (atom {}))

(defn update-atom! 
  [event window trigger {:keys [lower-bound upper-bound group-key event-type] :as opts} extent-state]
  (swap! fire-count update (vector lower-bound group-key) (fn [v] (inc (or v 0))))
  (swap! test-state merge {group-key extent-state}))

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

(deftest count-test
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
          :onyx/fn :clojure.core/identity
          :onyx/type :function
          :onyx/group-by-key :age
          :onyx/flux-policy :recover
          :onyx/n-peers 3
          :onyx/batch-size batch-size}

         {:onyx/name :out
          :onyx/plugin :onyx.plugin.core-async/output
          :onyx/type :output
          :onyx/medium :core.async
          :onyx/batch-size batch-size
          :onyx/max-peers 1
          :onyx/doc "Writes segments to a core.async channel"}]

        windows
        [{:window/id :collect-segments
          :window/task :identity
          :window/type :global
          :window/aggregation :onyx.windowing.aggregation/count
          :window/window-key :event-time}]

        triggers
        [{:trigger/window-id :collect-segments
          :trigger/id :sync
          :trigger/fire-all-extents? true
          :trigger/on :onyx.triggers/segment
          :trigger/threshold [1 :elements]
          :trigger/sync ::update-atom!}]

        lifecycles
        [{:lifecycle/task :in
          :lifecycle/calls ::in-calls}
         {:lifecycle/task :out
          :lifecycle/calls ::out-calls}]]

    (reset! in-chan (chan (inc (count input))))
    (reset! in-buffer {})
    (reset! out-chan (chan (sliding-buffer (inc (count input)))))
    (reset! fire-count {})
    (reset! test-state {})

    (with-test-env [test-env [5 env-config peer-config]]
      (let [{:keys [job-id]} (onyx.api/submit-job peer-config
                                                  {:catalog catalog
                                                   :workflow workflow
                                                   :lifecycles lifecycles
                                                   :windows windows
                                                   :triggers triggers
                                                   :task-scheduler :onyx.task-scheduler/balanced})
            start-time (System/currentTimeMillis)]
        (doseq [i input]
          (Thread/sleep 500)
          (>!! @in-chan i))
        (close! @in-chan)
        (let [_ (onyx.test-helper/feedback-exception! peer-config job-id)
              end-time (System/currentTimeMillis)
              max-n-extent-fires (apply max (vals @fire-count))
              results (take-segments! @out-chan 50)]
          (is (= (into #{} input) (into #{} results)))
          (is (= expected-windows @test-state)))))))
