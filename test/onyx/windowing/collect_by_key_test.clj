(ns onyx.windowing.collect-by-key-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is]]
            [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config with-test-env feedback-exception!]]
            [onyx.api]))

(def input
  [{:id 1  :team :a :event-time #inst "2015-09-13T03:00:00.829-00:00"}
   {:id 2  :team :a :event-time #inst "2015-09-13T03:04:00.829-00:00"}
   {:id 3  :team :b :event-time #inst "2015-09-13T03:05:00.829-00:00"}
   {:id 4  :team :a :event-time #inst "2015-09-13T03:06:00.829-00:00"}
   {:id 5  :team :a :event-time #inst "2015-09-13T03:07:00.829-00:00"}
   {:id 6  :team :b :event-time #inst "2015-09-13T03:08:00.829-00:00"}
   {:id 7  :team :a :event-time #inst "2015-09-13T03:09:00.829-00:00"}
   {:id 8  :team :c :event-time #inst "2015-09-13T03:15:00.829-00:00"}
   {:id 9  :team :a :event-time #inst "2015-09-13T03:25:00.829-00:00"}
   {:id 10 :team :c :event-time #inst "2015-09-13T03:45:00.829-00:00"}
   {:id 11 :team :a :event-time #inst "2015-09-13T03:03:00.829-00:00"}
   {:id 12 :team :a :event-time #inst "2015-09-13T03:56:00.829-00:00"}
   {:id 13 :team :b :event-time #inst "2015-09-13T03:59:00.829-00:00"}
   {:id 14 :team :a :event-time #inst "2015-09-13T03:32:00.829-00:00"}
   {:id 15 :team :c :event-time #inst "2015-09-13T03:16:00.829-00:00"}])

(def test-state (promise))

(defn update-atom! [event window trigger {:keys [event-type] :as opts} state]
  (when (= :job-completed event-type)
    (deliver test-state state)))

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

(deftest min-test
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
          :onyx/max-peers 1
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
          :window/aggregation [:onyx.windowing.aggregation/collect-by-key :team]
          :window/window-key :event-time}]

        triggers
        [{:trigger/window-id :collect-segments
          :trigger/id :sync
          
          :trigger/on :onyx.triggers/segment
          :trigger/fire-all-extents? true
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

    (with-test-env [test-env [3 env-config peer-config]]
      (doseq [i input]
        (>!! @in-chan i))
      (close! @in-chan)

      (let [job-id (:job-id (onyx.api/submit-job
                             peer-config
                             {:catalog catalog
                              :workflow workflow
                              :lifecycles lifecycles
                              :windows windows
                              :triggers triggers
                              :task-scheduler :onyx.task-scheduler/balanced}))
            _ (onyx.test-helper/feedback-exception! peer-config job-id)
            results (take-segments! @out-chan 50)]
        (is (= (into #{} input) (into #{} results)))
        (let [state (deref test-state 5000 nil)]
          (is (= {:a
                  #{{:id 4  :team :a :event-time #inst "2015-09-13T03:06:00.829-00:00"}
                    {:id 9  :team :a :event-time #inst "2015-09-13T03:25:00.829-00:00"}
                    {:id 5  :team :a :event-time #inst "2015-09-13T03:07:00.829-00:00"}
                    {:id 12 :team :a :event-time #inst "2015-09-13T03:56:00.829-00:00"}
                    {:id 2  :team :a :event-time #inst "2015-09-13T03:04:00.829-00:00"}
                    {:id 1  :team :a :event-time #inst "2015-09-13T03:00:00.829-00:00"}
                    {:id 11 :team :a :event-time #inst "2015-09-13T03:03:00.829-00:00"}
                    {:id 14 :team :a :event-time #inst "2015-09-13T03:32:00.829-00:00"}
                    {:id 7  :team :a :event-time #inst "2015-09-13T03:09:00.829-00:00"}}
                  :b
                  #{{:id 13 :team :b :event-time #inst "2015-09-13T03:59:00.829-00:00"}
                    {:id 3  :team :b :event-time #inst "2015-09-13T03:05:00.829-00:00"}
                    {:id 6  :team :b :event-time #inst "2015-09-13T03:08:00.829-00:00"}}
                  :c
                  #{{:id 8  :team :c :event-time #inst "2015-09-13T03:15:00.829-00:00"}
                    {:id 15 :team :c :event-time #inst "2015-09-13T03:16:00.829-00:00"}
                    {:id 10 :team :c :event-time #inst "2015-09-13T03:45:00.829-00:00"}}}
                 state)))))))
