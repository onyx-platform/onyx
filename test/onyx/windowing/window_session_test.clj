(ns onyx.windowing.window-session-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.test-helper :refer [load-config with-test-env add-test-env-peers!]]
            [onyx.api]))

(def input
  [{:event-id 0 :id 1 :event-time #inst "2015-09-13T03:00:00.829-00:00"}
   {:event-id 1 :id 1 :event-time #inst "2015-09-13T03:04:00.829-00:00"}
   {:event-id 2 :id 1 :event-time #inst "2015-09-13T03:16:00.829-00:00"}
   
   {:event-id 3 :id 2 :event-time #inst "2015-09-13T03:04:00.829-00:00"}
   {:event-id 4 :id 2 :event-time #inst "2015-09-13T03:05:00.829-00:00"}
   {:event-id 5 :id 2 :event-time #inst "2015-09-13T03:01:00.829-00:00"}
   
   {:event-id 6 :id 3 :event-time #inst "2015-09-13T03:19:00.829-00:00"}
   {:event-id 7 :id 3 :event-time #inst "2015-09-13T03:16:00.829-00:00"}
   {:event-id 8 :id 3 :event-time #inst "2015-09-13T03:12:00.829-00:00"}
   
   {:event-id 9 :id 4 :event-time #inst "2015-09-13T03:06:00.829-00:00"}
   {:event-id 10 :id 4 :event-time #inst "2015-09-13T03:25:00.829-00:00"}
   {:event-id 11 :id 4 :event-time #inst "2015-09-13T03:56:00.829-00:00"}
   
   {:event-id 12 :id 5 :event-time #inst "2015-09-13T03:07:00.829-00:00"}   
   {:event-id 13 :id 5 :event-time #inst "2015-09-13T03:09:00.829-00:00"}
   {:event-id 14 :id 5 :event-time #inst "2015-09-13T03:13:00.829-00:00"}])

(def expected-windows
  [[#inst "2015-09-13T03:00:00.829-00:00" #inst "2015-09-13T03:04:00.829-00:00"
    [{:event-id 0 :id 1 :event-time #inst "2015-09-13T03:00:00.829-00:00"}
     {:event-id 1 :id 1 :event-time #inst "2015-09-13T03:04:00.829-00:00"}]]

   [#inst "2015-09-13T03:16:00.829-00:00" #inst "2015-09-13T03:16:00.829-00:00"
    [{:event-id 2 :id 1 :event-time #inst "2015-09-13T03:16:00.829-00:00"}]]

   [#inst "2015-09-13T03:01:00.829-00:00" #inst "2015-09-13T03:05:00.829-00:00"
    [{:event-id 3 :id 2 :event-time #inst "2015-09-13T03:04:00.829-00:00"}
     {:event-id 4 :id 2 :event-time #inst "2015-09-13T03:05:00.829-00:00"}
     {:event-id 5 :id 2 :event-time #inst "2015-09-13T03:01:00.829-00:00"}]]

   [#inst "2015-09-13T03:12:00.829-00:00" #inst "2015-09-13T03:19:00.829-00:00"
    [{:event-id 6 :id 3 :event-time #inst "2015-09-13T03:19:00.829-00:00"}
     {:event-id 7 :id 3 :event-time #inst "2015-09-13T03:16:00.829-00:00"}
     {:event-id 8 :id 3 :event-time #inst "2015-09-13T03:12:00.829-00:00"}]]

   [#inst "2015-09-13T03:06:00.829-00:00" #inst "2015-09-13T03:06:00.829-00:00"
    [{:event-id 9 :id 4 :event-time #inst "2015-09-13T03:06:00.829-00:00"}]]

   [#inst "2015-09-13T03:25:00.829-00:00" #inst "2015-09-13T03:25:00.829-00:00"
    [{:event-id 10 :id 4 :event-time #inst "2015-09-13T03:25:00.829-00:00"}]]

   [#inst "2015-09-13T03:56:00.829-00:00" #inst "2015-09-13T03:56:00.829-00:00"
    [{:event-id 11 :id 4 :event-time #inst "2015-09-13T03:56:00.829-00:00"}]]

   [#inst "2015-09-13T03:07:00.829-00:00" #inst "2015-09-13T03:13:00.829-00:00"
    [{:event-id 12 :id 5 :event-time #inst "2015-09-13T03:07:00.829-00:00"}
     {:event-id 13 :id 5 :event-time #inst "2015-09-13T03:09:00.829-00:00"}
     {:event-id 14 :id 5 :event-time #inst "2015-09-13T03:13:00.829-00:00"}]]])

(def test-state (atom []))

(defn update-atom! [event window trigger {:keys [lower-bound upper-bound event-type] :as opts} extent-state]
  (when (= :job-completed event-type)
    (swap! test-state conj [(java.util.Date. (long lower-bound))
                            (java.util.Date. (long upper-bound))
                            extent-state])))

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

(deftest session-window-test
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
          :onyx/group-by-key :id
          :onyx/flux-policy :continue
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
          :window/type :session
          :window/storage-strategy [:incremental]
          :window/aggregation :onyx.windowing.aggregation/conj
          :window/window-key :event-time
          :window/timeout-gap [5 :minutes]}]

        triggers
        [{:trigger/window-id :collect-segments
          :trigger/id :sync
          :trigger/fire-all-extents? true
          :trigger/on :onyx.triggers/segment
          :trigger/threshold [15 :elements]
          :trigger/sync ::update-atom!}]

        lifecycles
        [{:lifecycle/task :in
          :lifecycle/calls ::in-calls}
         {:lifecycle/task :out
          :lifecycle/calls ::out-calls}]]

    (reset! in-chan (chan (inc (count input))))
    (reset! in-buffer {})
    (reset! test-state [])
    (reset! out-chan (chan (sliding-buffer (inc (count input)))))

    (with-test-env [test-env [3 env-config peer-config]]
      (doseq [i input]
        (>!! @in-chan i))
      (close! @in-chan)
      (let [{:keys [job-id]} (onyx.api/submit-job peer-config
                                                  {:catalog catalog
                                                   :workflow workflow
                                                   :lifecycles lifecycles
                                                   :windows windows
                                                   :triggers triggers
                                                   :task-scheduler :onyx.task-scheduler/balanced})
            _ (onyx.test-helper/feedback-exception! peer-config job-id)
            results (take-segments! @out-chan 500)]
        (is (= (into #{} input) (into #{} results)))
        (is (= (sort-by first expected-windows) (sort-by first @test-state)))))))
