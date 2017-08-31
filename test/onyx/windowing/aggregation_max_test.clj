(ns onyx.windowing.aggregation-max-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.test-helper :refer [load-config with-test-env]]
            [onyx-cep.api :as cep]
            [onyx-cep.runtime :as rt]
            [onyx.api]))

(def input
  [{:id 1  :age 3 :event-time #inst "2015-09-13T03:00:00.829-00:00"}
   {:id 2  :age 6 :event-time #inst "2015-09-13T03:04:00.829-00:00"}
   {:id 3  :age 2 :event-time #inst "2015-09-13T03:05:00.829-00:00"}
   {:id 4  :age 7 :event-time #inst "2015-09-13T03:06:00.829-00:00"}
   {:id 5  :age 10 :event-time #inst "2015-09-13T03:07:00.829-00:00"}
   {:id 6  :age 11 :event-time #inst "2015-09-13T03:08:00.829-00:00"}
   {:id 7  :age 19 :event-time #inst "2015-09-13T03:09:00.829-00:00"}
   {:id 8  :age 5 :event-time #inst "2015-09-13T03:15:00.829-00:00"}])

(def expected-windows
  '[[(7 10) (10 11) (11 19)]])

(def test-state (atom []))

(defn aggregation-apply-log [window runtime v]
  (rt/evaluate-segment runtime v))

(defn aggregation-fn-init [window]
  (-> (cep/new-pattern-sequence "test")
      (cep/begin "start" (fn [event & context] (> event 5)))
      (cep/next "bigger" (fn [event & context] (> event 9)))
      (rt/initialize-runtime)))

(defn aggregation-fn [window segment]
  (:age segment))

(defn super-aggregation [window state-1 state-2]
  (throw (Exception.)))

(def ^:export cep
  {:aggregation/init aggregation-fn-init
   :aggregation/create-state-update aggregation-fn
   :aggregation/apply-state-update aggregation-apply-log
   :aggregation/super-aggregation-fn super-aggregation})

(defn update-atom! [event window trigger 
                    {:keys [lower-bound upper-bound event-type] :as opts} 
                    extent-state]
  (when-not (empty? (:matches extent-state)) 
    (swap! test-state conj (:matches extent-state))))

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

(deftest max-test
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
          ;; TODO, need incremental because we need to know the extents in the extent store
          ;; This can change if we just store the extents, not the final vlaues.
          ;; Suggest a third window/materialize scheme?
          :window/materialize [:lazy]
          :window/aggregation [::cep :age]
          :window/init 0}]

        triggers
        [{:trigger/window-id :collect-segments
          :trigger/on :onyx.triggers/timer
          :trigger/period [1 :seconds]
          :trigger/id :sync
          :trigger/post-evictor [:all]
          :trigger/sync ::update-atom!}]

        lifecycles
        [{:lifecycle/task :in
          :lifecycle/calls ::in-calls}
         {:lifecycle/task :out
          :lifecycle/calls ::out-calls}]]

    (reset! in-chan (chan (inc (count input))))
    (reset! in-buffer {})
    (reset! out-chan (chan (sliding-buffer (inc (count input)))))
    (reset! test-state [])

    (with-test-env [test-env [3 env-config peer-config]]
      (doseq [i input]
        (>!! @in-chan i))
      (let [{:keys [job-id]} (onyx.api/submit-job
                              peer-config
                              {:catalog catalog
                               :workflow workflow
                               :lifecycles lifecycles
                               :windows windows
                               :triggers triggers
                               :task-scheduler :onyx.task-scheduler/balanced})
            _ (Thread/sleep 5000)
            _ (close! @in-chan)
            _ (onyx.test-helper/feedback-exception! peer-config job-id)
            results (take-segments! @out-chan 50)]

        (is (= (into #{} input) (into #{} results)))
        (is (= expected-windows @test-state))))))
