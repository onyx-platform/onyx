(ns onyx.stress.checkpoint-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer dropping-buffer]]
            [clojure.test :refer [deftest is]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [schema.core :as s]
            [onyx.schema :as os]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.test-helper :refer [load-config with-test-env]]
            [onyx.api]))

(def test-state (atom []))

(s/defn update-atom! [event :- os/Event 
                      window :- os/Window 
                      trigger :- os/Trigger 
                      {:keys [lower-bound upper-bound event-type] :as state-event} :- os/StateEvent 
                      extent-state]
  (when (zero? (rand-int 5000)) (println "TEST STATE HAS" (count @test-state)))
  (when-not (= :job-completed event-type)
    (swap! test-state conj [lower-bound upper-bound extent-state])))

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

;; Needs more memory than currently set -Xmx
(deftest ^:stress aggregation-stress-test 
    (let [input (->> (repeat 10000
                             [{:id 1  :age 21 :event-time #inst "2015-09-13T03:00:00.829-00:00"}
                              {:id 2  :age 12 :event-time #inst "2015-09-13T03:04:00.829-00:00"}
                              {:id 3  :age 3  :event-time #inst "2015-09-13T03:05:00.829-00:00"}
                              {:id 4  :age 64 :event-time #inst "2015-09-13T03:06:00.829-00:00"}
                              {:id 5  :age 53 :event-time #inst "2015-09-13T03:07:00.829-00:00"}
                              {:id 6  :age 52 :event-time #inst "2015-09-13T03:08:00.829-00:00"}
                              {:id 7  :age 24 :event-time #inst "2015-09-13T03:09:00.829-00:00"}
                              {:id 8  :age 35 :event-time #inst "2015-09-13T03:15:00.829-00:00"}
                              {:id 9  :age 49 :event-time #inst "2015-09-13T03:25:00.829-00:00"}
                              {:id 10 :age 37 :event-time #inst "2015-09-13T03:45:00.829-00:00"}
                              {:id 11 :age 15 :event-time #inst "2015-09-13T03:03:00.829-00:00"}
                              {:id 12 :age 22 :event-time #inst "2015-09-13T03:56:00.829-00:00"}
                              {:id 13 :age 83 :event-time #inst "2015-09-13T03:59:00.829-00:00"}
                              {:id 14 :age 60 :event-time #inst "2015-09-13T03:32:00.829-00:00"}
                              {:id 15 :age 35 :event-time #inst "2015-09-13T03:16:00.829-00:00"}])
                     (reduce into [])
                     (map (fn [a]
                            (assoc a 
                                   :randstuff
                                   (repeatedly 400
                                               #(rand-int 256))))))
          id (random-uuid)
          config (load-config)
          env-config (assoc (:env-config config) :onyx/tenancy-id id)
          peer-config (assoc (:peer-config config) :onyx/tenancy-id id :onyx.peer/coordinator-barrier-period-ms 20000)
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
            :window/type :fixed
            ;:window/aggregation [:onyx.windowing.aggregation/collect-by-key :randstuff]
            :window/aggregation :onyx.windowing.aggregation/conj
            :window/window-key :event-time
            :window/range [5 :minutes]}]

          triggers
          [{:trigger/window-id :collect-segments
            
            :trigger/fire-all-extents? true
            :trigger/on :onyx.triggers/segment
            :trigger/threshold [5 :elements]
            :trigger/sync ::update-atom!}]

          lifecycles
          [{:lifecycle/task :in
            :lifecycle/calls ::in-calls}
           {:lifecycle/task :out
            :lifecycle/calls ::out-calls}]]

    (reset! in-chan (chan (inc (count input))))
    (reset! in-buffer {})
    (reset! out-chan (chan (dropping-buffer 1)))
    (reset! test-state [])

    (with-test-env [test-env [3 env-config peer-config]]
      (doseq [i input]
        (>!! @in-chan i))
        (close! @in-chan)
        (let [job (onyx.api/submit-job
                   peer-config
                   {:catalog catalog
                    :workflow workflow
                    :lifecycles lifecycles
                    :windows windows
                    :triggers triggers
                    :task-scheduler :onyx.task-scheduler/balanced}) ]
          (onyx.test-helper/feedback-exception! peer-config (:job-id job))))))
