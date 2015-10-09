(ns onyx.windowing.basic-windowing-crash-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is]]
              [taoensso.timbre :as timbre]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config with-test-env add-test-env-peers!]]
            [onyx.api]))


;;; IMPORTANT - since this crashed before task, it'll never play a message twice
;;; Therefore this is not a good test of message deduping
;;; It also only tests crashes at certain points of the process. 
;;; For example, in this test, messages are likely already acked since the crash
;;; delay is rather long

(def input
  (reduce into [] 
          (repeat 20
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
                   {:id 15 :age 35 :event-time #inst "2015-09-13T03:16:00.829-00:00"}])))

(def expected-windows
  [[1442113200000 1442113499999 1] 
   [1442113200000 1442113499999 2] 
   [1442113200000 1442113499999 3] 
   [1442113200000 1442113499999 4] 
   [1442113200000 1442113499999 5] 
   [1442113200000 1442113499999 6] 

   [1442113500000 1442113799999 1] 
   [1442113500000 1442113799999 2] 
   [1442113500000 1442113799999 3] 
   [1442113500000 1442113799999 4] 
   [1442113500000 1442113799999 5]
   [1442113500000 1442113799999 6] 
   [1442113500000 1442113799999 7] 
   [1442113500000 1442113799999 8] 
   [1442113500000 1442113799999 9] 
   [1442113500000 1442113799999 10]

   [1442114100000 1442114399999 1] 
   [1442114100000 1442114399999 2] 
   [1442114100000 1442114399999 3] 
   [1442114100000 1442114399999 4] 

   [1442114700000 1442114999999 1] 
   [1442114700000 1442114999999 2] 

   [1442115000000 1442115299999 1] 
   [1442115000000 1442115299999 2] 

   [1442115900000 1442116199999 1] 
   [1442115900000 1442116199999 2] 

   [1442116500000 1442116799999 1] 
   [1442116500000 1442116799999 2] 
   [1442116500000 1442116799999 3] 
   [1442116500000 1442116799999 4] 
   ])

(def test-state (atom []))

(defn update-atom! [event window-id lower-bound upper-bound state]
  (swap! test-state conj [lower-bound upper-bound state]))

(defn restartable? [e] 
  true)

(def batch-num (atom 0))

(def identity-crash
  {:lifecycle/before-batch 
   (fn [event lifecycle]
     ; give the peer a bit of time to write the chunks out and ack the batches,
     ; since we want to ensure that the batches aren't re-read on restart
     ;{}
     (when (= (swap! batch-num inc) 2)
       (Thread/sleep 7000)
       (throw (ex-info "Restartable" {:restartable? true}))))})

(defrecord MonitoringStats
  [zookeeper-write-log-entry
   zookeeper-read-log-entry
   zookeeper-write-catalog
   zookeeper-write-workflow
   zookeeper-write-flow-conditions
   zookeeper-write-lifecycles
   zookeeper-write-task
   zookeeper-write-chunk
   zookeeper-write-job-scheduler
   zookeeper-write-messaging
   zookeeper-force-write-chunk
   zookeeper-write-origin
   zookeeper-read-catalog
   zookeeper-read-workflow
   zookeeper-read-flow-conditions
   zookeeper-read-lifecycles
   zookeeper-read-task
   zookeeper-read-chunk
   zookeeper-read-origin
   zookeeper-read-job-scheduler
   zookeeper-read-messaging
   zookeeper-gc-log-entry
   window-log-write-entry
   window-log-playback
   peer-ack-segments
   peer-retry-segment
   peer-try-complete-job
   peer-strip-sentinel
   peer-complete-message
   peer-gc-peer-link
   peer-backpressure-on
   peer-backpressure-off
   peer-prepare-join
   peer-notify-join
   peer-accept-join])


(defn identity-mod-id [segment]
  [segment (update segment :id * 10000)])

(deftest fault-tolerance-fixed-windows-segment-trigger

  (def in-chan (chan (inc (count input))))

  (def out-chan (chan (sliding-buffer (inc (count input)))))

  (defn inject-in-ch [event lifecycle]
    {:core.async/chan in-chan})

  (defn inject-out-ch [event lifecycle]
    {:core.async/chan out-chan})

  (def in-calls
    {:lifecycle/before-task-start inject-in-ch})

  (def out-calls
    {:lifecycle/before-task-start inject-out-ch})


  (let [id (java.util.UUID/randomUUID)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/id id)
        peer-config (assoc (:peer-config config) :onyx/id id)
        batch-size 10
        workflow
        [[:in :identity] [:identity :out]]

        catalog
        [{:onyx/name :in
          :onyx/plugin :onyx.plugin.core-async/input
          :onyx/type :input
          :onyx/medium :core.async
          :onyx/pending-timeout 30000
          ;; make the time between batches very long 
          ;; because we don't want too many empty batches
          ;; between which we'll get crashes
          :onyx/batch-timeout 20000
          :onyx/batch-size batch-size
          :onyx/max-peers 1
          :onyx/doc "Reads segments from a core.async channel"}

         {:onyx/name :identity
          :onyx/fn ::identity-mod-id
          :onyx/group-by-key :age ;; irrelevant because only one peer
          :onyx/restart-pred-fn ::restartable?
          :onyx/uniqueness-key :id
          :onyx/min-peers 1
          :onyx/max-peers 1
          :onyx/flux-policy :recover ;; should only recover if possible?
          :onyx/type :function
          :onyx/batch-size batch-size}

         {:onyx/name :out
          :onyx/plugin :onyx.plugin.core-async/output
          :onyx/type :output
          :onyx/medium :core.async
          :onyx/max-peers 1
          :onyx/batch-size batch-size
          :onyx/doc "Writes segments to a core.async channel"}]

        windows
        [{:window/id :collect-segments
          :window/task :identity
          :window/type :fixed
          :window/aggregation :onyx.windowing.aggregation/count
          :window/window-key :event-time
          :window/range [5 :minutes]}]

        triggers
        [{:trigger/window-id :collect-segments
          :trigger/refinement :accumulating
          :trigger/on :segment
          :trigger/fire-all-extents? true
          ;; Align threshhold with batch-size since we'll be restarting
          :trigger/threshold [1 :elements]
          :trigger/sync ::update-atom!}]

        lifecycles
        [{:lifecycle/task :in
          :lifecycle/calls ::in-calls}
         {:lifecycle/task :in
          :lifecycle/calls :onyx.plugin.core-async/reader-calls}
         {:lifecycle/task :out
          :lifecycle/calls ::out-calls}
         {:lifecycle/task :identity
          :lifecycle/calls ::identity-crash}
         {:lifecycle/task :out
          :lifecycle/calls :onyx.plugin.core-async/writer-calls}]


        stats-holder (let [stats (map->MonitoringStats {})]
                       (reduce (fn [st k] (assoc st k (atom []))) 
                               stats
                               (keys stats))) 

        event-fn (fn print-monitoring-event [_ event]
                   (let [stats-value (swap! (get stats-holder (:event event)) conj event)]
                     #_(timbre/info "MONITORING NOW: " stats-value)))

        monitoring-config {:monitoring :custom
                           :zookeeper-read-catalog event-fn
                           :window-log-playback event-fn
                           :window-log-write-entry event-fn}]
    (with-test-env [test-env [6 env-config peer-config monitoring-config]]
      (onyx.api/submit-job peer-config
                           {:catalog catalog
                            :workflow workflow
                            :lifecycles lifecycles
                            :windows windows
                            :triggers triggers
                            :task-scheduler :onyx.task-scheduler/balanced})
      (doseq [i input]
        (>!! in-chan i))
      (>!! in-chan :done)

      (close! in-chan)

      (let [results (take-segments! out-chan)]
        ;(is (= (into #{} input) (into #{} (butlast results))))
        (is (= :done (last results)))
        (is (= (distinct (sort expected-windows)) (distinct (sort @test-state))))))))
