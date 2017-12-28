(ns onyx.gc.gc-multiple-replicas-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is]]
            [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [onyx.tasks.seq]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.test-helper :refer [load-config with-test-env add-test-env-peers!]]
            [onyx.job :refer [add-task]]
            [onyx.api]))

;; README
;; The following test is a very brute force way to test
;; that we can gc checkpoints, and should be replaced with a better designed
;; test.

(def input
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
   {:id 15 :age 35 :event-time #inst "2015-09-13T03:16:00.829-00:00"}
   {:id 16  :age 12 :event-time #inst "2015-09-13T03:04:00.829-00:00"}
   {:id 17  :age 52 :event-time #inst "2015-09-13T03:08:00.829-00:00"}
   {:id 18  :age 53 :event-time #inst "2015-09-13T03:07:00.829-00:00"}
   {:id 19  :age 3 :event-time #inst "2015-09-13T03:05:00.829-00:00"}])

(defn output->final-counts [window-counts]
  (let [grouped (group-by (juxt first second) window-counts)]
    (set (map (fn get-latest [[[bounds k] all]]
                [bounds k (last (sort (map last all)))])
              grouped)))) 

(def expected-windows
  #{[[1442115000000 1442115299999] 60 1]
    [[1442113200000 1442113499999] 21 1]
    [[1442113500000 1442113799999] 64 1]
    [[1442113500000 1442113799999] 3 2]
    [[1442116500000 1442116799999] 83 1]
    [[1442115900000 1442116199999] 37 1]
    [[1442113500000 1442113799999] 52 2]
    [[1442114100000 1442114399999] 35 2]
    [[1442113500000 1442113799999] 53 2]
    [[1442113200000 1442113499999] 15 1]
    [[1442114700000 1442114999999] 49 1]
    [[1442113500000 1442113799999] 24 1]
    [[1442113200000 1442113499999] 12 2]
    [[1442116500000 1442116799999] 22 1]})

(defn restartable? [event lifecycle lifecycle-name e]
  :restart)

(def test-state (atom nil))

(def batch-num (atom nil))

(def in-chan (atom nil))
(def in-buffer (atom nil))

(def out-chan (atom nil))

(defn update-atom! [event window trigger {:keys [lower-bound upper-bound group-key event-type] :as opts} extent-state]
  (when (= :job-completed event-type)
    (swap! test-state conj [[lower-bound upper-bound] group-key extent-state])))

(def identity-crash
  {:lifecycle/handle-exception restartable?
   :lifecycle/after-read-batch (fn [event lifecycle]
                                 (when (and (not (empty? (:onyx.core/batch event))) 
                                                (zero? (rand-int 8)))
                                  (throw (ex-info "Restart me!" {})))
                                 {})})

(defn inject-in-ch [event lifecycle]
  {:core.async/chan @in-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan @out-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(defn slow-down-in [segment]
  (Thread/sleep 500)
  segment)

(defn at-out [segment]
  (info "AT OUT SEG" segment)
  segment)

(deftest fault-tolerance-fixed-windows-segment-trigger
  (do
    (reset! test-state [])
    (reset! batch-num 0)

    (reset! in-chan (chan (inc (count input))))
    (reset! in-buffer {})
    (reset! out-chan (chan 10000))

    (let [id (random-uuid)
          config (load-config)
          env-config (assoc (:env-config config) :onyx/tenancy-id id)
          peer-config (assoc (:peer-config config) 
                             :onyx/tenancy-id id
                             ;; Inject very frequently so we can get some full snapshots
                             :onyx.peer/coordinator-barrier-period-ms 1)
          batch-size 5 
          workflow
          [[:in :identity] [:identity :out]]

          catalog
          [{:onyx/name :identity
            :onyx/fn :clojure.core/identity
            :onyx/group-by-key :age
            :onyx/min-peers 2
            :onyx/max-peers 2
            :onyx/flux-policy :recover
            :onyx/type :function
            :onyx/batch-size batch-size}

           {:onyx/name :out
            :onyx/plugin :onyx.plugin.core-async/output
            :onyx/type :output
            :onyx/medium :core.async
            :onyx/fn ::at-out
            :onyx/max-peers 1
            :onyx/batch-size batch-size
            :onyx/doc "Writes segments to a core.async channel"}]

          windows
          [{:window/id :my-window
            :window/task :identity
            :window/type :fixed
            :window/aggregation :onyx.windowing.aggregation/count
            :window/window-key :event-time
            :window/range [5 :minutes]}]

          triggers
          [{:trigger/window-id :my-window
            :trigger/id :sync
            :trigger/on :onyx.triggers/segment
            ;; Align threshhold with batch-size since we'll be restarting
            :trigger/threshold [1 :elements]
            :trigger/sync ::update-atom!}]

          lifecycles
          [{:lifecycle/task :out
            :lifecycle/calls ::out-calls}
           {:lifecycle/task :identity
            :lifecycle/calls ::identity-crash}]
          job (-> {:catalog catalog
                   :workflow workflow
                   :lifecycles lifecycles
                   :windows windows
                   :triggers triggers
                   :task-scheduler :onyx.task-scheduler/balanced}
                  (add-task (onyx.tasks.seq/input-serialized 
                             :in {:onyx/batch-size 1 
                                  :onyx/fn ::slow-down-in
                                  :onyx/n-peers 1} 
                             input)))]

      (with-test-env [test-env [6 env-config peer-config]]
        (let [{:keys [job-id]} (onyx.api/submit-job peer-config job)
              _ (onyx.test-helper/feedback-exception! peer-config job-id)
              _ (run! (fn [_]
                        (Thread/sleep 500)
                        (onyx.api/gc-checkpoints peer-config id job-id))
                      (range 20))
              results (take-segments! @out-chan 5)]
          ;; one last time after job is done
          (Thread/sleep 500)
          (onyx.api/gc-checkpoints peer-config id job-id)
          ;; TODO: improve test by pulling in old coordinates, recovering and then starting gc again.
          ;; TODO: allow gc to take part over tenancy history.
          (let [checkpoint-epochs-rvs (onyx.log.curator/children 
                                       (:conn (:log (:env test-env)))
                                       (str (onyx.log.zookeeper/epoch-path id) "/" job-id))]
            (is (= checkpoint-epochs-rvs
                   [(str (:replica-version (onyx.api/job-snapshot-coordinates 
                                            peer-config
                                            id
                                            job-id)))]))
          (onyx.api/clear-checkpoints peer-config id job-id)
          (is (empty?
               (onyx.log.curator/children 
                (:conn (:log (:env test-env)))
                (str (onyx.log.zookeeper/epoch-path id) "/" job-id)))))
          (is (= expected-windows (output->final-counts @test-state))))))))
