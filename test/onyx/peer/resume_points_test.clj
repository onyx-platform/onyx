(ns onyx.peer.resume-points-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.test-helper :refer [load-config with-test-env add-test-env-peers!]]
            [onyx.api]
            
            [onyx.extensions :as extensions]
            [com.stuartsierra.component :as component]
            [onyx.system :as system]))

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
   {:id 15 :age 35 :event-time #inst "2015-09-13T03:16:00.829-00:00"}])

(def expected-windows [[Double/NEGATIVE_INFINITY Double/POSITIVE_INFINITY 15]])

(def test-state (atom []))

(defn update-atom! [event window trigger 
                    {:keys [lower-bound upper-bound event-type] :as opts} 
                    extent-state]
  (when (= :job-completed event-type)
    (swap! test-state 
           update 
           (:onyx.core/job-id event) 
           (fn [vs]
             (conj (vec vs)
                   [lower-bound upper-bound extent-state])))))

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

;; SAVEPOINTS TEST SHOULD USE WINDOWING, AND ADD A WINDOW, ALLOWING ONE TO BE
;; RESUMED, BUT INITIALISING THE OTHER TO NOTHING

;;;
;;SAVE POINTS,
; MAP FROM TASK TO TASK
; SLOT-ID TO SLOT-ID
; :DIRECT
; :ALL	

;;; SUPPLY REPLICA / EPOCH IN RESUME DATA - then coordinator won't have to do it
;;; API CALL TO GET LATEST REPLICA / EPOCH (also available in replica)
;;; Ability to supply different job-ids to migrate different tasks
;;; Input task to read from windows... also to read different window versions over time

(deftest savepoints-test
  (let [id (random-uuid)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id id)
        peer-config (assoc (:peer-config config) 
                           :onyx/tenancy-id id
                           :onyx.peer/coordinator-barrier-period-ms 1)
        batch-size 1
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
          :onyx/uniqueness-key :id
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
          :trigger/refinement :onyx.refinements/accumulating
          :trigger/on :onyx.triggers/segment
          :trigger/threshold [1 :elements]
          :trigger/sync ::update-atom!}]

        lifecycles
        [{:lifecycle/task :in
          :lifecycle/calls ::in-calls}
         {:lifecycle/task :out
          :lifecycle/calls ::out-calls}]]

    (reset! test-state {})
    (reset! in-chan (chan (inc (count input))))
    (reset! in-buffer {})
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
            results (take-segments! @out-chan 500)
            coordinates (let [client (component/start (system/onyx-client peer-config))]
                          (try
                           (extensions/read-checkpoint-coordinate (:log client) id job-id)
                           (finally (component/stop client))))
            job-2 (onyx.api/submit-job peer-config
                                       ;; TODO, validation check these against the job
                                       {:resume-point 
                                        {:in {:input (merge coordinates 
                                                            {:tenancy-id id
                                                             :job-id job-id
                                                             :task-id :in
                                                             :slot-migration :direct})}
                                         :identity {:windows {:collect-segments 
                                                              (merge coordinates 
                                                                     {:tenancy-id id
                                                                      :job-id job-id
                                                                      :task-id :identity
                                                                      :window-id :collect-segments
                                                                      :slot-migration :direct})}}}
                                        :catalog catalog
                                        :workflow workflow
                                        :lifecycles lifecycles
                                        :windows windows
                                        :triggers triggers
                                        :task-scheduler :onyx.task-scheduler/balanced})
            _ (onyx.test-helper/feedback-exception! peer-config (:job-id job-2))]
        (is (= (into #{} input) (into #{} results)))
        (is (= expected-windows (get @test-state job-id)) "job-1-windows-wrong")
        (is (= expected-windows (get @test-state (:job-id job-2))) "job-2-windows-wrong")))))
