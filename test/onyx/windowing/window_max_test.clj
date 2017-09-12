(ns onyx.windowing.window-max-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [onyx.windowing.aggregation]
            [onyx.refinements]
            [onyx.windowing.window-compile :as wc]
            [onyx.windowing.window-extensions :as we]
            [onyx.peer.window-state :as ws]
            [schema.test]
            [onyx.api]))

(use-fixtures :once schema.test/validate-schemas)

#_(deftest window-global-test 
  (let [segments [{:id 1  :age 21 :event-time #inst "2015-09-13T03:00:00.829-00:00"}
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
                  {:id 15 :age 35 :event-time #inst "2015-09-13T03:16:00.829-00:00"}]
        window {:window/id :collect-segments
                :window/task :identity
                :window/type :global
                :window/aggregation [:onyx.windowing.aggregation/max :age]
                :window/window-key :event-time
                :window/init 0}
        triggers [] #_[{:trigger/window-id :collect-segments
                        :trigger/id :sync
                        
                        :trigger/on :onyx.triggers/segment
                        :trigger/threshold [15 :elements]
                        :trigger/sync ::update-atom!}]
        window-state (wc/build-window-executor window triggers)

        reduc (reductions ws/next-state window-state segments)]
    (is (= [{} {1 21} {1 21} {1 21} {1 64} {1 64} {1 64} {1 64} {1 64} 
	    {1 64} {1 64} {1 64} {1 64} {1 83} {1 83} {1 83}]
	   (mapv ws/state reduc)))
    (is (= [nil ['(1 21)] ['(1 21)] ['(1 21)] ['(1 64)] ['(1 64)] ['(1 64)] ['(1 64)] ['(1 64)] 
	    ['(1 64)] ['(1 64)] ['(1 64)] ['(1 64)] ['(1 83)] ['(1 83)] ['(1 83)]] 
	   (mapv ws/log-entry reduc)))))

(def test-state (atom []))

(defn update-atom! [event window trigger {:keys [window/extent->bounds] :as opts} state]
  (doall 
    (map (fn [[extent extent-state] [lower-bound upper-bound]] 
           (swap! test-state conj [lower-bound upper-bound extent-state]))
         state
         (map extent->bounds (keys state)))))


#_(deftest window-fixed-test 
  (let [segments [{:id 1  :age 21 :event-time #inst "2015-09-13T03:00:00.829-00:00"}
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
		  {:id 15 :age 35 :event-time #inst "2015-09-13T03:16:00.829-00:00"}]
        task-map {}
	window {:window/id :collect-segments
		:window/task :identity
		:window/type :fixed
		:window/aggregation :onyx.windowing.aggregation/conj
		:window/window-key :event-time
		:window/range [5 :minutes]}
        triggers [{:trigger/window-id :collect-segments
                   
                   :trigger/id :sync
                   :trigger/on :onyx.triggers/segment
                   :trigger/threshold [5 :elements]
                   :trigger/sync ::update-atom!}]
	window-state (wc/build-window-executor window triggers task-map)
        reduc (vec (reductions (fn [st segment]
                                 (-> st
                                     (ws/aggregate-state segment) 
                                     (ws/triggers {:notification :new-segment
                                                   :segment segment}))) 
                               window-state 
                               segments))]

    (is (= [[1442113200000 1442113499999 
             [{:id 1 :age 21 :event-time #inst "2015-09-13T03:00:00.829-00:00"} 
              {:id 2 :age 12 :event-time #inst "2015-09-13T03:04:00.829-00:00"}]] 
            [1442113500000 1442113799999 
             [{:id 3 :age 3 :event-time #inst "2015-09-13T03:05:00.829-00:00"} 
              {:id 4 :age 64 :event-time #inst "2015-09-13T03:06:00.829-00:00"}
              {:id 5 :age 53 :event-time #inst "2015-09-13T03:07:00.829-00:00"}]] 
            [1442113200000 1442113499999
             [{:id 1 :age 21 :event-time #inst "2015-09-13T03:00:00.829-00:00"}
              {:id 2 :age 12 :event-time #inst "2015-09-13T03:04:00.829-00:00"}]] 
            [1442113500000 1442113799999 
             [{:id 3 :age 3 :event-time #inst "2015-09-13T03:05:00.829-00:00"}
              {:id 4 :age 64 :event-time #inst "2015-09-13T03:06:00.829-00:00"}
              {:id 5 :age 53 :event-time #inst "2015-09-13T03:07:00.829-00:00"}
              {:id 6 :age 52 :event-time #inst "2015-09-13T03:08:00.829-00:00"}
              {:id 7 :age 24 :event-time #inst "2015-09-13T03:09:00.829-00:00"}]] 
            [1442114100000 1442114399999 
             [{:id 8 :age 35 :event-time #inst "2015-09-13T03:15:00.829-00:00"}]] 
            [1442114700000 1442114999999 
             [{:id 9 :age 49 :event-time #inst "2015-09-13T03:25:00.829-00:00"}]]
            [1442115900000 1442116199999 
             [{:id 10 :age 37 :event-time #inst "2015-09-13T03:45:00.829-00:00"}]] 
            [1442113200000 1442113499999 
             [{:id 1 :age 21 :event-time #inst "2015-09-13T03:00:00.829-00:00"} 
              {:id 2 :age 12 :event-time #inst "2015-09-13T03:04:00.829-00:00"} 
              {:id 11 :age 15 :event-time #inst "2015-09-13T03:03:00.829-00:00"}]]
            [1442113500000 1442113799999 
             [{:id 3 :age 3 :event-time #inst "2015-09-13T03:05:00.829-00:00"} 
              {:id 4 :age 64 :event-time #inst "2015-09-13T03:06:00.829-00:00"} 
              {:id 5 :age 53 :event-time #inst "2015-09-13T03:07:00.829-00:00"} 
              {:id 6 :age 52 :event-time #inst "2015-09-13T03:08:00.829-00:00"} 
              {:id 7 :age 24 :event-time #inst "2015-09-13T03:09:00.829-00:00"}]] 
            [1442114100000 1442114399999 
             [{:id 8 :age 35 :event-time #inst "2015-09-13T03:15:00.829-00:00"}
              {:id 15 :age 35 :event-time #inst "2015-09-13T03:16:00.829-00:00"}]]
            [1442114700000 1442114999999 
             [{:id 9 :age 49 :event-time #inst "2015-09-13T03:25:00.829-00:00"}]] 
            [1442115900000 1442116199999 
             [{:id 10 :age 37 :event-time #inst "2015-09-13T03:45:00.829-00:00"}]] 
            [1442116500000 1442116799999 
             [{:id 12 :age 22 :event-time #inst "2015-09-13T03:56:00.829-00:00"} 
              {:id 13 :age 83 :event-time #inst "2015-09-13T03:59:00.829-00:00"}]] 
            [1442115000000 1442115299999 
             [{:id 14 :age 60 :event-time #inst "2015-09-13T03:32:00.829-00:00"}]]]
           @test-state))



    #_(is (= [[1442113200000 1442113499999 
	     [{:id 1 :age 21 :event-time #inst "2015-09-13T03:00:00.829-00:00"} 
	      {:id 2 :age 12 :event-time #inst "2015-09-13T03:04:00.829-00:00"} 
	      {:id 11 :age 15 :event-time #inst "2015-09-13T03:03:00.829-00:00"}]]
	    [1442113500000 1442113799999 
	     [{:id 3 :age 3 :event-time #inst "2015-09-13T03:05:00.829-00:00"} 
	      {:id 4 :age 64 :event-time #inst "2015-09-13T03:06:00.829-00:00"} 
	      {:id 5 :age 53 :event-time #inst "2015-09-13T03:07:00.829-00:00"} 
	      {:id 6 :age 52 :event-time #inst "2015-09-13T03:08:00.829-00:00"} 
	      {:id 7 :age 24 :event-time #inst "2015-09-13T03:09:00.829-00:00"}]] 
	    [1442114100000 1442114399999 
	     [{:id 8 :age 35 :event-time #inst "2015-09-13T03:15:00.829-00:00"}
	      {:id 15 :age 35 :event-time #inst "2015-09-13T03:16:00.829-00:00"}]]
	    [1442114700000 1442114999999 
	     [{:id 9 :age 49 :event-time #inst "2015-09-13T03:25:00.829-00:00"}]] 
	    [1442115900000 1442116199999 
	     [{:id 10 :age 37 :event-time #inst "2015-09-13T03:45:00.829-00:00"}]] 
	    [1442116500000 1442116799999 
	     [{:id 12 :age 22 :event-time #inst "2015-09-13T03:56:00.829-00:00"} 
	      {:id 13 :age 83 :event-time #inst "2015-09-13T03:59:00.829-00:00"}]] 
	    [1442115000000 1442115299999 
	     [{:id 14 :age 60 :event-time #inst "2015-09-13T03:32:00.829-00:00"}]]]
           (mapv (fn [[k v]]
                  (conj (we/bounds (:internal-window resolved-window) k) v))
                (ws/state (last reduc)))))))
