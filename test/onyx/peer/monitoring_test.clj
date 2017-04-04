(ns onyx.peer.monitoring-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config with-test-env]]
            [onyx.static.uuid :refer [random-uuid]]
            [clojure.java.jmx :as jmx]
            [onyx.api]))

(def n-messages 100)

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

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

(def state (atom {}))

(deftest monitoring-test
  (let [id (random-uuid)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id id)
        batch-size 20
        catalog [{:onyx/name :in
                  :onyx/plugin :onyx.plugin.core-async/input
                  :onyx/type :input
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1
                  :onyx/doc "Reads segments from a core.async channel"}

                 {:onyx/name :inc
                  :onyx/fn :onyx.peer.monitoring-test/my-inc
                  :onyx/type :function
                  :onyx/batch-size batch-size}

                 {:onyx/name :out
                  :onyx/plugin :onyx.plugin.core-async/output
                  :onyx/type :output
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1
                  :onyx/doc "Writes segments to a core.async channel"}]
        workflow [[:in :inc] [:inc :out]]
        lifecycles [{:lifecycle/task :in
                     :lifecycle/calls :onyx.peer.monitoring-test/in-calls}
                    {:lifecycle/task :out
                     :lifecycle/calls :onyx.peer.monitoring-test/out-calls}]
        peer-config (assoc (:peer-config config) :onyx/tenancy-id id)]

    (reset! in-chan (chan (inc n-messages)))
    (reset! in-buffer {})
    (reset! out-chan (chan (sliding-buffer (inc n-messages))))

    (with-test-env [test-env [3 env-config peer-config]]
      (doseq [n (range n-messages)]
        (>!! @in-chan {:n n}))
      (let [{:keys [job-id]} (onyx.api/submit-job peer-config
                                                  {:catalog catalog
                                                   :workflow workflow
                                                   :lifecycles lifecycles
                                                   :task-scheduler :onyx.task-scheduler/balanced})
            ;; wait for job to fully start up before counting our metrics
            _ (Thread/sleep 1000)
            _ (is (> (count (jmx/mbean-names "org.onyxplatform:*")) 50))
            _ (doseq [mbean (jmx/mbean-names "org.onyxplatform:*")] 
                (doseq [attribute (jmx/attribute-names mbean)]
                  (try 
                   (let [value (jmx/read mbean attribute)] 
                     (println (.getCanonicalKeyPropertyListString ^javax.management.ObjectName mbean) 
                              attribute 
                              value))
                   ;; Safe to swallow
                   (catch javax.management.RuntimeMBeanException _))))
            ;; close after we've counted
            _ (close! @in-chan)
            _ (onyx.test-helper/feedback-exception! peer-config job-id)
            results (take-segments! @out-chan 50)
            expected (set (map (fn [x] {:n (inc x)}) (range n-messages)))]
        (is (= expected (set results))))))) 
