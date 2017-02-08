(ns onyx.peer.multi-input-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config with-test-env]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.api]))

(def n-messages 15000)

(def in-chan-1 (atom nil))
(def in-buffer-1 (atom nil))

(def in-chan-2 (atom nil))
(def in-buffer-2 (atom nil))

(def in-chan-3 (atom nil))
(def in-buffer-3 (atom nil))

(def in-chan-4 (atom nil))
(def in-buffer-4 (atom nil))

(def out-chan (atom nil))

(def messages
  (->> 4
       (inc)
       (range)
       (map (fn [k] (* k (/ n-messages 4))))
       (partition 2 1)
       (map (partial apply range))
       (map (fn [r] (map (fn [x] {:n x}) r)))))

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(defn inject-in-1-ch [event lifecycle]
  {:core.async/buffer in-buffer-1
   :core.async/chan @in-chan-1})

(defn inject-in-2-ch [event lifecycle]
  {:core.async/buffer in-buffer-2
   :core.async/chan @in-chan-2})

(defn inject-in-3-ch [event lifecycle]
  {:core.async/buffer in-buffer-3
   :core.async/chan @in-chan-3})

(defn inject-in-4-ch [event lifecycle]
  {:core.async/buffer in-buffer-4
   :core.async/chan @in-chan-4})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan @out-chan})

(def in-1-calls
  {:lifecycle/before-task-start inject-in-1-ch})

(def in-2-calls
  {:lifecycle/before-task-start inject-in-2-ch})

(def in-3-calls
  {:lifecycle/before-task-start inject-in-3-ch})

(def in-4-calls
  {:lifecycle/before-task-start inject-in-4-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(deftest multi-input-test
  (let [id (random-uuid)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id id)
        peer-config (assoc (:peer-config config) :onyx/tenancy-id id)
        batch-size 20
        catalog [{:onyx/name :in-1
                  :onyx/plugin :onyx.plugin.core-async/input
                  :onyx/type :input
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1
                  :onyx/doc "Reads segments from a core.async channel"}

                 {:onyx/name :in-2
                  :onyx/plugin :onyx.plugin.core-async/input
                  :onyx/type :input
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1
                  :onyx/doc "Reads segments from a core.async channel"}

                 {:onyx/name :in-3
                  :onyx/plugin :onyx.plugin.core-async/input
                  :onyx/type :input
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1
                  :onyx/doc "Reads segments from a core.async channel"}

                 {:onyx/name :in-4
                  :onyx/plugin :onyx.plugin.core-async/input
                  :onyx/type :input
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1
                  :onyx/doc "Reads segments from a core.async channel"}

                 {:onyx/name :inc
                  :onyx/fn :onyx.peer.multi-input-test/my-inc
                  :onyx/type :function
                  :onyx/batch-size batch-size}

                 {:onyx/name :out
                  :onyx/plugin :onyx.plugin.core-async/output
                  :onyx/type :output
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1
                  :onyx/doc "Writes segments to a core.async channel"}]

        workflow [[:in-1 :inc]
                  [:in-2 :inc]
                  [:in-3 :inc]
                  [:in-4 :inc]
                  [:inc :out]]

        lifecycles [{:lifecycle/task :in-1
                     :lifecycle/calls :onyx.peer.multi-input-test/in-1-calls}
                    {:lifecycle/task :in-2
                     :lifecycle/calls :onyx.peer.multi-input-test/in-2-calls}
                    {:lifecycle/task :in-3
                     :lifecycle/calls :onyx.peer.multi-input-test/in-3-calls}
                    {:lifecycle/task :in-4
                     :lifecycle/calls :onyx.peer.multi-input-test/in-4-calls}
                    {:lifecycle/task :out
                     :lifecycle/calls :onyx.peer.multi-input-test/out-calls}]]

    (reset! in-chan-1 (chan (inc n-messages)))
    (reset! in-buffer-1 {})
    (reset! in-chan-2 (chan (inc n-messages)))
    (reset! in-buffer-2 {})
    (reset! in-chan-3 (chan (inc n-messages)))
    (reset! in-buffer-3 {})
    (reset! in-chan-4 (chan (inc n-messages)))
    (reset! in-buffer-4 {})
    (reset! out-chan (chan (sliding-buffer (inc n-messages))))

    (doseq [[q b] (map (fn [q b] [q b]) [in-chan-1 in-chan-2 in-chan-3 in-chan-4] messages)]
      (doseq [x b]
        (>!! @q x)))

    (close! @in-chan-1)
    (close! @in-chan-2)
    (close! @in-chan-3)
    (close! @in-chan-4)

    (with-test-env [test-env [6 env-config peer-config]]
      
      (let [{:keys [job-id]} (onyx.api/submit-job peer-config
                                                  {:catalog catalog :workflow workflow
                                                   :lifecycles lifecycles
                                                   :task-scheduler :onyx.task-scheduler/balanced})
            _ (onyx.test-helper/feedback-exception! peer-config job-id)
            results (take-segments! @out-chan 50)
            expected (set (map (fn [x] {:n (inc x)}) (range n-messages)))]
        (is (= expected (set results)))))))
