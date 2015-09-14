(ns onyx.peer.multi-input-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config]]
            [onyx.api]))

(def n-messages 15000)

(def in-chan-1 (chan (inc n-messages)))

(def in-chan-2 (chan (inc n-messages)))

(def in-chan-3 (chan (inc n-messages)))

(def in-chan-4 (chan (inc n-messages)))

(def out-chan (chan (sliding-buffer (inc n-messages))))

(def messages
  (->> 4
       (inc)
       (range)
       (map (fn [k] (* k (/ n-messages 4))))
       (partition 2 1)
       (map (partial apply range))
       (map (fn [r] (map (fn [x] {:n x}) r)))))

(doseq [[q b] (map (fn [q b] [q b]) [in-chan-1 in-chan-2 in-chan-3 in-chan-4] messages)]
  (>!! q b))

(>!! in-chan-1 :done)
(>!! in-chan-2 :done)
(>!! in-chan-3 :done)
(>!! in-chan-4 :done)

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(defn inject-in-1-ch [event lifecycle]
  {:core.async/chan in-chan-1})

(defn inject-in-2-ch [event lifecycle]
  {:core.async/chan in-chan-2})

(defn inject-in-3-ch [event lifecycle]
  {:core.async/chan in-chan-3})

(defn inject-in-4-ch [event lifecycle]
  {:core.async/chan in-chan-4})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan out-chan})

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
  (let [id (java.util.UUID/randomUUID)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/id id)
        peer-config (assoc (:peer-config config) :onyx/id id)
        env (onyx.api/start-env env-config)
        peer-group (onyx.api/start-peer-group peer-config)
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
                    {:lifecycle/task :in-1
                     :lifecycle/calls :onyx.plugin.core-async/reader-calls}
                    {:lifecycle/task :in-2
                     :lifecycle/calls :onyx.peer.multi-input-test/in-2-calls}
                    {:lifecycle/task :in-2
                     :lifecycle/calls :onyx.plugin.core-async/reader-calls}
                    {:lifecycle/task :in-3
                     :lifecycle/calls :onyx.peer.multi-input-test/in-3-calls}
                    {:lifecycle/task :in-3
                     :lifecycle/calls :onyx.plugin.core-async/reader-calls}
                    {:lifecycle/task :in-4
                     :lifecycle/calls :onyx.peer.multi-input-test/in-4-calls}
                    {:lifecycle/task :in-4
                     :lifecycle/calls :onyx.plugin.core-async/reader-calls}
                    {:lifecycle/task :out
                     :lifecycle/calls :onyx.peer.multi-input-test/out-calls}
                    {:lifecycle/task :out
                     :lifecycle/calls :onyx.plugin.core-async/writer-calls}]

        v-peers (onyx.api/start-peers 6 peer-group)

        _ (onyx.api/submit-job peer-config
                               {:catalog catalog :workflow workflow
                                :lifecycles lifecycles
                                :task-scheduler :onyx.task-scheduler/balanced})

        results (take-segments! out-chan)]

    (let [expected (set (map (fn [x] {:n (inc x)}) (range n-messages)))]
      (is (= (set (butlast results)) expected))
      (is (= (last results) :done)))

    (doseq [v-peer v-peers]
      (onyx.api/shutdown-peer v-peer))

    (onyx.api/shutdown-peer-group peer-group)

    (onyx.api/shutdown-env env))) 
