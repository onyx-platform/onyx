(ns onyx.peer.zookeeper-failover-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer take! put!] :as async]
            [clojure.test :refer [deftest is testing]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config with-test-env]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.api])
  (:import [org.apache.curator.test TestingCluster InstanceSpec]
           [org.apache.zookeeper ServerAdminClient]))

(def n-messages 100)

(defn my-inc [{:keys [n] :as segment}]
  (Thread/sleep 10)
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

(defn ruok? [^InstanceSpec instance]
  (let [[hostname port] (clojure.string/split (.getConnectString ^InstanceSpec instance) #":")
        ret (with-out-str (ServerAdminClient/ruok hostname (Integer/parseInt port)))]
    (.contains ^java.lang.String
               ret
               "retv=imok")))

(defn kill-server-for [cluster server ms]
  (future
    (Thread/sleep ms)
    (.restartServer ^TestingCluster cluster server))
  (.killServer ^TestingCluster cluster server))


(defn cluster-conn-string [cluster]
  (let [[one two three] (.getInstances ^TestingCluster cluster)]
    (str (.getConnectString ^InstanceSpec one) ","
         (.getConnectString ^InstanceSpec two) ","
         (.getConnectString ^InstanceSpec three))))

(defn inject-conn-config [config conn-str]
  (-> config
      (assoc-in [:env-config :zookeeper/address] conn-str)
      (assoc-in [:env-config :zookeeper/server?] false)
      (update-in [:env-config] dissoc :zookeeper.server/port)
      (assoc-in [:peer-config :zookeeper/address] conn-str)))

(deftest monitoring-test
  (with-open [cluster (TestingCluster. 3)]
    (.start cluster)
    (let [id (random-uuid)
          [one two three] (.getInstances ^TestingCluster cluster)
          zk-conn-str (cluster-conn-string cluster)
          config (inject-conn-config (load-config) zk-conn-str)
          env-config (assoc (:env-config config) :onyx/tenancy-id id)
          batch-size 1
          catalog [{:onyx/name :in
                    :onyx/plugin :onyx.plugin.core-async/input
                    :onyx/type :input
                    :onyx/medium :core.async
                    :onyx/batch-size batch-size
                    :onyx/max-peers 1
                    :onyx/doc "Reads segments from a core.async channel"}

                   {:onyx/name :inc
                    :onyx/fn ::my-inc
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
                       :lifecycle/calls ::in-calls}
                      {:lifecycle/task :out
                       :lifecycle/calls ::out-calls}]
          peer-config (assoc (:peer-config config) :onyx/tenancy-id id)]

      (reset! in-chan (chan))
      (reset! in-buffer {})
      (reset! out-chan (chan))
      (with-test-env [test-env [3 env-config peer-config]]
        (let [{:keys [job-id]} (onyx.api/submit-job peer-config
                                                    {:catalog catalog
                                                     :workflow workflow
                                                     :lifecycles lifecycles
                                                     :task-scheduler :onyx.task-scheduler/balanced})]
          (testing "That Zookeeper with the ensemble test client works."
            (async/go-loop [n 0]
              (when (< n 5)
                (async/>! @in-chan {:n n})
                (recur (inc n))))
            (is (= 5 (count (first
                             (async/alts!! [(async/timeout 1000)
                                            (async/go-loop [n 0 acc []]
                                              (if (< n 5)
                                                (recur (inc n)
                                                       (conj acc (async/<! @out-chan)))
                                                acc))]))))))

          (testing "That restarting a ZK server allows messages to continue processing"
            (println "Killing Server One")
            (is (kill-server-for cluster one 5000))
            (async/go-loop [n 0]
              (when (< n 1000)
                (async/>! @in-chan {:n n})
                (recur (inc n))))

            (is (= 1000 (count (first
                                (async/alts!! [(async/timeout 15000)
                                               (async/go-loop [n 0 acc []]
                                                 (if (< n 1000)
                                                   (recur (inc n)
                                                          (conj acc (async/<! @out-chan)))
                                                   acc))])))))
            (while (not (ruok? one))
              (Thread/sleep 1000)
              (println "Waiting on server one to be ok.")))


          (testing "That restarting a ZK server allows messages to continue processing"
            (println "Killing Server Two")
            (is (kill-server-for cluster two 5000))
            (async/go-loop [n 0]
              (when (< n 1000)
                (async/>! @in-chan {:n n})
                (recur (inc n))))

            (is (= 1000 (count (first
                                (async/alts!! [(async/timeout 15000)
                                               (async/go-loop [n 0 acc []]
                                                 (if (< n 1000)
                                                   (recur (inc n)
                                                          (conj acc (async/<! @out-chan)))
                                                   acc))])))))
            (while (not (ruok? two))
              (Thread/sleep 1000)
              (println "Waiting on server two to be ok.")))


          (testing "That restarting a ZK server allows messages to continue processing"
            (println "Killing Server Three")
            (is (kill-server-for cluster three 5000))
            (async/go-loop [n 0]
              (when (< n 1000)
                (async/>! @in-chan {:n n})
                (recur (inc n))))

            (is (= 1000 (count (first
                                (async/alts!! [(async/timeout 15000)
                                               (async/go-loop [n 0 acc []]
                                                 (if (< n 1000)
                                                   (recur (inc n)
                                                          (conj acc (async/<! @out-chan)))
                                                   acc))])))))
            (while (not (ruok? three))
              (Thread/sleep 1000)
              (println "Waiting on server three to be ok.")))
          (close! @in-chan))))))
