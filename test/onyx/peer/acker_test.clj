(ns onyx.peer.acker-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [midje.sweet :refer :all]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.plugin.core-async]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def env-config (assoc (:env-config config) :onyx/id id))

(def peer-config (assoc (:peer-config config) :onyx/id id))

(def env (onyx.api/start-env env-config))

(def n-messages 1000)

(def batch-size 40)

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(defn my-identity [segment]
  segment)

(def catalog
  [{:onyx/name :in
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :inc
    :onyx/fn :onyx.peer.acker-test/my-inc
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :identity
    :onyx/fn :onyx.peer.acker-test/my-identity
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :out
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}])

(def workflow
  [[:in :inc]
   [:inc :identity]
   [:identity :out]])

(def in-chan (chan (inc n-messages)))

(def out-chan (chan (sliding-buffer (inc n-messages))))

(defmethod l-ext/inject-lifecycle-resources :in
  [_ _] {:core.async/chan in-chan})

(defmethod l-ext/inject-lifecycle-resources :out
  [_ _] {:core.async/chan out-chan})

(doseq [n (range n-messages)]
  (>!! in-chan {:n n}))

(>!! in-chan :done)

(def v-peers (onyx.api/start-peers 4 peer-config))

(onyx.api/submit-job
 peer-config
 {:catalog catalog
  :workflow workflow
  :task-scheduler :onyx.task-scheduler/round-robin
  :acker/percentage 5
  :acker/exempt-input-tasks? true
  :acker/exempt-output-tasks? true
  :acker/exempt-tasks [:inc]})

(def results (doall (repeatedly (inc n-messages) (fn [] (<!! out-chan)))))

(let [expected (set (map (fn [x] {:n (inc x)}) (range n-messages)))]
  (fact (set (butlast results)) => expected)
  (fact (last results) => :done))

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)

