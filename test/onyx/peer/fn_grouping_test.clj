(ns onyx.peer.fn-grouping-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [midje.sweet :refer :all]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def env-config (assoc (:env-config config) :onyx/id id))

(def peer-config
  (assoc (:peer-config config)
    :onyx/id id
    :onyx.peer/job-scheduler :onyx.job-scheduler/round-robin))

(def env (onyx.api/start-env env-config))

(def output (atom []))

(def in-chan (chan 1000000))

(def out-chan (chan (sliding-buffer 1000000)))

(defmethod l-ext/inject-lifecycle-resources :in
  [_ _] {:core.async/chan in-chan})

(defmethod l-ext/inject-lifecycle-resources :out
  [_ _] {:core.async/chan out-chan})

(defmethod l-ext/inject-lifecycle-resources
  :onyx.peer.fn-grouping-test/sum-balance
  [_ event]
  (let [balance (atom {})]
    {:onyx.core/params [balance]
     :test/balance balance}))

(defmethod l-ext/close-lifecycle-resources
  :onyx.peer.fn-grouping-test/sum-balance
  [_ {:keys [test/balance]}]
  (swap! output conj @balance)
  {})

(defn sum-balance [state {:keys [name amount] :as segment}]
  (swap! state (fn [v] (assoc v name (+ (get v name 0) amount))))
  [])

(defn group-by-name [{:keys [name]}]
  name)

(def workflow
  [[:in :sum-balance]
   [:sum-balance :out]])

(def catalog
  [{:onyx/name :in
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size 40
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :sum-balance
    :onyx/ident :onyx.peer.fn-grouping-test/sum-balance
    :onyx/fn :onyx.peer.fn-grouping-test/sum-balance
    :onyx/type :function
    :onyx/group-by-fn :onyx.peer.fn-grouping-test/group-by-name
    :onyx/batch-size 40}

   {:onyx/name :out
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size 40
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}])

(def size 3000)

(def data
  (concat
   (map (fn [_] {:name "Mike" :amount 10}) (range size))
   (map (fn [_] {:name "Dorrene" :amount 10}) (range size))
   (map (fn [_] {:name "Benti" :amount 10}) (range size))
   (map (fn [_] {:name "John" :amount 10}) (range size))
   (map (fn [_] {:name "Shannon" :amount 10}) (range size))
   (map (fn [_] {:name "Kristen" :amount 10}) (range size))
   (map (fn [_] {:name "Benti" :amount 10}) (range size))
   (map (fn [_] {:name "Mike" :amount 10}) (range size))
   (map (fn [_] {:name "Steven" :amount 10}) (range size))
   (map (fn [_] {:name "Dorrene" :amount 10}) (range size))
   (map (fn [_] {:name "John" :amount 10}) (range size))
   (map (fn [_] {:name "Shannon" :amount 10}) (range size))
   (map (fn [_] {:name "Santana" :amount 10}) (range size))
   (map (fn [_] {:name "Roselyn" :amount 10}) (range size))
   (map (fn [_] {:name "Krista" :amount 10}) (range size))
   (map (fn [_] {:name "Starla" :amount 10}) (range size))
   (map (fn [_] {:name "Derick" :amount 10}) (range size))
   (map (fn [_] {:name "Orlando" :amount 10}) (range size))
   (map (fn [_] {:name "Rupert" :amount 10}) (range size))
   (map (fn [_] {:name "Kareem" :amount 10}) (range size))
   (map (fn [_] {:name "Lesli" :amount 10}) (range size))
   (map (fn [_] {:name "Carol" :amount 10}) (range size))
   (map (fn [_] {:name "Willie" :amount 10}) (range size))
   (map (fn [_] {:name "Noriko" :amount 10}) (range size))
   (map (fn [_] {:name "Corine" :amount 10}) (range size))
   (map (fn [_] {:name "Leandra" :amount 10}) (range size))
   (map (fn [_] {:name "Chadwick" :amount 10}) (range size))
   (map (fn [_] {:name "Teressa" :amount 10}) (range size))
   (map (fn [_] {:name "Tijuana" :amount 10}) (range size))
   (map (fn [_] {:name "Verna" :amount 10}) (range size))
   (map (fn [_] {:name "Alona" :amount 10}) (range size))
   (map (fn [_] {:name "Wilson" :amount 10}) (range size))
   (map (fn [_] {:name "Carly" :amount 10}) (range size))
   (map (fn [_] {:name "Nubia" :amount 10}) (range size))
   (map (fn [_] {:name "Hollie" :amount 10}) (range size))
   (map (fn [_] {:name "Allison" :amount 10}) (range size))
   (map (fn [_] {:name "Edwin" :amount 10}) (range size))
   (map (fn [_] {:name "Zola" :amount 10}) (range size))
   (map (fn [_] {:name "Britany" :amount 10}) (range size))
   (map (fn [_] {:name "Courtney" :amount 10}) (range size))
   (map (fn [_] {:name "Mathew" :amount 10}) (range size))
   (map (fn [_] {:name "Luz" :amount 10}) (range size))
   (map (fn [_] {:name "Tyesha" :amount 10}) (range size))
   (map (fn [_] {:name "Eusebia" :amount 10}) (range size))
   (map (fn [_] {:name "Fletcher" :amount 10}) (range size))))

(doseq [x data]
  (>!! in-chan x))

(>!! in-chan :done)
(close! in-chan)

(def v-peers (onyx.api/start-peers 3 peer-config))

(onyx.api/submit-job
 peer-config
 {:catalog catalog :workflow workflow
  :task-scheduler :onyx.task-scheduler/round-robin})

(def results (take-segments! out-chan))

(def out-val @output)

;;; Scan the key set, dropping any nils. Count the distinct keys.
;;; Do the same for the right hand side of the expression, but turn it into a set.
;;; If there's the same number of elements, then the grouping was mutually exclusive.
(fact (count (filter identity (mapcat keys out-val))) =>
      (count (into #{} (filter identity (mapcat keys out-val)))))

(fact results => [:done])

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)

