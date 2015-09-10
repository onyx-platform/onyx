(ns onyx.peer.kw-grouping-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config]]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def config (load-config))

(def env-config (assoc (:env-config config) :onyx/id id))

(def peer-config
  (assoc (:peer-config config)
    :onyx/id id
    :onyx.peer/job-scheduler :onyx.job-scheduler/balanced))

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def output (atom {}))

(def in-chan (chan 1000000))

(def out-chan (chan (sliding-buffer 1000000)))

(defn inject-sum-state [event lifecycle]
  (let [balance (atom {})]
    {:onyx.core/params [balance]
     :test/balance balance
     :test/id (:onyx.core/id event)}))

(defn flush-sum-state [{:keys [test/balance test/id] :as event} lifecycle]
  (swap! output (fn [hm item]
                  (assoc hm id item)) @balance))

(defn sum-balance [state {:keys [name first-name amount] :as segment}]
  (let [name (or name first-name [:first-name :first_name])]
    (swap! state (fn [v]
                   (assoc v name (+ (get v name 0) amount))))
    []))

(def workflow
  [[:in :sum-balance]
   [:sum-balance :out]])

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.core-async/input
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size 40
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :sum-balance
    :onyx/plugin :onyx.peer.kw-grouping-test/sum-balance
    :onyx/fn :onyx.peer.kw-grouping-test/sum-balance
    :onyx/type :function
    :onyx/group-by-key [:name :first-name [:first-name :first_name]]
    :onyx/min-peers 2
    :onyx/flux-policy :kill
    :onyx/batch-size 40}

   {:onyx/name :out
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size 40
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}])

(defn inject-in-ch [event lifecycle]
  {:core.async/chan in-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan out-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def sum-calls
  {:lifecycle/before-task-start inject-sum-state
   :lifecycle/after-task-stop flush-sum-state})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(def lifecycles
  [{:lifecycle/task :in
    :lifecycle/calls :onyx.peer.kw-grouping-test/in-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :sum-balance
    :lifecycle/calls :onyx.peer.kw-grouping-test/sum-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.peer.kw-grouping-test/out-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(def size 3000)

(def data
  (shuffle
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
    (map (fn [_] {:name "Fletcher" :amount 10}) (range size))
    (map (fn [_] {:first-name "Trey" :amount 10}) (range size))
    (map (fn [_] {:first-name "Jon" :amount 10}) (range size))
    (map (fn [_] {[:first-name :first_name] "JimBob" :amount 10}) (range size)))))

(doseq [x data]
  (>!! in-chan x))

(>!! in-chan :done)
(close! in-chan)

(def v-peers (onyx.api/start-peers 4 peer-group))

(onyx.api/submit-job
 peer-config
 {:catalog catalog :workflow workflow
  :lifecycles lifecycles
  :task-scheduler :onyx.task-scheduler/balanced})

(def results (take-segments! out-chan))

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(def out-val @output)

(fact (not (empty? out-val)))

;;;; We flush out each result set based on the peer id, then we do a diff. If the
;;;; last collection is empty, we know that each map is mutually exclusive.

(let [l (first (vals out-val))
      r (second (vals out-val))]
  (fact (empty? (last (clojure.data/diff l r)))))

(fact results => [:done])

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)
