(ns onyx.peer.kw-grouping-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config with-test-env]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.api]))

(def output (atom {}))

(def in-chan (atom nil))
(def in-buffer (atom nil))

(def out-chan (atom nil))

(defn inject-sum-state [event lifecycle]
  (let [balance (atom {})]
    {:onyx.core/params [balance]
     :test/balance balance
     :test/id (:id event)}))

(defn flush-sum-state [{:keys [test/balance test/id] :as event} lifecycle]
  (swap! output (fn [hm item]
                  (assoc hm id item)) @balance))

(defn sum-balance [state {:keys [name first-name amount] :as segment}]
  (let [name (or name first-name [:first-name :first_name])]
    (swap! state (fn [v]
                   (assoc v name (+ (get v name 0) amount))))
    []))

(defn inject-in-ch [event lifecycle]
  {:core.async/buffer in-buffer
   :core.async/chan @in-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan @out-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def sum-calls
  {:lifecycle/before-task-start inject-sum-state
   :lifecycle/after-task-stop flush-sum-state})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(deftest kw-grouping
  (let [id (random-uuid)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id id)
        peer-config (assoc (:peer-config config)
                           :onyx/tenancy-id id
                           :onyx.peer/job-scheduler :onyx.job-scheduler/balanced)
        workflow [[:in :sum-balance]
                  [:sum-balance :out]]

        catalog [{:onyx/name :in
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
                  :onyx/doc "Writes segments to a core.async channel"}]

        lifecycles [{:lifecycle/task :in
                     :lifecycle/calls :onyx.peer.kw-grouping-test/in-calls}
                    {:lifecycle/task :sum-balance
                     :lifecycle/calls :onyx.peer.kw-grouping-test/sum-calls}
                    {:lifecycle/task :out
                     :lifecycle/calls :onyx.peer.kw-grouping-test/out-calls}]

        size 3000
        data (shuffle
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
                 (map (fn [_] {[:first-name :first_name] "JimBob" :amount 10}) (range size))))]

    (reset! in-chan (chan 1000000))
    (reset! in-buffer {})
    (reset! out-chan (chan (sliding-buffer 1000000)))

    (with-test-env [test-env [4 env-config peer-config]]
      (doseq [x data]
        (>!! @in-chan x))
      (close! @in-chan)
      (let [job-id (:job-id (onyx.api/submit-job peer-config
                                                 {:catalog catalog :workflow workflow
                                                  :lifecycles lifecycles
                                                  :task-scheduler :onyx.task-scheduler/balanced}))
            _ (onyx.test-helper/feedback-exception! peer-config job-id)
            results (take-segments! @out-chan 50)]
        (is (= [] results))))

    ;; check outside the peer shutdown so that we can ensure task is fully stopped
    (let [out-val @output]
      (is (not (empty? out-val)))

      ;; We flush out each result set based on the peer id, then we do a diff. If the
      ;; last collection is empty, we know that each map is mutually exclusive.
      (let [l (first (vals out-val))
            r (second (vals out-val))]

        (is (empty? (last (clojure.data/diff l r))))))))
