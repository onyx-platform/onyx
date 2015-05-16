(ns onyx.peer.fault-tolerance
  (:require [onyx.helper-env :as helper-env :refer [->TestEnv map->TestEnv]]
            [clojure.core.async :refer [chan >!! <!! close! go-loop timeout thread alts!! sliding-buffer]]
            [midje.sweet :refer :all]
            [onyx.extensions :as extensions]
            [com.stuartsierra.component :as component]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config]]
            [onyx.api]))

(def config (load-config))

(def batch-size 5)

(defn int-1 [segment]
  (assoc segment :a 1))

(defn int-2 [segment]
  (assoc segment :b 1))

(def batch-timeout 500)

(def catalog
  [{:onyx/name :in
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-timeout batch-timeout
    :onyx/max-peers 1
    :onyx/batch-size batch-size
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :intermediate-1
    :onyx/fn :onyx.peer.fault-tolerance/int-1
    :onyx/batch-timeout batch-timeout
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :intermediate-2
    :onyx/fn :onyx.peer.fault-tolerance/int-2
    :onyx/type :function
    :onyx/batch-timeout batch-timeout
    :onyx/batch-size batch-size}

   {:onyx/name :out
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/batch-timeout batch-timeout
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/doc "Writes segments to a core.async channel"}])

(def workflow [[:in :intermediate-1] 
               [:intermediate-1 :intermediate-2] 
               [:intermediate-2 :out]])

(def chan-size 10000)

(def in-chan (chan (inc chan-size)))

(def out-chan (chan (inc chan-size)))

(defn inject-in-ch [event lifecycle]
  {:core.async/chan in-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan out-chan})

(def in-calls
  {:lifecycle/before-task-start :onyx.peer.fault-tolerance/inject-in-ch})

(def out-calls
  {:lifecycle/before-task-start :onyx.peer.fault-tolerance/inject-out-ch})

(def lifecycles
  [{:lifecycle/task :in
    :lifecycle/calls :onyx.peer.fault-tolerance/in-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.peer.fault-tolerance/out-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(def incomplete 
  (atom #{}))

(def bad-values
  (atom #{}))

(def n-messages-total 100000)

(when false
  (def test-env (component/start (->TestEnv (:env-config config) 
                                            (:peer-config config))))


  (try
    (helper-env/add-peers test-env 9)
    (let [load-data-ch (thread 
                         (loop [ls (repeatedly n-messages-total (fn [] {:id (java.util.UUID/randomUUID)}))]
                           (when-let [segment (first ls)]
                             (swap! incomplete conj (int-2 (int-1 segment)))
                             (>!! in-chan segment)
                             (recur (rest ls))))
                         (>!! in-chan :done)
                         (close! in-chan))

          job-id (:job-id (helper-env/run-job test-env 
                                              {:catalog catalog
                                               :workflow workflow
                                               :lifecycles lifecycles
                                               :task-scheduler :onyx.task-scheduler/balanced}))
          _ (Thread/sleep 4000)
          task-peers (-> test-env
                         :replica
                         deref
                         :allocations
                         (get job-id))

          task-ids (->> task-peers
                        keys
                        (map (partial extensions/read-chunk (:log (:env test-env)) :task))
                        (map (juxt :name :id))
                        (into {}))

          non-input-task-ids (set (vals (dissoc task-ids :in)))

          remove-completed-ch (go-loop []
                                       (let [v (<!! out-chan)]
                                         (println "got " v)
                                         (when (and v (not= v :done)) 
                                           (swap! incomplete disj v)
                                           (when (or (nil? (:a v))
                                                     (nil? (:b v)))
                                             (swap! bad-values conj v))
                                           (recur))))
          kill-ch (chan 1)
          chaos-ch (go-loop []
                            (let [; average the the full circuit worth of timeouts
                                  chaos-kill-ms (rand-int (* 3 batch-timeout 2))
                                  [v ch] (alts!! [(timeout chaos-kill-ms) kill-ch])]
                              (when-not (and (= ch kill-ch) v) 
                                (try 
                                  (when-let [non-input-peers (->> (get (:allocations @(:replica test-env)) job-id)
                                                                  (filter (fn [[task-id peers]]
                                                                            (and (> (count peers) 1)
                                                                                 (non-input-task-ids task-id))))
                                                                  vals
                                                                  flatten
                                                                  seq)]
                                    (when-let [peer-val (helper-env/lookup-peer test-env (rand-nth non-input-peers))] 
                                      (println "Killing a peer")
                                      (helper-env/remove-peer test-env peer-val)
                                      (helper-env/add-peers test-env 1)))
                                  (catch Exception e
                                    (println "FAILURE MANAGING: " e)))
                                (recur))))]
      (<!! remove-completed-ch)
      (println "All done")
      (fact @incomplete => #{})
      (let [after-done (loop [after-done #{}]
                         (let [[v ch] (alts!! [out-chan (timeout 10000)])]
                           (if v 
                             (recur (conj after-done v))
                             after-done)))]
        (fact after-done => #{}))
      (fact @bad-values => #{})
      (close! kill-ch)
      #_(<!! mess-with-peers-ch))
    (finally 
      (component/stop test-env))))
