(ns onyx.peer.killed-job-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [midje.sweet :refer :all]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config]]
            [onyx.extensions :as extensions]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def config (load-config))

(def n-messages 15000)

(def batch-size 1320)

(def env-config (assoc (:env-config config) :onyx/id id))

(def peer-config (assoc (:peer-config config) :onyx/id id))

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def in-chan-1 (chan (inc n-messages)))

(def out-chan-1 (chan (sliding-buffer (inc n-messages))))

(def in-chan-2 (chan (inc n-messages)))

(def out-chan-2 (chan (sliding-buffer (inc n-messages))))

(try
  ;;; Don't write any segments to j1 so that the job will stay alive until we kill it.
  (doseq [n (range n-messages)]
    (>!! in-chan-2 {:n n}))

  (>!! in-chan-2 :done)

  (defn my-inc [{:keys [n] :as segment}]
    (assoc segment :n (inc n)))

  (def catalog-1
    [{:onyx/name :in-1
      :onyx/ident :core.async/read-from-chan
      :onyx/type :input
      :onyx/medium :core.async
      :onyx/batch-size batch-size
      :onyx/max-peers 1
      :onyx/doc "Reads segments from a core.async channel"}

     {:onyx/name :inc
      :onyx/fn :onyx.peer.killed-job-test/my-inc
      :onyx/type :function
      :onyx/batch-size batch-size}

     {:onyx/name :out-1
      :onyx/ident :core.async/write-to-chan
      :onyx/type :output
      :onyx/medium :core.async
      :onyx/batch-size batch-size
      :onyx/max-peers 1
      :onyx/doc "Writes segments to a core.async channel"}])

  (def catalog-2
    [{:onyx/name :in-2
      :onyx/ident :core.async/read-from-chan
      :onyx/type :input
      :onyx/medium :core.async
      :onyx/batch-size batch-size
      :onyx/max-peers 1
      :onyx/doc "Reads segments from a core.async channel"}

     {:onyx/name :inc
      :onyx/fn :onyx.peer.killed-job-test/my-inc
      :onyx/type :function
      :onyx/batch-size batch-size}

     {:onyx/name :out-2
      :onyx/ident :core.async/write-to-chan
      :onyx/type :output
      :onyx/medium :core.async
      :onyx/batch-size batch-size
      :onyx/max-peers 1
      :onyx/doc "Writes segments to a core.async channel"}])

(defmethod l-ext/inject-lifecycle-resources :in-1
  [_ _] {:core.async/chan in-chan-1})

(defmethod l-ext/inject-lifecycle-resources :out-1
  [_ _] {:core.async/chan out-chan-1})

(defmethod l-ext/inject-lifecycle-resources :in-2
  [_ _] {:core.async/chan in-chan-2})

(defmethod l-ext/inject-lifecycle-resources :out-2
  [_ _] {:core.async/chan out-chan-2})



  (def workflow-1 [[:in-1 :inc] [:inc :out-1]])

  (def workflow-2 [[:in-2 :inc] [:inc :out-2]])

  (def v-peers (onyx.api/start-peers 3 peer-group))

  (def j1 (:job-id (onyx.api/submit-job
            peer-config
            {:catalog catalog-1 :workflow workflow-1
             :task-scheduler :onyx.task-scheduler/balanced})))

  (def j2 (:job-id (onyx.api/submit-job
                     peer-config
                     {:catalog catalog-2 :workflow workflow-2
                      :task-scheduler :onyx.task-scheduler/balanced})))

  (onyx.api/kill-job peer-config j1)

  (def results (take-segments! out-chan-2))

  (def ch (chan 100))

  ;; Make sure we find the killed job in the replica, then bail
  (loop [replica (extensions/subscribe-to-log (:log env) ch)]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)]
      (when-not (= (first (:killed-jobs new-replica)) j1)
        (recur new-replica))))

  (let [expected (set (map (fn [x] {:n (inc x)}) (range n-messages)))]
    (fact (set (butlast results)) => expected)
    (fact (last results) => :done))
  (finally 
    (doseq [v-peer v-peers]
      (onyx.api/shutdown-peer v-peer))

    (onyx.api/shutdown-peer-group peer-group)
    
    (onyx.api/shutdown-env env)))
