(ns onyx.peer.fault-tolerance
  (:require [onyx.helper-env :as helper-env :refer [->TestEnv map->TestEnv]]
            [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [midje.sweet :refer :all]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.extensions :as extensions]
            [com.stuartsierra.component :as component]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config]]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def config (load-config))

(def test-env (component/start (->TestEnv (:env-config config) (:peer-config config))))

(def batch-size 5)

(def process-middle identity)

(def catalog
  [{:onyx/name :in
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :inc
    :onyx/fn :onyx.peer.fault-tolerance/process-middle
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :out
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/doc "Writes segments to a core.async channel"}])

(def workflow [[:in :inc] [:inc :out]])

(def in-chan (chan (inc n-messages)))

(def out-chan (chan (inc n-messages)))

(defmethod l-ext/inject-lifecycle-resources :in
  [_ _] {:core.async/chan in-chan})

(defmethod l-ext/inject-lifecycle-resources :out
  [_ _] {:core.async/chan out-chan})

(def incomplete 
  (atom #{}))

(def n-messages-total 10000)

(def load-data-fut (future 
                     (loop [ls (repeatedly n-messages-total (fn [] {:id (java.util.UUID/randomUUID)}))]
                       (let [segment (first ls)]
                         (swap! incomplete conj segment)
                         (>!! in-chan segment)
                         (recur (rest ls))))
                     (>!! in-chan :done)
                     (close! in-chan)))

(try
  (helper-env/add-peers test-env 5)
  (let [job-id  (:job-id (helper-env/run-job test-env 
                                             {:catalog catalog
                                              :workflow workflow
                                              :task-scheduler :onyx.task-scheduler/balanced}))]

    (fact (set (take-segments! out-chan)) => (conj @incomplete :done))
    (finally 
      (component/stop test-env))))

