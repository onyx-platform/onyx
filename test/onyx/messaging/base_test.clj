(ns onyx.messaging.base-test
  (:require [clojure.core.async :refer [chan >!! <!!]]
            [midje.sweet :refer :all]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.plugin.core-async]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def scheduler :onyx.job-scheduler/greedy)

(def messaging :http-kit)

(def env-config
  {:zookeeper/address (:address (:zookeeper config))
   :zookeeper/server? true
   :zookeeper.server/port (:spawn-port (:zookeeper config))
   :onyx/id id
   :onyx.peer/job-scheduler scheduler
   :onyx.messaging/impl messaging})

(def peer-config
  {:zookeeper/address (:address (:zookeeper config))
   :onyx/id id
   :onyx.peer/inbox-capacity (:inbox-capacity (:peer config))
   :onyx.peer/outbox-capacity (:outbox-capacity (:peer config))
   :onyx.peer/join-failure-back-off 500
   :onyx.peer/job-scheduler scheduler
   :onyx.messaging/impl messaging})

(def env (onyx.api/start-env env-config))

(def n-messages 100)

(def batch-size 10)

(defn my-inc [segment]
  (prn "-->" segment)
  (assoc segment :n (inc (:n segment))))

(def catalog
  [{:onyx/name :in
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :inc
    :onyx/fn :onyx.messaging.base-test/my-inc
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size}

   {:onyx/name :out
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}])

(def workflow [[:in :inc] [:inc :out]])

(def in-chan (chan 100))

(def out-chan (chan 100))

(defmethod l-ext/inject-lifecycle-resources :in
  [_ _] {:core.async/in-chan in-chan})

(defmethod l-ext/inject-lifecycle-resources :out
  [_ _] {:core.async/out-chan out-chan})

(def v-peers (onyx.api/start-peers! 3 peer-config))

(onyx.api/submit-job
 peer-config
 {:catalog catalog :workflow workflow
  :task-scheduler :onyx.task-scheduler/round-robin})

(doseq [n (range 25)]
  (>!! in-chan {:n n}))

(>!! in-chan :done)

(comment
  (doseq [v-peer v-peers]
    (onyx.api/shutdown-peer v-peer))

  (onyx.api/shutdown-env env)

  )

