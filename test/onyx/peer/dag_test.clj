(ns onyx.peer.dag-test
  (:require [com.stuartsierra.component :as component]
            [onyx.system :refer [onyx-development-env]]
            [midje.sweet :refer :all]
            [onyx.queue.hornetq-utils :as hq-util]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def env-config
  {:hornetq/mode :udp
   :hornetq/server? true
   :hornetq.server/type :embedded
   :hornetq.udp/cluster-name (:cluster-name (:hornetq config))
   :hornetq.udp/group-address (:group-address (:hornetq config))
   :hornetq.udp/group-port (:group-port (:hornetq config))
   :hornetq.udp/refresh-timeout (:refresh-timeout (:hornetq config))
   :hornetq.udp/discovery-timeout (:discovery-timeout (:hornetq config))
   :hornetq.embedded/config (:configs (:hornetq config))
   :zookeeper/address (:address (:zookeeper config))
   :zookeeper/server? true
   :zookeeper.server/port (:spawn-port (:zookeeper config))
   :onyx/id id
   :onyx.coordinator/revoke-delay 5000})

(def peer-config
  {:hornetq/mode :udp
   :hornetq.udp/cluster-name (:cluster-name (:hornetq config))
   :hornetq.udp/group-address (:group-address (:hornetq config))
   :hornetq.udp/group-port (:group-port (:hornetq config))
   :hornetq.udp/refresh-timeout (:refresh-timeout (:hornetq config))
   :hornetq.udp/discovery-timeout (:discovery-timeout (:hornetq config))
   :zookeeper/address (:address (:zookeeper config))
   :onyx/id id
   :onyx.peer/inbox-capacity (:inbox-capacity (:peer config))
   :onyx.peer/outbox-capacity (:outbox-capacity (:peer config))
   :onyx.peer/job-scheduler :onyx.job-scheduler/round-robin})

(def env (onyx.api/start-env env-config))

(def n-queued-messages 15000)

(def batch-size 1320)

(def echo 1000)

(def hq-config {"host" (:host (:non-clustered (:hornetq config)))
                "port" (:port (:non-clustered (:hornetq config)))})

(def a-queue (str (java.util.UUID/randomUUID)))

(def b-queue (str (java.util.UUID/randomUUID)))

(def c-queue (str (java.util.UUID/randomUUID)))

(def j-queue (str (java.util.UUID/randomUUID)))

(def k-queue (str (java.util.UUID/randomUUID)))

(def l-queue (str (java.util.UUID/randomUUID)))

(def a-segments
  (map (fn [n] {:n n})
       (range n-queued-messages)))

(def b-segments
  (map (fn [n] {:n n})
       (range n-queued-messages (* 2 n-queued-messages))))

(def c-segments
  (map (fn [n] {:n n})
       (range (* 2 n-queued-messages) (* 3 n-queued-messages))))

(hq-util/create-queue! hq-config a-queue)

(hq-util/create-queue! hq-config b-queue)

(hq-util/create-queue! hq-config c-queue)

(hq-util/create-queue! hq-config j-queue)

(hq-util/create-queue! hq-config k-queue)

(hq-util/create-queue! hq-config l-queue)

(hq-util/write-and-cap! hq-config a-queue a-segments echo)

(hq-util/write-and-cap! hq-config b-queue b-segments echo)

(hq-util/write-and-cap! hq-config c-queue c-segments echo)

(def d identity)

(def e identity)

(def f identity)

(def g identity)

(def h identity)

(def i identity)

(def catalog
  [{:onyx/name :A
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name a-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}

   {:onyx/name :B
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name b-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}

   {:onyx/name :C
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name c-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}

   {:onyx/name :D
    :onyx/fn :onyx.peer.dag-test/d
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size}

   {:onyx/name :E
    :onyx/fn :onyx.peer.dag-test/e
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size}

   {:onyx/name :F
    :onyx/fn :onyx.peer.dag-test/f
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size}

   {:onyx/name :G
    :onyx/fn :onyx.peer.dag-test/g
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size}

   {:onyx/name :H
    :onyx/fn :onyx.peer.dag-test/h
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size}

   {:onyx/name :I
    :onyx/fn :onyx.peer.dag-test/i
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size}

   {:onyx/name :J
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name j-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}

   {:onyx/name :K
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name k-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}

   {:onyx/name :L
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name l-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}])

;;; A    B       C
;;;  \  /        |
;;;   D- >       E
;;;   |  \     / | \
;;;   F   \-> G  H  I
;;;  / \       \ | /
;;; J   K        L

(def workflow
  [[:A :D]
   [:B :D]
   [:D :F]
   [:F :J]
   [:F :K]
   [:C :E]
   [:E :G]
   [:E :H]
   [:E :I]
   [:G :L]
   [:H :L]
   [:I :L]
   [:D :G]])

(def v-peers (onyx.api/start-peers! 6 peer-config))

(onyx.api/submit-job peer-config
                     {:catalog catalog :workflow workflow
                      :task-scheduler :onyx.task-scheduler/round-robin})

(def j-results (hq-util/consume-queue! hq-config j-queue echo))

(def k-results (hq-util/consume-queue! hq-config k-queue echo))

(def l-results (hq-util/consume-queue! hq-config l-queue echo))

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)

(fact (last j-results) => :done)

(fact (last k-results) => :done)

(fact (last l-results) => :done)

(fact (into #{} (concat a-segments b-segments))
      => (into #{} (butlast j-results)))

(fact (into #{} (concat a-segments b-segments))
      => (into #{} (butlast k-results)))

(fact (into #{} (concat a-segments b-segments c-segments))
      => (into #{} (butlast l-results)))

