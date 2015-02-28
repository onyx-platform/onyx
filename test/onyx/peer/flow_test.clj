(ns onyx.peer.flow-test
  (:require [midje.sweet :refer :all]
            [onyx.system :refer [onyx-development-env]]
            [onyx.queue.hornetq-utils :as hq-util]
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
   :onyx/id id})

(def peer-config
  {:hornetq/mode :udp
   :hornetq.udp/cluster-name (:cluster-name (:hornetq config))
   :hornetq.udp/group-address (:group-address (:hornetq config))
   :hornetq.udp/group-port (:group-port (:hornetq config))
   :hornetq.udp/refresh-timeout (:refresh-timeout (:hornetq config))
   :hornetq.udp/discovery-timeout (:discovery-timeout (:hornetq config))
   :zookeeper/address (:address (:zookeeper config))
   :onyx/id id
   :onyx.peer/job-scheduler :onyx.job-scheduler/greedy})

(def env (onyx.api/start-env env-config))

(def batch-size 10)

(def echo 1000)

(def people-in-queue (str (java.util.UUID/randomUUID)))

(def colors-in-queue (str (java.util.UUID/randomUUID)))

(def children-out-queue (str (java.util.UUID/randomUUID)))

(def adults-out-queue (str (java.util.UUID/randomUUID)))

(def athletes-wa-out-queue (str (java.util.UUID/randomUUID)))

(def everyone-out-queue (str (java.util.UUID/randomUUID)))

(def red-out-queue (str (java.util.UUID/randomUUID)))

(def blue-out-queue (str (java.util.UUID/randomUUID)))

(def green-out-queue (str (java.util.UUID/randomUUID)))

(def hq-config {"host" (:host (:non-clustered (:hornetq config)))
                "port" (:port (:non-clustered (:hornetq config)))})

(hq-util/create-queue! hq-config people-in-queue)
(hq-util/create-queue! hq-config colors-in-queue)
(hq-util/create-queue! hq-config children-out-queue)
(hq-util/create-queue! hq-config adults-out-queue)
(hq-util/create-queue! hq-config athletes-wa-out-queue)
(hq-util/create-queue! hq-config everyone-out-queue)
(hq-util/create-queue! hq-config red-out-queue)
(hq-util/create-queue! hq-config blue-out-queue)
(hq-util/create-queue! hq-config green-out-queue)

(hq-util/write-and-cap!
 hq-config people-in-queue
 [{:age 24 :job "athlete" :location "Washington"}
  {:age 17 :job "programmer" :location "Washington"}
  {:age 18 :job "mechanic" :location "Vermont"}
  {:age 13 :job "student" :location "Maine"}
  {:age 42 :job "doctor" :location "Florida"}
  {:age 64 :job "athlete" :location "Pennsylvania"}
  {:age 35 :job "bus driver" :location "Texas"}
  {:age 50 :job "lawyer" :location "California"}
  {:age 25 :job "psychologist" :location "Washington"}]
 echo)

(hq-util/write-and-cap!
 hq-config colors-in-queue
 [{:color "red"}
  {:color "blue"}
  {:color "white"}
  {:color "green"}
  {:color "orange"}
  {:color "black"}
  {:color "purple"}
  {:color "cyan"}
  {:color "yellow"}]
 echo)

(def catalog
  [{:onyx/name :people-in
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name people-in-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}

   {:onyx/name :colors-in
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name colors-in-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}

   {:onyx/name :process-children
    :onyx/fn :onyx.peer.flow-test/process-children
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size}

   {:onyx/name :process-adults
    :onyx/fn :onyx.peer.flow-test/process-adults
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size}

   {:onyx/name :process-athletes-in-washington
    :onyx/fn :onyx.peer.flow-test/process-athletes-in-washington
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size}

   {:onyx/name :process-everyone
    :onyx/fn :onyx.peer.flow-test/process-everyone
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size}

   {:onyx/name :process-red
    :onyx/fn :onyx.peer.flow-test/process-red
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size}

   {:onyx/name :process-blue
    :onyx/fn :onyx.peer.flow-test/process-blue
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size}

   {:onyx/name :process-green
    :onyx/fn :onyx.peer.flow-test/process-green
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size}

   {:onyx/name :children-out
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name children-out-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}

   {:onyx/name :adults-out
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name adults-out-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}

   {:onyx/name :athletes-wa-out
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name athletes-wa-out-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}

   {:onyx/name :everyone-out
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name everyone-out-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}

   {:onyx/name :red-out
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name red-out-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}

   {:onyx/name :blue-out
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name blue-out-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}

   {:onyx/name :green-out
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name green-out-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}])

(def workflow
  [[:people-in :process-children]
   [:people-in :process-adults]
   [:people-in :process-athletes-in-washington]
   [:people-in :process-everyone]

   [:colors-in :process-red]
   [:colors-in :process-blue]
   [:colors-in :process-green]

   [:process-children :children-out]
   [:process-adults :adults-out]
   [:process-athletes-in-washington :athletes-wa-out]
   [:process-everyone :everyone-out]

   [:process-red :red-out]
   [:process-blue :blue-out]
   [:process-green :green-out]])

(def flow-conditions
  [{:flow/from :people-in
    :flow/to [:process-children]
    :child/age 17
    :flow/predicate [:onyx.peer.flow-test/child? :child/age]}

   {:flow/from :people-in
    :flow/to [:process-adults]
    :flow/predicate :onyx.peer.flow-test/adult?}

   {:flow/from :people-in
    :flow/to [:process-athletes-in-washington]
    :flow/predicate [:and :onyx.peer.flow-test/athlete? :onyx.peer.flow-test/washington-resident?]}

   {:flow/from :people-in
    :flow/to [:process-everyone]
    :flow/predicate :onyx.peer.flow-test/constantly-true}

   {:flow/from :colors-in
    :flow/to :all
    :flow/short-circuit? true
    :flow/exclude-keys [:extra-key]
    :flow/predicate :onyx.peer.flow-test/white?}

   {:flow/from :colors-in
    :flow/to :none
    :flow/short-circuit? true
    :flow/exclude-keys [:extra-key]
    :flow/predicate :onyx.peer.flow-test/black?}

   {:flow/from :colors-in
    :flow/to [:process-red]
    :flow/short-circuit? true
    :flow/exclude-keys [:extra-key]
    :flow/predicate :onyx.peer.flow-test/red?}

   {:flow/from :colors-in
    :flow/to [:process-blue]
    :flow/short-circuit? true
    :flow/exclude-keys [:extra-key]
    :flow/predicate :onyx.peer.flow-test/blue?}

   {:flow/from :colors-in
    :flow/to [:process-green]
    :flow/short-circuit? true
    :flow/exclude-keys [:extra-key]
    :flow/predicate :onyx.peer.flow-test/green?}

   {:flow/from :colors-in
    :flow/to [:process-red]
    :flow/exclude-keys [:extra-key]
    :flow/predicate :onyx.peer.flow-test/orange?}

   {:flow/from :colors-in
    :flow/to [:process-blue]
    :flow/exclude-keys [:extra-key]
    :flow/predicate :onyx.peer.flow-test/orange?}

   {:flow/from :colors-in
    :flow/to [:process-green]
    :flow/exclude-keys [:extra-key]
    :flow/predicate :onyx.peer.flow-test/orange?}])

(defn child? [event {:keys [age]} max-child-age]
  (<= age max-child-age))

(defn adult? [event {:keys [age]}]
  (>= age 18))

(defn athlete? [event {:keys [job]}]
  (= job "athlete"))

(defn washington-resident? [event {:keys [location]}]
  (= location "Washington"))

(defn black? [{:keys [color]}]
  (= color "black"))

(defn white? [{:keys [color]}]
  (= color "white"))

(defn red? [event {:keys [color]}]
  (= color "red"))

(defn blue? [event {:keys [color]}]
  (= color "blue"))

(defn green? [event {:keys [color]}]
  (= color "green"))

(defn orange? [event {:keys [color]}]
  (= color "orange"))

(def constantly-true (constantly true))

(def process-children identity)

(def process-adults identity)

(def process-athletes-in-washington identity)

(def process-everyone identity)

(defn process-red [segment]
  (assoc segment :extra-key "Some extra context for the predicates"))

(defn process-blue [segment]
  (assoc segment :extra-key "Some extra context for the predicates"))

(defn process-green [segment]
  (assoc segment :extra-key "Some extra context for the predicates"))

(def v-peers (onyx.api/start-peers! 1 peer-config))

(onyx.api/submit-job
 peer-config
 {:catalog catalog :workflow workflow
  :flow-conditions flow-conditions
  :task-scheduler :onyx.task-scheduler/round-robin})

(def results (hq-util/consume-queue! hq-config out-queue echo))

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(let [expected (set (map (fn [x] {:n (inc x)}) (filter even? (range 11 n-messages))))]
  (fact (set (butlast results)) => expected)
  (fact (last results) => :done))

(onyx.api/shutdown-env env)

