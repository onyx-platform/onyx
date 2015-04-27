(ns onyx.peer.people-flow-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [midje.sweet :refer :all]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config]]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def config (load-config))

(def env-config (assoc (:env-config config) :onyx/id id))

(def peer-config (assoc (:peer-config config) :onyx/id id))

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def batch-size 10)

(def people-in-chan (chan 100))

(def children-out-chan (chan (sliding-buffer 100)))

(def adults-out-chan (chan (sliding-buffer 100)))

(def athletes-wa-out-chan (chan (sliding-buffer 100)))

(def everyone-out-chan (chan (sliding-buffer 100)))

(doseq [x [{:age 24 :job "athlete" :location "Washington"}
           {:age 17 :job "programmer" :location "Washington"}
           {:age 18 :job "mechanic" :location "Vermont"}
           {:age 13 :job "student" :location "Maine"}
           {:age 42 :job "doctor" :location "Florida"}
           {:age 64 :job "athlete" :location "Pennsylvania"}
           {:age 35 :job "bus driver" :location "Texas"}
           {:age 50 :job "lawyer" :location "California"}
           {:age 25 :job "psychologist" :location "Washington"}]]
  (>!! people-in-chan x))

(>!! people-in-chan :done)

(def catalog
  [{:onyx/name :people-in
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :process-children
    :onyx/fn :onyx.peer.people-flow-test/process-children
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :process-adults
    :onyx/fn :onyx.peer.people-flow-test/process-adults
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :process-athletes-in-washington
    :onyx/fn :onyx.peer.people-flow-test/process-athletes-in-washington
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :process-everyone
    :onyx/fn :onyx.peer.people-flow-test/process-everyone
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :children-out
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}

   {:onyx/name :adults-out
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}

   {:onyx/name :athletes-wa-out
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}

   {:onyx/name :everyone-out
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}])

(def workflow
  [[:people-in :process-children]
   [:people-in :process-adults]
   [:people-in :process-athletes-in-washington]
   [:people-in :process-everyone]

   [:process-children :children-out]
   [:process-adults :adults-out]
   [:process-athletes-in-washington :athletes-wa-out]
   [:process-everyone :everyone-out]])

(def flow-conditions
  [{:flow/from :people-in
    :flow/to [:process-children]
    :child/age 17
    :flow/predicate [:onyx.peer.people-flow-test/child? :child/age]}

   {:flow/from :people-in
    :flow/to [:process-adults]
    :flow/predicate :onyx.peer.people-flow-test/adult?}

   {:flow/from :people-in
    :flow/to [:process-athletes-in-washington]
    :flow/predicate [:and :onyx.peer.people-flow-test/athlete? :onyx.peer.people-flow-test/washington-resident?]}

   {:flow/from :people-in
    :flow/to [:process-everyone]
    :flow/predicate :onyx.peer.people-flow-test/constantly-true}])

(defn child? [event old {:keys [age]} all-new max-child-age]
  (<= age max-child-age))

(defn adult? [event old {:keys [age]} all-new]
  (>= age 18))

(defn athlete? [event old {:keys [job]} all-new]
  (= job "athlete"))

(defn washington-resident? [event old {:keys [location]} all-new]
  (= location "Washington"))

(def constantly-true (constantly true))

(def process-children identity)

(def process-adults identity)

(def process-athletes-in-washington identity)

(def process-everyone identity)

(def process-red identity)

(def process-blue identity)

(def process-green identity)

(def v-peers (onyx.api/start-peers 9 peer-group))

(onyx.api/submit-job
 peer-config
 {:catalog catalog :workflow workflow
  :flow-conditions flow-conditions
  :lifecycles lifecycles
  :task-scheduler :onyx.task-scheduler/balanced})

(def children (take-segments! children-out-chan))

(def adults (take-segments! adults-out-chan))

(def athletes-wa (take-segments! athletes-wa-out-chan))

(def everyone (take-segments! everyone-out-chan))

(def children-expectatations
  #{{:age 17 :job "programmer" :location "Washington"}
    {:age 13 :job "student" :location "Maine"}        
    :done})

(def adults-expectatations
  #{{:age 24 :job "athlete" :location "Washington"}
    {:age 18 :job "mechanic" :location "Vermont"}
    {:age 42 :job "doctor" :location "Florida"}
    {:age 64 :job "athlete" :location "Pennsylvania"}
    {:age 35 :job "bus driver" :location "Texas"}
    {:age 50 :job "lawyer" :location "California"}
    {:age 25 :job "psychologist" :location "Washington"}
    :done})

(def athletes-wa-expectatations
  #{{:age 24 :job "athlete" :location "Washington"}
    :done})

(def everyone-expectatations
  #{{:age 24 :job "athlete" :location "Washington"}
    {:age 17 :job "programmer" :location "Washington"}
    {:age 18 :job "mechanic" :location "Vermont"}
    {:age 13 :job "student" :location "Maine"}
    {:age 42 :job "doctor" :location "Florida"}
    {:age 64 :job "athlete" :location "Pennsylvania"}
    {:age 35 :job "bus driver" :location "Texas"}
    {:age 50 :job "lawyer" :location "California"}
    {:age 25 :job "psychologist" :location "Washington"}
    :done})

(fact (into #{} children) => children-expectatations)
(fact (into #{} adults) => adults-expectatations)
(fact (into #{} athletes-wa) => athletes-wa-expectatations)
(fact (into #{} everyone) => everyone-expectatations)

(close! people-in-chan)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)

