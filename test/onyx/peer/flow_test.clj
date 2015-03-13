(ns onyx.peer.flow-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [midje.sweet :refer :all]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def env-config (assoc (:env-config config) :onyx/id id))

(def peer-config (assoc (:peer-config config) :onyx/id id))

(def env (onyx.api/start-env env-config))

(def batch-size 10)

(def people-in-chan (chan 100))

(def colors-in-chan (chan 100))

(def children-out-chan (chan (sliding-buffer 100)))

(def adults-out-chan (chan (sliding-buffer 100)))

(def athletes-wa-out-chan (chan (sliding-buffer 100)))

(def everyone-out-chan (chan (sliding-buffer 100)))

(def red-out-chan (chan (sliding-buffer 100)))

(def blue-out-chan (chan (sliding-buffer 100)))

(def green-out-chan (chan (sliding-buffer 100)))

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

(doseq [x [{:color "red" :extra-key "Some extra context for the predicates"}
           {:color "blue" :extra-key "Some extra context for the predicates"}
           {:color "white" :extra-key "Some extra context for the predicates"}
           {:color "green" :extra-key "Some extra context for the predicates"}
           {:color "orange" :extra-key "Some extra context for the predicates"}
           {:color "black" :extra-key "Some extra context for the predicates"}
           {:color "purple" :extra-key "Some extra context for the predicates"}
           {:color "cyan" :extra-key "Some extra context for the predicates"}
           {:color "yellow" :extra-key "Some extra context for the predicates"}]]
  (>!! colors-in-chan x))

(>!! colors-in-chan :done)

(close! people-in-chan)
(close! colors-in-chan)

(def catalog
  [{:onyx/name :people-in
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :colors-in
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

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
    :onyx/doc "Writes segments to a core.async channel"}

   {:onyx/name :red-out
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}

   {:onyx/name :blue-out
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}

   {:onyx/name :green-out
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}])

(defmethod l-ext/inject-lifecycle-resources :people-in
  [_ _] {:core.async/chan people-in-chan})

(defmethod l-ext/inject-lifecycle-resources :colors-in
  [_ _] {:core.async/chan colors-in-chan})

(defmethod l-ext/inject-lifecycle-resources :children-out
  [_ _] {:core.async/chan children-out-chan})

(defmethod l-ext/inject-lifecycle-resources :adults-out
  [_ _] {:core.async/chan adults-out-chan})

(defmethod l-ext/inject-lifecycle-resources :athletes-wa-out
  [_ _] {:core.async/chan athletes-wa-out-chan})

(defmethod l-ext/inject-lifecycle-resources :everyone-out
  [_ _] {:core.async/chan everyone-out-chan})

(defmethod l-ext/inject-lifecycle-resources :red-out
  [_ _] {:core.async/chan red-out-chan})

(defmethod l-ext/inject-lifecycle-resources :blue-out
  [_ _] {:core.async/chan blue-out-chan})

(defmethod l-ext/inject-lifecycle-resources :green-out
  [_ _] {:core.async/chan green-out-chan})

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

(defn black? [event {:keys [color]}]
  (= color "black"))

(defn white? [event {:keys [color]}]
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

(def process-red identity)

(def process-blue identity)

(def process-green identity)

(def v-peers (onyx.api/start-peers 16 peer-config))

(onyx.api/submit-job
 peer-config
 {:catalog catalog :workflow workflow
  :flow-conditions flow-conditions
  :task-scheduler :onyx.task-scheduler/round-robin})

(def children (take-segments! children-out-chan))

(def adults (take-segments! adults-out-chan))

(def athletes-wa (take-segments! athletes-wa-out-chan))

(def everyone (take-segments! everyone-out-chan))

(def red (take-segments! red-out-chan))

(def blue (take-segments! blue-out-chan))

(def green (take-segments! green-out-chan))

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

(def red-expectatations
  #{{:color "white"}
    {:color "red"}
    {:color "orange"}
    :done})

(def blue-expectatations
  #{{:color "white"}
    {:color "blue"}
    {:color "orange"}
    :done})

(def green-expectatations
  #{{:color "white"}
    {:color "green"}
    {:color "orange"}
    :done})

(fact (into #{} children) => children-expectatations)
(fact (into #{} adults) => adults-expectatations)
(fact (into #{} athletes-wa) => athletes-wa-expectatations)
(fact (into #{} everyone) => everyone-expectatations)

(fact (into #{} green) => green-expectatations)
(fact (into #{} red) => red-expectatations)
(fact (into #{} blue) => blue-expectatations)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)

