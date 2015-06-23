(ns onyx.peer.colors-flow-test
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

(def colors-in-chan (chan 100))

(def red-out-chan (chan (sliding-buffer 100)))

(def blue-out-chan (chan (sliding-buffer 100)))

(def green-out-chan (chan (sliding-buffer 100)))

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

(def catalog
  [{:onyx/name :colors-in
    :onyx/ident :onyx.plugin.core-async/input
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :process-red
    :onyx/fn :onyx.peer.colors-flow-test/process-red
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size}

   {:onyx/name :process-blue
    :onyx/fn :onyx.peer.colors-flow-test/process-blue
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size}

   {:onyx/name :process-green
    :onyx/fn :onyx.peer.colors-flow-test/process-green
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size}

   {:onyx/name :red-out
    :onyx/ident :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}

   {:onyx/name :blue-out
    :onyx/ident :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}

   {:onyx/name :green-out
    :onyx/ident :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}])

(def workflow
  [[:colors-in :process-red]
   [:colors-in :process-blue]
   [:colors-in :process-green]

   [:process-red :red-out]
   [:process-blue :blue-out]
   [:process-green :green-out]])

(def flow-conditions
  [{:flow/from :colors-in
    :flow/to :all
    :flow/short-circuit? true
    :flow/exclude-keys [:extra-key]
    :flow/predicate :onyx.peer.colors-flow-test/white?}

   {:flow/from :colors-in
    :flow/to :none
    :flow/short-circuit? true
    :flow/exclude-keys [:extra-key]
    :flow/action :retry
    :flow/predicate :onyx.peer.colors-flow-test/black?}

   {:flow/from :colors-in
    :flow/to [:process-red]
    :flow/short-circuit? true
    :flow/exclude-keys [:extra-key]
    :flow/predicate :onyx.peer.colors-flow-test/red?}

   {:flow/from :colors-in
    :flow/to [:process-blue]
    :flow/short-circuit? true
    :flow/exclude-keys [:extra-key]
    :flow/predicate :onyx.peer.colors-flow-test/blue?}

   {:flow/from :colors-in
    :flow/to [:process-green]
    :flow/short-circuit? true
    :flow/exclude-keys [:extra-key]
    :flow/predicate :onyx.peer.colors-flow-test/green?}

   {:flow/from :colors-in
    :flow/to [:process-red]
    :flow/exclude-keys [:extra-key]
    :flow/predicate :onyx.peer.colors-flow-test/orange?}

   {:flow/from :colors-in
    :flow/to [:process-blue]
    :flow/exclude-keys [:extra-key]
    :flow/predicate :onyx.peer.colors-flow-test/orange?}

   {:flow/from :colors-in
    :flow/to [:process-green]
    :flow/exclude-keys [:extra-key]
    :flow/predicate :onyx.peer.colors-flow-test/orange?}])

(defn inject-colors-in-ch [event lifecycle]
  {:core.async/chan colors-in-chan})

(defn inject-red-out-ch [event lifecycle]
  {:core.async/chan red-out-chan})

(defn inject-blue-out-ch [event lifecycle]
  {:core.async/chan blue-out-chan})

(defn inject-green-out-ch [event lifecycle]
  {:core.async/chan green-out-chan})

(def colors-in-calls
  {:lifecycle/before-task-start inject-colors-in-ch})

(def red-out-calls
  {:lifecycle/before-task-start inject-red-out-ch})

(def blue-out-calls
  {:lifecycle/before-task-start inject-blue-out-ch})

(def green-out-calls
  {:lifecycle/before-task-start inject-green-out-ch})

(def lifecycles
  [{:lifecycle/task :colors-in
    :lifecycle/calls :onyx.peer.colors-flow-test/colors-in-calls}
   {:lifecycle/task :colors-in
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :red-out
    :lifecycle/calls :onyx.peer.colors-flow-test/red-out-calls}
   {:lifecycle/task :red-out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}
   {:lifecycle/task :blue-out
    :lifecycle/calls :onyx.peer.colors-flow-test/blue-out-calls}
   {:lifecycle/task :blue-out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}
   {:lifecycle/task :green-out
    :lifecycle/calls :onyx.peer.colors-flow-test/green-out-calls}
   {:lifecycle/task :green-out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(def seen-before? (atom false))

(defn black? [event old {:keys [color]} all-new]
  (if (and (not @seen-before?) (= color "black"))
    (do
      (swap! seen-before? (constantly true))
      true)
    false))

(defn white? [event old {:keys [color]} all-new]
  (= color "white"))

(defn red? [event old {:keys [color]} all-new]
  (= color "red"))

(defn blue? [event old {:keys [color]} all-new]
  (= color "blue"))

(defn green? [event old {:keys [color]} all-new]
  (= color "green"))

(defn orange? [event old {:keys [color]} all-new]
  (= color "orange"))

(def constantly-true (constantly true))

(def process-red identity)

(def process-blue identity)

(def process-green identity)

(def v-peers (onyx.api/start-peers 7 peer-group))

(onyx.api/submit-job
 peer-config
 {:catalog catalog :workflow workflow
  :flow-conditions flow-conditions :lifecycles lifecycles
  :task-scheduler :onyx.task-scheduler/balanced})

(def red (take-segments! red-out-chan))

(def blue (take-segments! blue-out-chan))

(def green (take-segments! green-out-chan))

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

(fact (into #{} green) => green-expectatations)
(fact (into #{} red) => red-expectatations)
(fact (into #{} blue) => blue-expectatations)

(close! colors-in-chan)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)
