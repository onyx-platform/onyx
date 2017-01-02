(ns onyx.peer.colors-flow-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config with-test-env]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.api]))

(def colors-in-chan (atom nil))
(def colors-in-buffer (atom nil))

(def red-out-chan (atom nil))

(def blue-out-chan (atom nil))

(def green-out-chan (atom nil))

(def all-out-chan (atom nil))

(defn inject-colors-in-ch [event lifecycle]
  {:core.async/buffer colors-in-buffer
   :core.async/chan @colors-in-chan})

(defn inject-red-out-ch [event lifecycle]
  {:core.async/chan @red-out-chan})

(defn inject-blue-out-ch [event lifecycle]
  {:core.async/chan @blue-out-chan})

(defn inject-green-out-ch [event lifecycle]
  {:core.async/chan @green-out-chan})

(defn inject-all-out-ch [event lifecycle]
  {:core.async/chan @all-out-chan})

(def colors-in-calls
  {:lifecycle/before-task-start inject-colors-in-ch})

(def red-out-calls
  {:lifecycle/before-task-start inject-red-out-ch})

(def blue-out-calls
  {:lifecycle/before-task-start inject-blue-out-ch})

(def green-out-calls
  {:lifecycle/before-task-start inject-green-out-ch})

(def all-out-calls
  {:lifecycle/before-task-start inject-all-out-ch})

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

(def retry-counter
  (atom (long 0)))

(def retry-calls
  {:lifecycle/after-retry-segment
   (fn retry-count-inc [event message-id rets lifecycle]
     (swap! retry-counter inc))})

;; :broken due of use of flow retry
;; TODO, re-enable retries and tests
;; This test has been re-enabled to test flow conditions.
(deftest ^:smoke colors-flow
  (let [id (random-uuid)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id id)
        peer-config (assoc (:peer-config config) :onyx/tenancy-id id)
        batch-size 10

        catalog [{:onyx/name :colors-in
                  :onyx/plugin :onyx.plugin.core-async/input
                  :onyx/type :input
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1
                  :onyx/doc "Reads segments from a core.async channel"}

                 {:onyx/name :process-red
                  :onyx/fn :onyx.peer.colors-flow-test/process-red
                  :onyx/type :function
                  :onyx/batch-size batch-size}

                 {:onyx/name :process-blue
                  :onyx/fn :onyx.peer.colors-flow-test/process-blue
                  :onyx/type :function
                  :onyx/batch-size batch-size}

                 {:onyx/name :process-green
                  :onyx/fn :onyx.peer.colors-flow-test/process-green
                  :onyx/type :function
                  :onyx/batch-size batch-size}

                 {:onyx/name :red-out
                  :onyx/plugin :onyx.plugin.core-async/output
                  :onyx/type :output
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1
                  :onyx/doc "Writes segments to a core.async channel"}

                 {:onyx/name :blue-out
                  :onyx/plugin :onyx.plugin.core-async/output
                  :onyx/type :output
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1
                  :onyx/doc "Writes segments to a core.async channel"}

                 {:onyx/name :green-out
                  :onyx/plugin :onyx.plugin.core-async/output
                  :onyx/type :output
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1
                  :onyx/doc "Writes segments to a core.async channel"}

                 {:onyx/name :all-out
                  :onyx/plugin :onyx.plugin.core-async/output
                  :onyx/type :output
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1
                  :onyx/doc "Writes segments to a core.async channel"}]

        workflow [[:colors-in :process-red]
                  [:colors-in :process-blue]
                  [:colors-in :process-green]

                  [:process-red :red-out]
                  [:process-blue :blue-out]
                  [:process-green :green-out]

                  [:process-red :all-out]
                  [:process-blue :all-out]
                  [:process-green :all-out]]

        flow-conditions [{:flow/from :colors-in
                          :flow/to :all
                          :flow/short-circuit? true
                          :flow/exclude-keys [:extra-key]
                          :flow/predicate :onyx.peer.colors-flow-test/white?}

                         #_{:flow/from :colors-in
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
                          :flow/predicate :onyx.peer.colors-flow-test/orange?}

                         {:flow/from :all
                          :flow/short-circuit? true
                          :flow/to [:red-out :green-out :blue-out :all-out]
                          :flow/predicate :onyx.peer.colors-flow-test/constantly-true}]

        lifecycles [{:lifecycle/task :colors-in
                     :lifecycle/calls :onyx.peer.colors-flow-test/colors-in-calls}
                    #_{:lifecycle/task :colors-in
                     :lifecycle/calls :onyx.peer.colors-flow-test/retry-calls}
                    {:lifecycle/task :red-out
                     :lifecycle/calls :onyx.peer.colors-flow-test/red-out-calls}
                    {:lifecycle/task :blue-out
                     :lifecycle/calls :onyx.peer.colors-flow-test/blue-out-calls}
                    {:lifecycle/task :green-out
                     :lifecycle/calls :onyx.peer.colors-flow-test/green-out-calls}
                    {:lifecycle/task :all-out
                     :lifecycle/calls :onyx.peer.colors-flow-test/all-out-calls}]]

    (reset! colors-in-chan (chan 100))
    (reset! colors-in-buffer {})
    (reset! red-out-chan (chan (sliding-buffer 100)))
    (reset! blue-out-chan (chan (sliding-buffer 100)))
    (reset! green-out-chan (chan (sliding-buffer 100)))
    (reset! all-out-chan (chan (sliding-buffer 100)))

    (with-test-env [test-env [8 env-config peer-config]]
      (doseq [x [{:color "red" :extra-key "Some extra context for the predicates"}
                 {:color "blue" :extra-key "Some extra context for the predicates"}
                 {:color "white" :extra-key "Some extra context for the predicates"}
                 {:color "green" :extra-key "Some extra context for the predicates"}
                 {:color "orange" :extra-key "Some extra context for the predicates"}
                 {:color "black" :extra-key "Some extra context for the predicates"}
                 {:color "purple" :extra-key "Some extra context for the predicates"}
                 {:color "cyan" :extra-key "Some extra context for the predicates"}
                 {:color "yellow" :extra-key "Some extra context for the predicates"}]]
        (>!! @colors-in-chan x))
      (close! @colors-in-chan)

      (->> (onyx.api/submit-job peer-config
                                {:catalog catalog 
                                 :workflow workflow
                                 :flow-conditions flow-conditions 
                                 :lifecycles lifecycles
                                 :task-scheduler :onyx.task-scheduler/balanced})
           (:job-id)
           (onyx.test-helper/feedback-exception! peer-config))

      (let [red (take-segments! @red-out-chan 50)
            blue (take-segments! @blue-out-chan 50)
            green (take-segments! @green-out-chan 50)
            all (take-segments! @all-out-chan 50)
            red-expectations #{{:color "white"}
                               {:color "red"}
                               {:color "orange"}}
            blue-expectations #{{:color "white"}
                                {:color "blue"}
                                {:color "orange"}}
            green-expectations #{{:color "white"}
                                 {:color "green"}
                                 {:color "orange"}}
            all-expectations #{{:color "blue"}
                               {:color "white"}
                               {:color "orange"}
                               {:color "green"}
                               {:color "red"}}]

        (is (= green-expectations (into #{} green)))
        (is (= red-expectations (into #{} red)))
        (is (= blue-expectations (into #{} blue)))
        (is (= all-expectations (into #{} all)))
        #_(is (= 1 @retry-counter)))

      (close! @colors-in-chan))))
