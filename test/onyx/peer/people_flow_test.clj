(ns onyx.peer.people-flow-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config with-test-env]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.api]))

(def people-in-chan (atom nil))
(def people-in-buffer (atom nil))

(def children-out-chan (atom nil))

(def adults-out-chan (atom nil))

(def athletes-wa-out-chan (atom nil))

(def everyone-out-chan (atom nil))

(defn inject-people-in-ch [event lifecycle]
  {:core.async/buffer people-in-buffer
   :core.async/chan @people-in-chan})

(defn inject-children-out-ch [event lifecycle]
  {:core.async/chan @children-out-chan})

(defn inject-adults-out-ch [event lifecycle]
  {:core.async/chan @adults-out-chan})

(defn inject-athletes-wa-out-ch [event lifecycle]
  {:core.async/chan @athletes-wa-out-chan})

(defn inject-everyone-out-ch [event lifecycle]
  {:core.async/chan @everyone-out-chan})

(def people-in-calls
  {:lifecycle/before-task-start inject-people-in-ch})

(def children-out-calls
  {:lifecycle/before-task-start inject-children-out-ch})

(def adults-out-calls
  {:lifecycle/before-task-start inject-adults-out-ch})

(def athletes-wa-out-calls
  {:lifecycle/before-task-start inject-athletes-wa-out-ch})

(def everyone-out-calls
  {:lifecycle/before-task-start inject-everyone-out-ch})

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

(deftest people-flow-test
  (let [id (random-uuid)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id id)
        peer-config (assoc (:peer-config config) :onyx/tenancy-id id)
        batch-size 10
        catalog [{:onyx/name :people-in
                  :onyx/plugin :onyx.plugin.core-async/input
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
                  :onyx/plugin :onyx.plugin.core-async/output
                  :onyx/type :output
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1
                  :onyx/doc "Writes segments to a core.async channel"}

                 {:onyx/name :adults-out
                  :onyx/plugin :onyx.plugin.core-async/output
                  :onyx/type :output
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1
                  :onyx/doc "Writes segments to a core.async channel"}

                 {:onyx/name :athletes-wa-out
                  :onyx/plugin :onyx.plugin.core-async/output
                  :onyx/type :output
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1
                  :onyx/doc "Writes segments to a core.async channel"}

                 {:onyx/name :everyone-out
                  :onyx/plugin :onyx.plugin.core-async/output
                  :onyx/type :output
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1
                  :onyx/doc "Writes segments to a core.async channel"}]

        workflow [[:people-in :process-children]
                  [:people-in :process-adults]
                  [:people-in :process-athletes-in-washington]
                  [:people-in :process-everyone]

                  [:process-children :children-out]
                  [:process-adults :adults-out]
                  [:process-athletes-in-washington :athletes-wa-out]
                  [:process-everyone :everyone-out]]

        flow-conditions [{:flow/from :people-in
                          :flow/to [:process-children]
                          :child/age 17
                          :flow/predicate [:onyx.peer.people-flow-test/child? :child/age]}

                         {:flow/from :people-in
                          :flow/to [:process-adults]
                          :flow/predicate :onyx.peer.people-flow-test/adult?}

                         {:flow/from :people-in
                          :flow/to [:process-athletes-in-washington]
                          :flow/predicate [:and 
                                           :onyx.peer.people-flow-test/athlete? 
                                           :onyx.peer.people-flow-test/washington-resident?]}

                         {:flow/from :people-in
                          :flow/to [:process-everyone]
                          :flow/predicate :onyx.peer.people-flow-test/constantly-true}]

        lifecycles [{:lifecycle/task :people-in
                     :lifecycle/calls :onyx.peer.people-flow-test/people-in-calls}
                    {:lifecycle/task :children-out
                     :lifecycle/calls :onyx.peer.people-flow-test/children-out-calls}
                    {:lifecycle/task :adults-out
                     :lifecycle/calls :onyx.peer.people-flow-test/adults-out-calls}
                    {:lifecycle/task :athletes-wa-out
                     :lifecycle/calls :onyx.peer.people-flow-test/athletes-wa-out-calls}
                    {:lifecycle/task :everyone-out
                     :lifecycle/calls :onyx.peer.people-flow-test/everyone-out-calls}]

        children-expectatations #{{:age 17 :job "programmer" :location "Washington"}
                                  {:age 13 :job "student" :location "Maine"}}
        adults-expectatations #{{:age 24 :job "athlete" :location "Washington"}
                                {:age 18 :job "mechanic" :location "Vermont"}
                                {:age 42 :job "doctor" :location "Florida"}
                                {:age 64 :job "athlete" :location "Pennsylvania"}
                                {:age 35 :job "bus driver" :location "Texas"}
                                {:age 50 :job "lawyer" :location "California"}
                                {:age 25 :job "psychologist" :location "Washington"}}
        athletes-wa-expectatations #{{:age 24 :job "athlete" :location "Washington"}}
        everyone-expectatations #{{:age 24 :job "athlete" :location "Washington"}
                                  {:age 17 :job "programmer" :location "Washington"}
                                  {:age 18 :job "mechanic" :location "Vermont"}
                                  {:age 13 :job "student" :location "Maine"}
                                  {:age 42 :job "doctor" :location "Florida"}
                                  {:age 64 :job "athlete" :location "Pennsylvania"}
                                  {:age 35 :job "bus driver" :location "Texas"}
                                  {:age 50 :job "lawyer" :location "California"}
                                  {:age 25 :job "psychologist" :location "Washington"}}]

    (reset! people-in-chan (chan 100))
    (reset! people-in-buffer {})
    (reset! children-out-chan (chan (sliding-buffer 100)))
    (reset! adults-out-chan (chan (sliding-buffer 100)))
    (reset! athletes-wa-out-chan (chan (sliding-buffer 100)))
    (reset! everyone-out-chan (chan (sliding-buffer 100)))

    (with-test-env [test-env [9 env-config peer-config]]
        (doseq [x [{:age 24 :job "athlete" :location "Washington"}
                     {:age 17 :job "programmer" :location "Washington"}
                     {:age 18 :job "mechanic" :location "Vermont"}
                     {:age 13 :job "student" :location "Maine"}
                     {:age 42 :job "doctor" :location "Florida"}
                     {:age 64 :job "athlete" :location "Pennsylvania"}
                     {:age 35 :job "bus driver" :location "Texas"}
                     {:age 50 :job "lawyer" :location "California"}
                     {:age 25 :job "psychologist" :location "Washington"}]]
            (>!! @people-in-chan x))
        (close! @people-in-chan)
        (let [{:keys [job-id]} (onyx.api/submit-job peer-config
                                                    {:catalog catalog 
                                                     :workflow workflow
                                                     :flow-conditions flow-conditions
                                                     :lifecycles lifecycles
                                                     :task-scheduler :onyx.task-scheduler/balanced})
              _ (onyx.test-helper/feedback-exception! peer-config job-id)
              children (take-segments! @children-out-chan 50)
              adults (take-segments! @adults-out-chan 50)
              athletes-wa (take-segments! @athletes-wa-out-chan 50)
              everyone (take-segments! @everyone-out-chan 50)] 

          (is (= children-expectatations (into #{} children)))
          (is (= adults-expectatations (into #{} adults)))
          (is (= athletes-wa-expectatations (into #{} athletes-wa)))
          (is (= everyone-expectatations (into #{} everyone)))))))
