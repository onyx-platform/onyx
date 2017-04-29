(ns onyx.peer.flow-pred-exception-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.static.uuid :refer [random-uuid]]
            [taoensso.timbre :refer [info warn trace fatal] :as timbre]
            [onyx.test-helper :refer [load-config with-test-env]]
            [onyx.api]))

(def in-chan (atom nil))
(def in-buffer-1 (atom nil))

(def out-chan (atom nil))
(def err-out-chan (atom nil))

(defn inject-in-ch [event lifecycle]
  {:core.async/buffer in-buffer-1
   :core.async/chan @in-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan @out-chan})

(defn inject-err-out-ch [event lifecycle]
  {:core.async/chan @err-out-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(def err-out-calls
  {:lifecycle/before-task-start inject-err-out-ch})

(defn my-inc [segment]
  (update segment :n inc))

(defn n-even? [event old new all-new]  
  (if (even? (:n old))
    true
    (throw (ex-info "Value wasn't even." {}))))

(defn transform-exception [event segment e]
  {:error? true :value segment})

(deftest flow-exceptions
  (let [id (random-uuid)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id id)
        peer-config (assoc (:peer-config config) :onyx/tenancy-id id)
        batch-size 10

        catalog [{:onyx/name :in
                  :onyx/plugin :onyx.plugin.core-async/input
                  :onyx/type :input
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1
                  :onyx/doc "Reads segments from a core.async channel"}

                 {:onyx/name :inc
                  :onyx/fn ::my-inc
                  :onyx/type :function
                  :onyx/batch-size batch-size}

                 {:onyx/name :out
                  :onyx/plugin :onyx.plugin.core-async/output
                  :onyx/type :output
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1
                  :onyx/doc "Writes segments to a core.async channel"}

                 {:onyx/name :err-out
                  :onyx/plugin :onyx.plugin.core-async/output
                  :onyx/type :output
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1
                  :onyx/doc "Writes segments to a core.async channel"}]

        workflow [[:in :inc]
                  [:inc :out]
                  [:inc :err-out]]

        lifecycles [{:lifecycle/task :in
                     :lifecycle/calls ::in-calls}
                    {:lifecycle/task :out
                     :lifecycle/calls ::out-calls}
                    {:lifecycle/task :err-out
                     :lifecycle/calls ::err-out-calls}]

        flow-conditions [{:flow/from :inc
                          :flow/to [:out]
                          :flow/short-circuit? true
                          :flow/predicate [::n-even?]
                          :flow/predicate-errors-to [:err-out]
                          :flow/post-transform ::transform-exception}]]

    (reset! in-chan (chan 100))
    (reset! in-buffer-1 {})
    (reset! out-chan (chan (sliding-buffer 100)))
    (reset! err-out-chan (chan (sliding-buffer 100)))

    (with-test-env [test-env [4 env-config peer-config]]
      (let [ch @in-chan]
        (>!! ch {:n 1})
        (>!! ch {:n 2})
        (>!! ch {:n 3})
        (>!! ch {:n 4})
        (>!! ch {:n 5})
        (>!! ch {:n 6}))
      
      (close! @in-chan)
      (->> {:catalog catalog 
            :workflow workflow
            :flow-conditions flow-conditions 
            :lifecycles lifecycles
            :task-scheduler :onyx.task-scheduler/balanced}
           (onyx.api/submit-job peer-config)
           (:job-id)
           (onyx.test-helper/feedback-exception! peer-config))
      (let [results (take-segments! @out-chan 50)]
        (is (= #{{:n 3} {:n 5} {:n 7}}
               (into #{} results))))

      (let [results (take-segments! @err-out-chan 50)]
        (is (= #{{:error? true :value {:old {:n 1} :new {:n 2}}}
                 {:error? true :value {:old {:n 3} :new {:n 4}}}
                 {:error? true :value {:old {:n 5} :new {:n 6}}}}
               (into #{} results)))))))
