(ns onyx.peer.flow-exception-test
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

(defn inject-in-ch [event lifecycle]
  {:core.async/buffer in-buffer-1
   :core.async/chan @in-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan @out-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(defn even-exception? [event old e all-new]
  (= (:error (ex-data e)) :even))

(defn five-exception? [event old e all-new]
  (= (:error (ex-data e)) :five))

(defn transform-even [event segment e]
  {:error? true :value (:n segment)})

(defn transform-five [event segment e]
  {:error? true :value "abc"})

(defn my-inc [{:keys [n] :as segment}]
  (cond (even? n)
        (throw (ex-info "Number was even" {:error :even :n n}))
        (= 5 n)
        (throw (ex-info "Number was 5" {:error :five :n n}))
        :else segment))

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
                  :onyx/fn :onyx.peer.flow-exception-test/my-inc
                  :onyx/type :function
                  :onyx/batch-size batch-size}

                 {:onyx/name :out
                  :onyx/plugin :onyx.plugin.core-async/output
                  :onyx/type :output
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1
                  :onyx/doc "Writes segments to a core.async channel"}]

        workflow [[:in :inc]
                  [:inc :out]]

        lifecycles [{:lifecycle/task :in
                     :lifecycle/calls :onyx.peer.flow-exception-test/in-calls}
                    {:lifecycle/task :out
                     :lifecycle/calls :onyx.peer.flow-exception-test/out-calls}]

        flow-conditions [{:flow/from :inc
                          :flow/to [:out]
                          :flow/short-circuit? true
                          :flow/thrown-exception? true
                          :flow/predicate [:onyx.peer.flow-exception-test/even-exception?]
                          :flow/post-transform :onyx.peer.flow-exception-test/transform-even}

                         {:flow/from :inc
                          :flow/to [:out]
                          :flow/short-circuit? true
                          :flow/thrown-exception? true
                          :flow/predicate [:onyx.peer.flow-exception-test/five-exception?]
                          :flow/post-transform :onyx.peer.flow-exception-test/transform-five}]]

    (reset! in-chan (chan 100))
    (reset! in-buffer-1 {})
    (reset! out-chan (chan (sliding-buffer 100)))

    (with-test-env [test-env [3 env-config peer-config]]
      (doseq [x (range 20)]
        (>!! @in-chan {:n x}))
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
        (is (= #{{:error? true :value 0}
                 {:n 1}
                 {:error? true :value 2}
                 {:n 3}
                 {:error? true :value 4}
                 {:error? true :value "abc"}
                 {:error? true :value 6}
                 {:n 7}
                 {:error? true :value 8}
                 {:n 9}
                 {:error? true :value 10}
                 {:n 11}
                 {:error? true :value 12}
                 {:n 13}
                 {:error? true :value 14}
                 {:n 15}
                 {:error? true :value 16}
                 {:n 17}
                 {:error? true :value 18}
                 {:n 19}}
               (into #{} results)))))))
