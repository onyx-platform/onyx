(ns onyx.peer.join-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config with-test-env]]
            [onyx.api]
            [onyx.static.uuid :refer [random-uuid]]
            [taoensso.timbre :refer [info warn trace fatal] :as timbre]))

(def people
  [{:id 1 :name "Mike" :age 23}
   {:id 2 :name "Dorrene" :age 24}
   {:id 3 :name "Benti" :age 23}
   {:id 4 :name "John" :age 19}
   {:id 5 :name "Shannon" :age 31}
   {:id 6 :name "Kristen" :age 25}])

(def names (map #(select-keys % [:id :name]) people))

(def ages (map #(select-keys % [:id :age]) people))

(def name-chan (atom nil))
(def name-buffer (atom nil))

(def age-chan (atom nil))
(def age-buffer (atom nil))

(def out-chan (atom nil))

(defn inject-names-ch [event lifecycle]
  {:core.async/buffer name-buffer
   :core.async/chan @name-chan})

(defn inject-ages-ch [event lifecycle]
  {:core.async/buffer age-buffer
   :core.async/chan @age-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan @out-chan})

(defn inject-join-state [event lifecycle]
  {:onyx.core/params [(atom {})]})

(def names-calls
  {:lifecycle/before-task-start inject-names-ch})

(def ages-calls
  {:lifecycle/before-task-start inject-ages-ch})

(def join-calls
  {:lifecycle/before-task-start inject-join-state})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(defn join-person [local-state segment]
  (let [state @local-state]
    (if-let [record (get state (:id segment))]
      (let [result (merge record segment)]
        (swap! local-state dissoc (:id segment))
        result)
      (do (swap! local-state assoc (:id segment) (dissoc segment :id))
          []))))

(deftest join-segments
  (let [id (random-uuid)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id id)
        peer-config (assoc (:peer-config config) :onyx/tenancy-id id)
        batch-size 2

        catalog [{:onyx/name :names
                  :onyx/plugin :onyx.plugin.core-async/input
                  :onyx/type :input
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1
                  :onyx/doc "Reads segments from a core.async channel"}

                 {:onyx/name :ages
                  :onyx/plugin :onyx.plugin.core-async/input
                  :onyx/type :input
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1
                  :onyx/doc "Reads segments from a core.async channel"}

                 {:onyx/name :join-person
                  :onyx/fn :onyx.peer.join-test/join-person
                  :onyx/type :function
                  :onyx/group-by-key :id
                  :onyx/min-peers 1
                  :onyx/flux-policy :kill
                  :onyx/batch-size batch-size}

                 {:onyx/name :out
                  :onyx/plugin :onyx.plugin.core-async/output
                  :onyx/type :output
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1
                  :onyx/doc "Writes segments to a core.async channel"}]

        workflow [[:names :join-person]
                  [:ages :join-person]
                  [:join-person :out]]

        lifecycles [{:lifecycle/task :names
                     :lifecycle/calls :onyx.peer.join-test/names-calls}
                    {:lifecycle/task :ages
                     :lifecycle/calls :onyx.peer.join-test/ages-calls}
                    {:lifecycle/task :out
                     :lifecycle/calls :onyx.peer.join-test/out-calls}
                    {:lifecycle/task :join-person
                     :lifecycle/calls :onyx.peer.join-test/join-calls}]]

    (reset! name-chan (chan (inc (count names))))
    (reset! name-buffer {})
    (reset! age-chan (chan (inc (count ages))))
    (reset! age-buffer {})
    (reset! out-chan (chan 10000))

    (with-test-env [test-env [4 env-config peer-config]]
      (doseq [name names]
        (>!! @name-chan name))

      (doseq [age ages]
        (>!! @age-chan age))

      (close! @name-chan)
      (close! @age-chan)

      (let [{:keys [job-id]} (onyx.api/submit-job
                              peer-config
                              {:catalog catalog :workflow workflow
                               :lifecycles lifecycles
                               :task-scheduler :onyx.task-scheduler/balanced})
            _ (onyx.test-helper/feedback-exception! peer-config job-id)
            results (take-segments! @out-chan 50)]
        (is (= (set people) (set results)))))))
