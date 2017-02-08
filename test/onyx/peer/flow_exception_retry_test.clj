(ns onyx.peer.flow-exception-retry-test
  (:require  [clojure.test :as t]
             [clojure.core.async :refer [chan >!! <!! close! go]]
             [onyx.plugin.core-async :refer [take-segments! get-core-async-channels]]
             [onyx.test-helper :refer [load-config with-test-env]]
             [onyx.static.uuid :refer [random-uuid]]
             [onyx.api]))

(def heal? (atom false))

(def results (atom {{:n 0} 0
                    {:n 1} 0
                    {:n 2} 0
                    {:n 3} 0
                    {:n 4} 0
                    {:n 5} 0
                    {:n 6} 0
                    {:n 7} 0
                    {:n 8} 0
                    {:n 9} 0}))

(defn my-inc [segment]
  (if @heal?
    (update segment :n inc)
    (do (swap! results update segment inc)
        (throw (Exception.)))))

(def always-true (constantly true))

(defn check-within-one [results]
  (let [upper (apply max (vals results))
        lower (apply min (vals results))]
    (every? (set [upper lower]) (vals results))))

;; broken because we don't support flow condition retries
(t/deftest ^:broken exception-retry
  (let [id (random-uuid)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id id)
        peer-config (assoc (:peer-config config) :onyx/tenancy-id id)
        batch-size 10
        batch-timeout 100

        workflow [[:in :inc]
                  [:inc :out]]

        catalog [{:onyx/name :in
                  :onyx/plugin :onyx.plugin.core-async/input
                  :onyx/type :input
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/batch-timeout batch-timeout
                  :onyx/max-peers 1
                  :onyx/doc "Reads segments from a core.async channel"}

                 {:onyx/name :inc
                  :onyx/fn ::my-inc
                  :onyx/type :function
                  :onyx/batch-size batch-size
                  :onyx/batch-timeout batch-timeout}

                 {:onyx/name :out
                  :onyx/plugin :onyx.plugin.core-async/output

                  :onyx/type :output
                  :onyx/medium :core.async
                  :onyx/max-peers 1
                  :onyx/batch-size batch-size
                  :onyx/batch-timeout batch-timeout
                  :onyx/doc "Writes segments to a core.async channel"}]
        lifecycles [{:lifecycle/task :in
                     :core.async/id (random-uuid)
                     :lifecycle/calls :onyx.plugin.core-async/in-calls}
                    {:lifecycle/task :out
                     :core.async/id (random-uuid)
                     :lifecycle/calls :onyx.plugin.core-async/out-calls}]
        flow-conditions [{:flow/from :inc
                          :flow/to :none
                          :flow/thrown-exception? true
                          :flow/short-circuit? true
                          :flow/predicate ::always-true
                          :flow/action :retry}]
        job {:catalog catalog :lifecycles lifecycles
             :flow-conditions flow-conditions :workflow workflow
             :task-scheduler :onyx.task-scheduler/balanced}
        {:keys [in out]} (get-core-async-channels job)
        ;;; Wait 10 seconds before healing the exception throwing task
        healer (future (do (Thread/sleep 10000) (reset! heal? true)))]
    (with-test-env [test-env [3 env-config peer-config]]
      (try
        (doseq [x (range 10)]
          (>!! in {:n x}))
        (>!! in :done)
        (onyx.api/submit-job peer-config job)
        (t/is (= (set (take-segments! out))
                 (set [{:n 1} {:n 2} {:n 3} {:n 4} {:n 5} {:n 6} {:n 7} {:n 8} {:n 9} {:n 10} :done])))
        ;; Check that the amount of times a segment was seen before the heal
        ;; is within 1 lifecycle loop, because the heal could have happened partway
        ;; through the [{:n 1} {:n 2}...] sequence.
        (t/is (check-within-one @results))
        (finally
          (future-cancel healer))))))
