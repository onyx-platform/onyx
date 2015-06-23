(ns onyx.test-job
  (:require [onyx.test-helper]
            [clojure.test :refer :all]
            [clojure.core.async :refer [chan >!! <!! close! sliding-buffer go-loop]]
            [taoensso.timbre :refer [fatal] :as timbre]
            [onyx.extensions :as extensions]
            [com.stuartsierra.component :as component]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config]]
            [onyx.api]))

(def config (load-config))

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(defn input-names [catalog]
  (->> catalog
       (filter (comp (partial = :input) :onyx/type))
       (map :onyx/name)))

(defn output-names [catalog]
  (->> catalog
       (filter (comp (partial = :output) :onyx/type))
       (map :onyx/name)))

(defn names->chans-map [names]
  (->> names
       (map (fn [k] (vector k (chan))))
       (into {})))

(defn workflow->inner-task-names [workflow]
  (clojure.set/intersection (set (map first workflow))
                            (set (map second workflow))))

(defn name->alt-name [k]
  (keyword (str (name k) "_alt")))

(defn out-task-name [task]
  (keyword (str (name task) "_out")))

(defn add-debug-output-tasks [{:keys [workflow catalog] :as job}]
  (let [inner-task-names (workflow->inner-task-names workflow)
        extra-dag-edges (map (juxt identity out-task-name) inner-task-names)]
    (-> job
        (update-in [:catalog] into (map (fn [task]
                                          {:onyx/name (out-task-name task)
                                           :onyx/ident :onyx.plugin.core-async/output
                                           :onyx/type :output
                                           :onyx/medium :core.async
                                           :onyx/batch-size 20
                                           :onyx/max-peers 1})
                                        inner-task-names))
        (update-in [:workflow] into extra-dag-edges))))

(defprotocol Manager
  (run-job [this job inputs])
  (add-peers [this n-peers])
  (remove-peers [this n-peers]))

(defrecord TestEnv [env-config peer-config]
  Manager
  (add-peers [component n-peers]
    (swap! (:v-peers component) into (onyx.api/start-peers n-peers (:peer-group component))))

  (remove-peers [component n-peers]
    (let [v-peers (take n-peers @(:v-peers component))]
      (doseq [v-peer v-peers]
        (onyx.api/shutdown-peer v-peer))
      (swap! (:v-peers component) #(vec (drop n-peers %)))))

  (run-job [component {:keys [workflow catalog task-scheduler] :as job} inputs]
    (let [n-peers-required (count (distinct (flatten workflow)))
          n-peers (count @(:v-peers component))] 
      (when (< n-peers n-peers-required)
        (throw (Exception. (format "%s peers required to run this job and only %s peers are available." 
                                   n-peers-required 
                                   n-peers)))))

                                        ; Convert all input and output tasks to core.async input and output tasks
                                        ; This code should be made into a helper function like add-debug-output-tasks
    (let [in-names (set (input-names catalog)) 
          out-names (set (output-names catalog))
          alt-name->name (into {} 
                               (map (fn [k] [(name->alt-name k) k]) 
                                    (concat in-names out-names)))
          in-names-alt (map name->alt-name in-names)
          in-chans (names->chans-map in-names-alt) 
          out-names-alt (map name->alt-name out-names)
          out-chans (names->chans-map out-names-alt) 
          in-entries (map (fn [n] 
                            {:onyx/name n
                             :onyx/ident :onyx.plugin.core-async/input
                             :onyx/type :input
                             :onyx/medium :core.async
                             :onyx/batch-size 20
                             :onyx/max-peers 1}) 
                          in-names-alt)
          out-entries (map (fn [n] 
                             {:onyx/name n
                              :onyx/ident :onyx.plugin.core-async/output
                              :onyx/type :output
                              :onyx/medium :core.async
                              :onyx/batch-size 20
                              :onyx/max-peers 1}) 
                           out-names-alt)
          lifecycles (map (fn [n]
                            {:lifecycle/name n
                             :lifecycle/calls (keyword (str "onyx.test-job/" n "-calls"))})
                          (concat in-names-alt out-names-alt))
          workflow-alt (mapv (fn [edge]
                               (mapv (fn [task] 
                                       (if (or (in-names task) (out-names task))
                                         (name->alt-name task)
                                         task)) 
                                     edge))
                             workflow)

          _ (doall 
             (map (fn [[k c]]
                    (let [f-sym (symbol (str "inject-" (name k)))]
                      (intern *ns* f-sym (eval `(fn ~k ~[] {:core.async/chan c})))
                      (intern *ns* (symbol (str (name k) "-calls"))
                              {:lifecycle/before-task-start f-sym})))
                  (merge in-chans out-chans)))

          job (onyx.api/submit-job (:peer-config component) 
                                   {:workflow workflow-alt
                                    :catalog (concat catalog in-entries out-entries)
                                    :lifecycles lifecycles
                                    :task-scheduler task-scheduler})

          _ (doall 
             (map (fn [[k in-ch]]
                    (doseq [v (inputs (alt-name->name k))]
                      (>!! in-ch v))
                    (>!! in-ch :done)
                    (close! in-ch))
                  in-chans))]
      (future
        (let [results (into {} 
                            (map (fn [[k out-ch]]
                                   [(alt-name->name k) 
                                    (take-segments! out-ch)])
                                 out-chans))]
          (doall (map (comp close! val) out-chans))
          results))))

  component/Lifecycle
  (component/start [component]
    (let [id (java.util.UUID/randomUUID)
          env-config (assoc env-config :onyx/id id)
          peer-config (assoc peer-config :onyx/id id)
          env (onyx.api/start-env env-config)]
      (try 
        (let [peer-group (onyx.api/start-peer-group peer-config)
              ch (chan)
              !replica (atom {})
              _ (go-loop [replica (extensions/subscribe-to-log (:log env) ch)]
                         (let [entry (<!! ch)
                               new-replica (extensions/apply-log-entry entry replica)]
                           (reset! !replica new-replica)
                           (recur new-replica)))] 
          (assoc component 
            :env-config env-config
            :peer-config peer-config
            :env env 
            :peer-group peer-group
            :replica !replica
            :v-peers (atom [])
            :log-ch ch))
        (catch Exception e
          (fatal e "Could not start peer group")
          (onyx.api/shutdown-env env)))))
  (component/stop [component]
    (close! (:log-ch component))
    (doseq [v-peer @(:v-peers component)]
      (try 
        (onyx.api/shutdown-peer v-peer)
        (catch Exception e
          (fatal e "Could not shutdown v-peer " v-peer))))
    (try 
      (onyx.api/shutdown-peer-group (:peer-group component))
      (catch Exception e
        (fatal e "Could not stop peer-group")))
    (try 
      (onyx.api/shutdown-env (:env component))
      (catch Exception e
        (fatal e "Could not stop environment")))
    (assoc component :env nil :peer-group nil :replica nil :v-peers nil)))

(comment
  (deftest test-test-env 
  (testing "Test env runs job with debug tasks"
    (let [env-t (component/start (map->TestEnv config))] 
      (try
        (add-peers env-t 4)         
        (is (= {:out [{:n 2} :done]
                :inc_out [{:n 2} :done]}
               @(run-job env-t (add-debug-output-tasks 
                                {:catalog [{:onyx/name :in
                                            :onyx/ident :onyx.plugin.core-async/input
                                            :onyx/type :input
                                            :onyx/medium :core.async
                                            :onyx/batch-size 20
                                            :onyx/max-peers 1
                                            :onyx/doc "Reads segments from a core.async channel"}

                                           {:onyx/name :inc
                                            :onyx/fn :onyx.test-job/my-inc
                                            :onyx/type :function
                                            :onyx/batch-size 20}

                                           {:onyx/name :out
                                            :onyx/ident :onyx.plugin.core-async/output
                                            :onyx/type :output
                                            :onyx/medium :core.async
                                            :onyx/batch-size 20
                                            :onyx/max-peers 1
                                            :onyx/doc "Writes segments to a core.async channel"}]
                                 :workflow [[:in :inc] [:inc :out]]
                                 :task-scheduler :onyx.task-scheduler/balanced})
                         {:in [{:n 1}]})))
        (finally (component/stop env-t)))))))
