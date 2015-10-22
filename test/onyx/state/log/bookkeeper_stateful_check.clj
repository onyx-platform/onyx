(ns onyx.state.log.bookkeeper-stateful-check
  (:require [stateful-check.core :refer [reality-matches-model print-test-results]]
            [onyx.log.generators :as loggen]
            [onyx.extensions :as ext]
            [onyx.state.state-extensions :as state-ext]
            [onyx.messaging.dummy-messenger :refer [dummy-messenger]]
            [onyx.peer.task-lifecycle :as task-lifecycle :refer [->WindowState]]
            [clojure.test.check :refer [quick-check] :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [onyx.test-helper :refer [load-config]]           
            [clojure.test :refer :all]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]))

(comment
(defn new-replica []
  (atom {:replica {:job-scheduler :onyx.job-scheduler/balanced
                   :messaging {:onyx.messaging/impl :dummy-messenger}}
         :message-id 0}))

(def messenger (dummy-messenger {:onyx.peer/try-join-once? false}))

(def env 
  (let [id (java.util.UUID/randomUUID)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/id id)
        peer-config (assoc (:peer-config config) 
                           :onyx/id id
                           :onyx.bookkeeper/write-batch-size 1
                           :onyx.bookkeeper/write-batch-timeout 10000000)
        env (onyx.api/start-env env-config)]

    (try
      
      
(def new-peer-spec
  {:next-state (fn [state args result]
                 (let [pipeline {}] 
                   {:event (assoc pipeline 
                                  :onyx.core/window-state 
                                  (->WindowState (state-ext/initialize-filter :set pipeline) 
                                                 {}))}))
   :real/command #'new-replica})


(def new-log-entry
  {:model/args (fn [state]
                 (gen/tuple
                   (gen/return (:replica-real state))
                   (gen/elements (:peers state))))
   :model/precondition (fn [state _]
                         (not-empty (:peers state)))
   :real/postcondition (fn [state next-state args updated-real]
                         (= (count (:peers (:replica updated-real)))
                            (count (:peers next-state))))
   :real/command #'apply-leave-entry
   :next-state (fn [state [_ peer-id] _]
                 (update-in state [:peers] disj peer-id))})

(def replica-spec
  {:commands {:new-peer new-peer-spec
              :new-log-entry new-log-entry
              :add-peer add-peer-spec
              :remove-peer remove-peer-spec}
   :model/generate-command (fn [state]
                             (cond (nil? state)
                                   (gen/return :new-replica)
                                   (not-empty (:peers state))
                                   (gen/elements [:add-peer :remove-peer])
                                   :else
                                   (gen/elements [:add-peer])))})


#_(defspec run-replica-spec 5 (reality-matches-model? replica-spec))
#_(print-test-results replica-spec (quick-check 50 (reality-matches-model? replica-spec) :seed 1417059242645))



(finally
  (onyx.api/shutdown-env env))))))
