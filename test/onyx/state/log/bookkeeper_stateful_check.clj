(ns onyx.state.log.bookkeeper-stateful-check
  (:require [stateful-check.core :refer [reality-matches-model print-test-results specification-correct?]]
            [onyx.log.generators :as loggen]
            [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
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
            [onyx.state.log.bookkeeper :as log-bk]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]))

(defn new-replica []
    (atom {:replica {:job-scheduler :onyx.job-scheduler/balanced
                     :messaging {:onyx.messaging/impl :dummy-messenger}}
           :message-id 0}))


(def messenger (dummy-messenger {:onyx.peer/try-join-once? false}))

(def id (java.util.UUID/randomUUID))

(def config (load-config))

(def env-config (assoc (:env-config config) :onyx/id id))

(def peer-config (assoc (:peer-config config) 
                        :onyx/id id
                        :onyx.bookkeeper/write-batch-size 1
                        :onyx.bookkeeper/write-batch-timeout 10000000))

(def initial-bookkeeper-state {:logs [[]] :ledger-ids [0]})

(def bookkeeper-state (atom initial-bookkeeper-state))
(def bookkeeper-peer-state (atom nil))

(defn add-to-bookkeeper [arg]
  (swap! bookkeeper-state (fn [bs-state]
                            (update-in bs-state [:logs (last (:ledger-ids bs-state))] conj arg))))

(def add-to-log-espec
  {:model/args (fn [state]
                 [gen/int])
   :next-state (fn [state [arg] result]
                 (conj state arg))
   :real/postcondition (fn [prev-state next-state [arg] result]
                         (= (reduce into [] (:logs @bookkeeper-state)) next-state))
   :real/command #'add-to-bookkeeper})

(defn switch-to-next-ledger []
  (swap! bookkeeper-state (fn [bs-state]
                            (-> bs-state
                                (update :logs conj [])
                                (update :ledger-ids conj (inc (apply max (:ledger-ids bs-state))))))))

(def peer-crash-new-log-spec
  {:model/args (fn [state]
                 [])
   :next-state (fn [state [arg] result]
                 state)
   ;:real/postcondition (fn [prev-state next-state [arg] result]
   ;                      (= (reduce into [] (:logs @bookkeeper-state)) next-state))
   :real/command #'switch-to-next-ledger})

(defn new-peer-log []
  (let [bk-peer-val {:ledger-ids []
                     :event-log []
                     :event (let [pipeline {:onyx.core/peer-opts peer-config}] 
                              (-> pipeline
                                  ;(assoc :onyx.core/window-state (->WindowState (state-ext/initialize-filter :set pipeline) {}))
                                  (assoc :onyx.core/state-log (state-ext/initialize-log :bookkeeper pipeline))))}] 
    (reset! bookkeeper-state initial-bookkeeper-state)
    (reset! bookkeeper-peer-state bk-peer-val)))

(defn close-peer-log [state]
  (let [real-state-event (:event @bookkeeper-peer-state)] 
    (info " Closing log")
    (state-ext/close-log (:onyx.core/state-log real-state-event) real-state-event)))

(def log-spec
  {:commands {:add-to-log add-to-log-espec
              :peer-crash-new-log peer-crash-new-log-spec}
   :real/setup #'new-peer-log 
   :real/cleanup #'close-peer-log
   :real/postcondition (fn [final-state]
                         (= (reduce into [] (:logs @bookkeeper-state)) final-state))
   :initial-state (fn [setup] [])
   ;:model/generate-command (fn [state]
   ;                          (gen/elements [(gen/return :add-to-log)
   ;                                         (gen/return :peer-crash-new-log)]))
   })


(deftest log-test-correct
  (with-redefs [log-bk/assign-bookkeeper-log-id-spin (fn [event new-ledger-id]
                                                       (swap! bookkeeper-peer-state 
                                                              (fn [ps]
                                                                (update ps :ledger-ids conj new-ledger-id))))]
    (let [env (onyx.api/start-env env-config)]
      (try
        (is (specification-correct? log-spec))
        (finally
          (onyx.api/shutdown-env env))))))
