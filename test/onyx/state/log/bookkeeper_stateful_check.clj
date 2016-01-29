(ns onyx.state.log.bookkeeper-stateful-check
  (:require [stateful-check.core :refer [reality-matches-model print-test-results specification-correct?]]
            [onyx.log.generators :as loggen]
            [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [onyx.extensions :as ext]
            [onyx.state.state-extensions :as state-ext]
            [onyx.messaging.dummy-messenger :refer [dummy-messenger]]
            [onyx.peer.task-lifecycle :as task-lifecycle :refer [->WindowState]]
            [clojure.core.async :refer [chan close!]]
            [clojure.test.check :refer [quick-check] :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [onyx.test-helper :refer [load-config]]           
            [clojure.test :refer :all]
            [onyx.monitoring.no-op-monitoring :refer [no-op-monitoring-agent]]
            [onyx.state.log.bookkeeper :as log-bk]
            [onyx.log.commands.common]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]))

(def messenger (dummy-messenger {:onyx.peer/try-join-once? false}))

(def id (java.util.UUID/randomUUID))

(def config (load-config))

(def env-config (assoc (:env-config config) :onyx/id id))

(def peer-config (assoc (:peer-config config) 
                        :onyx/id id
                        :onyx.bookkeeper/write-batch-size 2 
                        :onyx.bookkeeper/write-batch-backoff 5))

(def bookkeeper-peer-state (atom nil))

(defn init-state [& args]
  [])

(defn new-pipeline []
  (let [pipeline {:onyx.core/task-id :task-id
                  :onyx.core/monitoring (no-op-monitoring-agent)
                  :onyx.core/kill-ch (chan)
                  :onyx.core/task-kill-ch (chan)
                  :onyx.core/peer-opts peer-config}] 
    (-> pipeline
        (assoc :onyx.core/window-state (->WindowState (state-ext/initialize-filter :set pipeline) (init-state)))
        (assoc :onyx.core/state-log (state-ext/initialize-log :bookkeeper pipeline)))))

(def previous-peer-state (atom nil))

(defn add-to-bookkeeper [arg]
  (let [event (:event @bookkeeper-peer-state)
        state-log (:onyx.core/state-log event)
        ack-promise (promise)]
    (swap! bookkeeper-peer-state update :ack-promises conj ack-promise)
    (state-ext/store-log-entry state-log event (fn [] (deliver ack-promise :done)) arg)))

(defn new-log-after-crash []
  (let [pipeline (new-pipeline)]
    (run! deref (:ack-promises @bookkeeper-peer-state))
    (reset! previous-peer-state @bookkeeper-peer-state)
    (swap! bookkeeper-peer-state assoc :event pipeline)))

(defn close-peer [event]
  (close! (:onyx.core/task-kill-ch event))
  (state-ext/close-log (:onyx.core/state-log event) event))

(defn new-peer-log []
  (reset! bookkeeper-peer-state {:ledger-ids []
                                 :ack-promises []
                                 :event-log []})
  ;; must setup before new-pipeline so that ledger id is recorded
  (swap! bookkeeper-peer-state assoc :event (new-pipeline)))

(def crash-and-playback-spec
  {:model/args (fn [state]
                 [])
   :next-state (fn [state [arg] result]
                 state)
   :real/postcondition (fn [prev-state next-state args result]
                         (try 
                           (let [playback-state (state-ext/playback-log-entries (:onyx.core/state-log (:event @bookkeeper-peer-state))
                                                                                (:event @bookkeeper-peer-state)
                                                                                (init-state)
                                                                                (fn [state entry]
                                                                                  (conj state entry)))]
                             (= next-state playback-state))
                              (finally
                                ;; cleanup *after* we playback and test, as this is realistic in cases where
                                ;; a log may not have been cleaned up
                                (close-peer (:event @previous-peer-state)))))
   :real/command #'new-log-after-crash})

(def add-to-log-espec
  {:model/args (fn [state]
                 [gen/int])
   :next-state (fn [state [arg] result]
                 (conj state arg))
   :real/command #'add-to-bookkeeper})

(defn close-peer-cleanup [state]
  (close-peer (:event @bookkeeper-peer-state)))

(def log-spec
  {:commands {:add-to-log add-to-log-espec
              :crash-and-playback crash-and-playback-spec}
   :real/setup #'new-peer-log 
   :real/cleanup #'close-peer-cleanup
   :real/postcondition (fn [state] true)
   :initial-state #'init-state})

(deftest log-test-correct
  (with-redefs [log-bk/assign-bookkeeper-log-id-spin (fn [event new-ledger-id]
                                                       (swap! bookkeeper-peer-state update :ledger-ids conj new-ledger-id))
                log-bk/event->ledger-ids (fn [event] 
                                           (:ledger-ids @bookkeeper-peer-state))
                onyx.log.commands.common/peer-slot-id (fn [event] 0)]
    (let [env (onyx.api/start-env env-config)]
      (try
        (is (specification-correct? log-spec {:num-tests 10}))
        (finally
          (onyx.api/shutdown-env env))))))
