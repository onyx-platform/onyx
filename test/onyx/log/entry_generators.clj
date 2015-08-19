(ns onyx.log.entry-generators
  (:require [stateful-check.core :refer [reality-matches-model print-test-results]]
            [onyx.log.generators :as loggen]
            [onyx.extensions :as ext]
            [onyx.messaging.dummy-messenger :refer [dummy-messenger]]
            [clojure.test.check :refer [quick-check] :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test :refer :all]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]))

(defn new-replica []
  (atom {:replica {:job-scheduler :onyx.job-scheduler/balanced
                   :messaging {:onyx.messaging/impl :dummy-messenger}}
         :message-id 0}))

(def new-replica-spec
  {:next-state (fn [state args result]
                 {:replica-real result
                  :peers #{}
                  :peer-counter 0})
   :real/command #'new-replica})

(def messenger (dummy-messenger {:onyx.peer/try-join-once? false}))

(defn peerless-entry? [log-entry]
  (#{:submit-job :kill-job :gc} (:fn log-entry)))

(defn active-peers [replica entry]
  (cond-> (set (concat (:peers replica)
                       ; might not need these with the below entries
                       (vals (or (:prepared replica) {}))
                       (vals (or (:accepted replica) {}))))

    ;; if peer is joining, immediately count them as part of the peers that can generate reactions
    (= :prepare-join-cluster (:fn entry))
    (conj (:joiner (:args entry)))

    (and (not (peerless-entry? entry))
         (:id (:args entry))) (conj (:id (:args entry)))
    (and (not (peerless-entry? entry))
         (:joiner (:args entry))) (conj (:joiner (:args entry)))))

(defn apply-entry [{:keys [replica message-id]} entry]
  (let [entry (assoc entry :message-id message-id)
        updated-replica (ext/apply-log-entry entry replica)
        diff (ext/replica-diff entry replica updated-replica)
        peers (active-peers updated-replica entry)
        peer-reactions (mapcat (fn [peer-id]
                                 (ext/reactions entry
                                                replica
                                                updated-replica
                                                diff
                                                {:messenger messenger
                                                 :id peer-id
                                                 :opts {:onyx.peer/try-join-once?
                                                        (:onyx.peer/try-join-once? (:opts messenger) true)}}))
                               peers)]
    (reduce apply-entry
            {:message-id (inc message-id)
             :replica updated-replica}
            peer-reactions)))

(defn apply-join-entry [replica-real peer-id]
  (reset! replica-real (apply-entry @replica-real (loggen/build-join-entry peer-id))))

(defn apply-leave-entry [replica-real peer-id]
  (reset! replica-real (apply-entry @replica-real {:fn :leave-cluster :args {:id peer-id}})))

(def add-peer-spec
  {:model/args (fn [state]
                 (gen/tuple
                   (gen/return (:replica-real state))
                   (gen/return (:peer-counter state))))
   :model/precondition (fn [state _]
                         (:peer-counter state))
   :real/postcondition (fn [state next-state args updated-real]
                         (= (count (:peers (:replica updated-real)))
                            (count (:peers next-state))))
   :real/command #'apply-join-entry
   :next-state (fn [state [_ peer-counter] _]
                 (-> state
                     (update-in [:peers] conj peer-counter)
                     (update-in [:peer-counter] inc)))})

(def remove-peer-spec
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
  {:commands {:new-replica new-replica-spec
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
