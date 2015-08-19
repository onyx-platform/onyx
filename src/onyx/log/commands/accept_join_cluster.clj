(ns onyx.log.commands.accept-join-cluster
  (:require [clojure.core.async :refer [chan go >! <! >!! close!]]
            [clojure.data :refer [diff]]
            [clojure.set :refer [union difference map-invert]]
            [taoensso.timbre :refer [info] :as timbre]
            [onyx.extensions :as extensions]
            [onyx.log.commands.common :as common]
            [onyx.scheduling.common-job-scheduler :refer [reconfigure-cluster-workload]]))

(defmethod extensions/apply-log-entry :accept-join-cluster
  [{:keys [args]} replica]
  (let [{:keys [accepted-joiner accepted-observer]} args
        target (or (get-in replica [:pairs accepted-observer])
                   accepted-observer)
        accepted? (get-in replica [:accepted accepted-observer])
        already-joined? (some #{accepted-joiner} (:peers replica))
        no-observer? (not (some #{target} (:peers replica)))]
    (if (or already-joined? no-observer? (not accepted?))
      replica
      (-> replica
          (update-in [:pairs] merge {accepted-observer accepted-joiner})
          (update-in [:pairs] merge {accepted-joiner target})
          (update-in [:accepted] dissoc accepted-observer)
          (update-in [:peers] vec)
          (update-in [:peers] conj accepted-joiner)
          (assoc-in [:peer-state accepted-joiner] :idle)
          (reconfigure-cluster-workload)))))

(defmethod extensions/replica-diff :accept-join-cluster
  [entry old new]
  (if-not (= old new)
    (let [rets (first (diff (:accepted old) (:accepted new)))]
      (assert (<= (count rets) 1))
      (when (seq rets)
        {:observer (first (keys rets))
         :subject (first (vals rets))}))))

(defmethod extensions/reactions :accept-join-cluster
  [{:keys [args] :as entry} old new diff state]
  (let [accepted-joiner (:accepted-joiner args)
        already-joined? (some #{accepted-joiner} (:peers old))]
    (if (and (not already-joined?)
             (nil? diff)
             (= (:id state) accepted-joiner))
      [{:fn :abort-join-cluster
        :args {:id accepted-joiner}
        :immediate? true}]
      [])))

(defn unbuffer-messages [state diff new]
  (if (= (:id state) (:subject diff))
    (do (extensions/open-peer-site (:messenger state)
                                   (get-in new [:peer-sites (:id state)]))
        (doseq [entry (:buffered-outbox state)]
          (>!! (:outbox-ch state) entry))
        (assoc (dissoc state :buffered-outbox) :stall-output? false))
    state))

(defmethod extensions/fire-side-effects! :accept-join-cluster
  [{:keys [args]} old new diff {:keys [monitoring] :as state}]
  (when (= (:subject args) (:id state))
    (extensions/emit monitoring {:event :peer-accept-join :id (:id state)}))
  (if-not (= old new)
    (let [next-state (unbuffer-messages state diff new)]
      (common/start-new-lifecycle old new diff next-state))
    state))
