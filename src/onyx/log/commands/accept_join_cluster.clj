(ns onyx.log.commands.accept-join-cluster
  (:require [clojure.core.async :refer [chan go >! <! >!! close!]]
            [clojure.data :refer [diff]]
            [clojure.set :refer [union difference map-invert]]
            [taoensso.timbre :refer [info] :as timbre]
            [onyx.extensions :as extensions]
            [onyx.peer.virtual-peer]
            [onyx.log.commands.common :as common]
            [schema.core :as s]
            [onyx.schema :refer [Replica LogEntry Reactions ReplicaDiff State]]
            [onyx.scheduling.common-job-scheduler :refer [reconfigure-cluster-workload]]))

(s/defmethod extensions/apply-log-entry :accept-join-cluster :- Replica
  [{:keys [args]} :- LogEntry replica :- Replica]
  (let [{:keys [accepted-joiner accepted-observer]} args
        target (or (get-in replica [:pairs accepted-observer])
                   accepted-observer)
        accepted? (= accepted-joiner (get-in replica [:accepted accepted-observer]))
        already-joined? (some #{accepted-joiner} (:groups replica))
        no-observer? (not (some #{target} (:groups replica)))]
    (if (or already-joined? no-observer? (not accepted?))
      replica
      (-> replica
          (update-in [:pairs] merge {accepted-observer accepted-joiner})
          (update-in [:pairs] merge {accepted-joiner target})
          (update-in [:accepted] dissoc accepted-observer)
          (update-in [:groups] vec)
          (update-in [:groups] conj accepted-joiner)
          (common/promote-orphans accepted-joiner)
          (reconfigure-cluster-workload replica)))))

(s/defmethod extensions/replica-diff :accept-join-cluster :- ReplicaDiff
  [entry :- LogEntry old :- Replica new :- Replica]
  (if-not (= old new)
    (let [rets (first (diff (:accepted old) (:accepted new)))]
      (assert (<= (count rets) 1))
      (when (seq rets)
        {:observer (first (keys rets))
         :subject (first (vals rets))}))))

(s/defmethod extensions/reactions [:accept-join-cluster :group] :- Reactions
  [{:keys [args] :as entry} :- LogEntry 
   old new diff :- ReplicaDiff state :- State]
  (let [accepted-joiner (:accepted-joiner args)]
    (if (and (nil? diff)
             (= (:id state) accepted-joiner))
      [{:fn :abort-join-cluster
        :args {:id accepted-joiner}}]
      [])))

(s/defmethod extensions/fire-side-effects! [:accept-join-cluster :group] :- State
  [{:keys [args]} :- LogEntry 
   old :- Replica 
   new :- Replica 
   diff :- ReplicaDiff 
   {:keys [monitoring] :as state} :- State]
  (when (= (:subject args) (:id state))
    (extensions/emit monitoring {:event :group-accept-join :id (:id state)}))
  state)

(s/defmethod extensions/fire-side-effects! [:accept-join-cluster :peer] :- State
  [{:keys [args message-id] :as entry} old new diff state]
  (common/start-new-lifecycle old new diff state :peer-reallocated))
