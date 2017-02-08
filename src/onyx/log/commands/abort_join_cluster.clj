(ns onyx.log.commands.abort-join-cluster
  (:require [clojure.core.async :refer [chan go >! <! >!! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [schema.core :as s]
            [onyx.schema :refer [Replica LogEntry Reactions ReplicaDiff State]]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.static.default-vals :refer [arg-or-default]]
            [taoensso.timbre :refer [info] :as timbre]
            [onyx.extensions :as extensions]))

(defn already-joined? [replica entry]
  (some #{(:id (:args entry))} (:peers replica)))

(s/defmethod extensions/apply-log-entry :abort-join-cluster :- Replica
  [{:keys [args] :as entry} :- LogEntry replica]
  (assert (:id args))
  (if-not (already-joined? replica entry)
    (-> replica
        (update-in [:prepared] dissoc (get (map-invert (:prepared replica)) (:id args)))
        (update-in [:accepted] dissoc (get (map-invert (:accepted replica)) (:id args)))
        (update-in [:aborted] (fnil conj #{}) (:id args)))
    replica))

(s/defmethod extensions/replica-diff :abort-join-cluster :- ReplicaDiff
  [entry :- LogEntry old new]
  (let [prepared (vals (first (diff (:prepared old) (:prepared new))))
        accepted (vals (first (diff (:accepted old) (:accepted new))))]
    (assert (<= (count prepared) 1))
    (assert (<= (count accepted) 1))
    (when (or (seq prepared) (seq accepted))
      {:aborted (or (first prepared) (first accepted))})))

(s/defmethod extensions/reactions [:abort-join-cluster :group] :- Reactions
  [{:keys [args] :as entry} old new diff state]
  ;; Rename these keys to onyx.node or onyx.peer-group?
  (when (and (not (:onyx.peer/try-join-once? (:peer-opts (:messenger state))))
             (not (already-joined? old entry))
             (= (:id args) (:id state)))
    [{:fn :prepare-join-cluster
      :args {:joiner (:id state)}}]))

(s/defmethod extensions/fire-side-effects! [:abort-join-cluster :group] :- State
  [{:keys [args]} old new diff state]
  ;; Abort back-off/retry
  (when (= (:id args) (:id state))
    ;; Back off for a randomized time, mean :onyx.peer/join-failure-back-off
    ;; Rename these keys to onyx.node or onyx.peer-group?
    (Thread/sleep (rand-int (* 2 (arg-or-default :onyx.peer/join-failure-back-off (:opts state))))))
  state)
