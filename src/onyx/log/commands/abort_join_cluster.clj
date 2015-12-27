(ns onyx.log.commands.abort-join-cluster
  (:require [clojure.core.async :refer [chan go >! <! >!! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [schema.core :as s]
            [onyx.schema :refer [Replica LogEntry Reactions ReplicaDiff State]]
            [taoensso.timbre :refer [info] :as timbre]
            [onyx.extensions :as extensions]))

(s/defmethod extensions/apply-log-entry :abort-join-cluster :- Replica
  [{:keys [args message-id]} :- LogEntry replica]
  (if-not (get (set (:peers replica)) (:id args))
    (-> replica
        (update-in [:prepared] dissoc (get (map-invert (:prepared replica)) (:id args)))
        (update-in [:accepted] dissoc (get (map-invert (:accepted replica)) (:id args)))
        (update-in [:peer-sites] dissoc (:id args)))
    (do
      ;(info "Ignoring abort for " args (:peers replica))
      replica)))

(s/defmethod extensions/replica-diff :abort-join-cluster :- ReplicaDiff
  [entry :- LogEntry old new]
  (let [prepared (vals (first (diff (:prepared old) (:prepared new))))
        accepted (vals (first (diff (:accepted old) (:accepted new))))]
    (assert (<= (count prepared) 1))
    (assert (<= (count accepted) 1))
    (when (or (seq prepared) (seq accepted))
      {:aborted (or (first prepared) (first accepted))})))

(s/defmethod extensions/reactions :abort-join-cluster :- Reactions
  [{:keys [args]} old new diff peer-args]
  (when (and ;; not already joined
             (not (get (set (:peers old)) (:id args)))
             ;; and this is us
             (= (:id args) (:id peer-args))
             (not (:onyx.peer/try-join-once? (:peer-opts (:messenger peer-args)))))
    [{:fn :prepare-join-cluster
      :args {:joiner (:id peer-args)
             :peer-site (extensions/peer-site (:messenger peer-args))}}]))

(s/defmethod extensions/fire-side-effects! :abort-join-cluster :- State
  [{:keys [args]} old new diff state]
  ;; Abort back-off/retry
  (when (= (:id args) (:id state))
    (Thread/sleep (or (:onyx.peer/join-failure-back-off (:opts state)) 250)))
  state)
