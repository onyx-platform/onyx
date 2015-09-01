(ns onyx.log.commands.abort-join-cluster
  (:require [clojure.core.async :refer [chan go >! <! >!! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [schema.core :as s]
            [onyx.schema :refer [Replica LogEntry Reactions]]
            [taoensso.timbre :as timbre]
            [onyx.extensions :as extensions]))

(s/defmethod extensions/apply-log-entry :abort-join-cluster :- Replica
  [{:keys [args message-id]} :- LogEntry replica :- Replica]
  (-> replica
      (update-in [:prepared] dissoc (get (map-invert (:prepared replica)) (:id args)))
      (update-in [:accepted] dissoc (get (map-invert (:accepted replica)) (:id args)))
      (update-in [:peer-sites] dissoc (:id args))))

(s/defmethod extensions/replica-diff :abort-join-cluster
  [entry old :- Replica new :- Replica]
  (let [prepared (vals (first (diff (:prepared old) (:prepared new))))
        accepted (vals (first (diff (:accepted old) (:accepted new))))]
    (assert (<= (count prepared) 1))
    (assert (<= (count accepted) 1))
    (when (or (seq prepared) (seq accepted))
      {:aborted (or (first prepared) (first accepted))})))

(s/defmethod extensions/reactions :abort-join-cluster :- Reactions
  [{:keys [args]} old :- Replica new :- Replica diff peer-args]
  (when (and (= (:id args) (:id peer-args))
             (not (:onyx.peer/try-join-once? (:peer-opts (:messenger peer-args)))))
    [{:fn :prepare-join-cluster
      :args {:joiner (:id peer-args)
             :peer-site (extensions/peer-site (:messenger peer-args))}
      :immediate? true}]))

(s/defmethod extensions/fire-side-effects! :abort-join-cluster
  [{:keys [args]} :- LogEntry old :- Replica new :- Replica diff state]
  ;; Abort back-off/retry
  (when (= (:id args) (:id state))
    (Thread/sleep (or (:onyx.peer/join-failure-back-off (:opts state)) 250)))
  state)
