(ns onyx.log.commands.signal-ready
  (:require [clojure.core.async :refer [>!!]]
            [clojure.data :refer [diff]]
            [onyx.log.commands.common :as common]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :signal-ready
  [{:keys [args]} replica]
  (let [id (:id args)] 
    (if (and (common/peer->allocated-job (:allocations replica) id)
             (= :idle (get-in replica [:peer-state id])))
      (assoc-in replica [:peer-state id] :active)
      replica)))

(defmethod extensions/replica-diff :signal-ready
  [{:keys [args]} old new]
  (second (diff (:peer-state old) (:peer-state new))))

(defmethod extensions/reactions :signal-ready
  [{:keys [args]} old new diff peer-args]
  [])

(defmethod extensions/fire-side-effects! :signal-ready
  [{:keys [args message-id]} old new diff state]
  (let [job (:job (common/peer->allocated-job (:allocations new) (:id state)))]
    (when (common/should-seal? new {:job job} state message-id)
      (>!! (:seal-ch state) true)))
  state)
