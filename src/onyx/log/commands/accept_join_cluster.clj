(ns onyx.log.commands.accept-join-cluster
  (:require [clojure.core.async :refer [chan go >! <! >!! close!]]
            [clojure.data :refer [diff]]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :accept-join-cluster
  [{:keys [args]} replica]
  (let [{:keys [accepted-joiner accepted-observer]} args
        target (or (get-in replica [:pairs accepted-observer])
                   accepted-observer)]
    (-> replica
        (update-in [:pairs] merge {accepted-observer accepted-joiner})
        (update-in [:pairs] merge {accepted-joiner target})
        (update-in [:accepted] dissoc accepted-observer)
        (update-in [:peers] vec)
        (update-in [:peers] conj accepted-joiner)
        (update-in [:peers] vec)
        (assoc-in [:peer-state accepted-joiner] :idle))))

(defmethod extensions/replica-diff :accept-join-cluster
  [entry old new]
  (let [rets (first (diff (:accepted old) (:accepted new)))]
    (assert (<= (count rets) 1))
    (when (seq rets)
      {:observer (first (keys rets))
       :subject (first (vals rets))})))

(defmethod extensions/reactions :accept-join-cluster
  [entry old new diff state]
  (when (common/volunteer-via-accept? old new diff state)
    [{:fn :volunteer-for-task :args {:id (:id state)}}]))

(defmethod extensions/fire-side-effects! :accept-join-cluster
  [entry old new diff state]
  (if (= (:id state) (:subject diff))
    (do (doseq [entry (:buffered-outbox state)]
          (>!! (:outbox-ch state) entry))
        (assoc (dissoc state :buffered-outbox) :stall-output? false))
    state))

