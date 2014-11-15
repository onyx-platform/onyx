(ns onyx.log.accept-join-cluster
  (:require [clojure.core.async :refer [chan go >! <! >!! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :accept-join-cluster
  [{:keys [args]} replica]
  (let [{:keys [accepted updated-watch]} args]
    (-> replica
        (update-in [:pairs] merge {(:observer accepted) (:subject accepted)})
        (update-in [:pairs] merge {(:observer updated-watch) (:subject updated-watch)})
        (update-in [:accepted] dissoc (:observer accepted))
        (update-in [:peers] conj (:observer accepted)))))

(defmethod extensions/replica-diff :accept-join-cluster
  [entry old new]
  (let [rets (first (diff (:accepted old) (:accepted new)))]
    (assert (<= (count rets) 1))
    (when (seq rets)
      {:observer (first (keys rets))
       :subject (first (vals rets))})))

(defmethod extensions/reactions :accept-join-cluster
  [entry old new diff peer-args]
  [])

(defmethod extensions/fire-side-effects! :accept-join-cluster
  [entry old new diff state]
  (if (= (:id state) (:observer (:accepted diff)))
    (do (doseq [entry (:buffered-outbox state)]
          (>!! (:outbox-ch state) entry))
        (dissoc state :buffered-outbox))
    state))

