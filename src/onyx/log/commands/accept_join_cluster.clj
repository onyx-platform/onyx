(ns onyx.log.accept-join-cluster
  (:require [clojure.core.async :refer [chan go >! <! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :accept-join-cluster
  [kw {:keys [accepted updated-watch]}]
  (fn [replica message-id]
    (-> replica
        (update-in [:pairs] merge {(:observer accepted) (:subject accepted)})
        (update-in [:pairs] merge {(:observer updated-watch) (:subject updated-watch)})
        (update-in [:accepted] dissoc (:observer accepted))
        (update-in [:peers] conj (:observer accepted)))))

(defmethod extensions/replica-diff :accept-join-cluster
  [kw old new args]
  (let [rets (first (diff (:accepted old) (:accepted new)))]
    (assert (<= (count rets) 1))
    (when (seq rets)
      {:observer (first (keys rets))
       :subject (first (vals rets))})))

(defmethod extensions/reactions :accept-join-cluster
  [kw old new diff args]
  [])

(defmethod extensions/fire-side-effects! :accept-join-cluster
  [kw old new diff {:keys [env id]} state]
  (when (= id (:observer (:accepted state)))
    (extensions/flush-outbox (:outbox env))))

