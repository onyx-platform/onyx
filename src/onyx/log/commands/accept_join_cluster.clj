(ns onyx.log.commands.accept-join-cluster
  (:require [clojure.core.async :refer [chan go >! <! >!! close!]]
            [clojure.data :refer [diff]]
            [taoensso.timbre :refer [info] :as timbre]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :accept-join-cluster
  [{:keys [args site-resources]} replica]
  (let [observer (:accepted-observer args)
        self-stitched (:self-stitched args)
        joiner (or (:accepted-joiner args) self-stitched)]
    (-> (if self-stitched
          replica
          (-> replica
              (assoc-in [:pairs observer] joiner)
              (assoc-in [:pairs joiner] (or (get-in replica [:pairs observer]) 
                                            observer))
              (update-in [:accepted] dissoc observer)))
        (update-in [:peers] set)
        (update-in [:peers] conj joiner)
        (assoc-in [:peer-state joiner] :idle)
        (assoc-in [:peer-site-resources joiner] site-resources))))

(defmethod extensions/replica-diff :accept-join-cluster
  [entry old new]
  (let [subject (first (second (diff (:peers old) (:peers new))))]
    (if-let [observer (get (:pairs new) subject)]
      {:subject subject :observer observer}
      {:subject subject})))

(defmethod extensions/reactions :accept-join-cluster
  [entry old new diff state]
  (when (common/volunteer-via-accept? old new diff state)
    [{:fn :volunteer-for-task :args {:id (:id state)}}]))

(defmethod extensions/fire-side-effects! :accept-join-cluster
  [entry old new diff state]
  (if (= (:id state) (:subject diff))
    (let [site-resources (get-in new [:peer-site-resources (:subject diff)])] 
      (extensions/open-peer-site (:messenger state) site-resources)
      (doseq [entry (:buffered-outbox state)]
          (>!! (:outbox-ch state) entry))
        (assoc (dissoc state :buffered-outbox) :stall-output? false))
    state))

