(ns onyx.log.commands.enable-virtual-peer
  (:require [onyx.extensions :as extensions]))

(defn add-site-acker [replica {:keys [joiner peer-site]}]
  (assert (:messaging replica) ":messaging key missing in replica, cannot continue")
  (-> replica
      (assoc-in [:peer-sites joiner]
                (merge
                  peer-site
                  (extensions/assign-acker-resources replica
                                                     joiner
                                                     peer-site
                                                     (:peer-sites replica))))))

(defn register-acker [state diff new]
  (when (= (:id state) (:subject diff))
    (extensions/register-acker
     (:messenger state)
     (get-in new [:peer-sites (:id state)]))))


;;; TODO - index vpeer -> group id and group id -> vpeer, probably useful
;;; TODO - add acker site on join
;;; TODO - handle group not joined yet
;;; TODO - joining peer should supply its tags
;;; TODO - make group-leave-cluster
;;; TODO - switch monitoring for peer-join-cluster to group-join-cluster, add peer-join attempts too, & accept, & notify

(comment
  (assoc-in [:peer-state joiner] :idle)
  (assoc-in [:peer-tags joiner] (:tags args))

  (extensions/register-acker (:messenger state)
                             (get-in new [:peer-sites (:id state)]))


  ;; on accept
  (if-not (= old new)
    (do (register-acker state diff new)
        (common/start-new-lifecycle old new diff state :peer-reallocated)))
  )

