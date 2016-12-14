(ns onyx.messaging.messenger-state
  (:require [clojure.set :refer [intersection union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.scheduling.common-task-scheduler :as cts]
            [onyx.log.commands.common :as common]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.extensions :as extensions]
            [taoensso.timbre :refer [info warn]]
            [onyx.static.planning :as planning]
            [onyx.static.default-vals :refer [defaults arg-or-default]]))

(defn messenger-connections 
  [{:keys [allocations peer-sites] :as replica} 
   {:keys [workflow catalog task serialized-task job-id id peer-opts] :as event}]
  (let [task-map (planning/find-task catalog task)
        {:keys [egress-tasks ingress-tasks]} serialized-task
        receivable-peers (fn [task-id] (get-in allocations [job-id task-id] []))
        this-task-id (:task-id event)
        egress-pubs (->> egress-tasks 
                         (mapcat (fn [task-id] 
                                   (let [peers (->> task-id 
                                                    (receivable-peers)
                                                    ;; collocated peers
                                                    (group-by (fn [peer-id]
                                                                [(common/messenger-slot-id replica job-id task-id peer-id)
                                                                 (peer-sites peer-id)])))]
                                     (map (fn [[[slot-id peer-site] peer-ids]]
                                            {:src-peer-id id
                                             ;; Refactor dst-task-id to include job-id too
                                             :dst-task-id [job-id task-id]
                                             ;; lookup the slot-id I'm sending to
                                             ;; should group on this
                                             ;; TODO, add update-publications
                                             ;; then can transition properly, update dst-peer-ids
                                             :dst-peer-ids (set peer-ids)
                                             ;; Need to find-physically task peers for the peer site, but without 
                                             :slot-id slot-id
                                             :site peer-site})
                                          peers))))
                         set)
        source-peers (->> ingress-tasks 
                          (mapcat receivable-peers)
                          set)
        ingress-sub (if (= (:onyx/type task-map) :input) 
                      (if-let [coordinator-id (get-in replica [:coordinators job-id])]
                        {;; Should we allocate a coordinator a unique uuid?
                         :sources [{:site (peer-sites coordinator-id)
                                    :src-peer-id [:coordinator coordinator-id]}]
                         :batch-size (:onyx/batch-size task-map)
                         :dst-task-id [job-id this-task-id]
                         :site (peer-sites id)
                         ;; input tasks can all listen on the same slot
                         ;; because the barriers are the same
                         ;; even if the input peers are allocated to different slots
                         :slot-id common/all-slots}  
                        ;; May be cases where not fully allocated?
                        ;; Check on this
                        (throw (ex-info "Job should always have a coordinator, or messenger should be stopped instead." 
                                        {:replica replica})))
                      {:sources (mapv (fn [peer-id] 
                                        {:site (peer-sites peer-id)
                                         :src-peer-id peer-id}) 
                                      source-peers)
                       ;; batch-size unused? should just be fragment message count alone?
                       :batch-size 10
                       :dst-task-id [job-id this-task-id]
                       ;; lookup my slot-id
                       :slot-id (common/messenger-slot-id replica job-id this-task-id id)
                       :site (peer-sites id)})]
    {:pubs egress-pubs
     :sub ingress-sub}))

(defn transition-messenger [messenger new-replica-version new-pub-subs]
  (-> messenger
      (m/update-subscriber (:sub new-pub-subs))
      (m/update-publishers (:pubs new-pub-subs))
      (m/set-replica-version! new-replica-version)))

(defn next-messenger-state! [messenger {:keys [job-id] :as event} old-replica new-replica]
  (assert (map? old-replica))
  (assert (map? new-replica))
  (assert (not= old-replica new-replica))
  (let [new-version (get-in new-replica [:allocation-version job-id])
        new-pub-subs (messenger-connections new-replica event)
        new-messenger (transition-messenger messenger new-version new-pub-subs)]
    new-messenger))
