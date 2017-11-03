(ns onyx.messaging.messenger-state
  (:require [clojure.set :refer [intersection union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.scheduling.common-task-scheduler :as cts]
            [onyx.log.commands.common :as common]
            [onyx.messaging.aeron.utils :as autil]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.extensions :as extensions]
            [taoensso.timbre :refer [info warn]]
            [onyx.static.planning :as planning]
            [onyx.static.default-vals :refer [arg-or-default]]))

(defn build-final-publication-info [peer-opts job-id src-peer-id serialized-task [[site dst-task-id slot-id short-id] dsts]] 
    (let [short-circuit? (autil/short-circuit? peer-opts site)
          term-buffer-size (if short-circuit? 
                             (arg-or-default :onyx.messaging/term-buffer-size.segment-short-circuit peer-opts)
                             (arg-or-default :onyx.messaging/term-buffer-size.segment peer-opts))] 
      {:site site
       :short-id short-id
       :job-id job-id
       :src-peer-id src-peer-id
       :short-circuit? short-circuit?
       :term-buffer-size term-buffer-size
       :batch-size (get-in serialized-task [:egress-tasks-batch-sizes dst-task-id])
       :dst-task-id [job-id dst-task-id]
       :slot-id slot-id
       :dst-peer-ids (set (map :dst-peer-id dsts))}))

(defn messenger-connections 
  [{:keys [allocations peer-sites message-short-ids in->out] :as replica} 
   {:keys [onyx.core/workflow onyx.core/catalog onyx.core/task onyx.core/serialized-task 
           onyx.core/job-id onyx.core/id onyx.core/peer-opts] :as event}]
  (let [task-map (planning/find-task catalog task)
        this-task-id (:onyx.core/task-id event)
        egress-pubs (->> message-short-ids
                         ;; hacky workaround to strip coordinator pubs
                         (remove (fn [[{:keys [src-peer-type]} _]]
                                   (= :coordinator src-peer-type)))
                         (filter (fn [[{:keys [src-peer-id]} _]]
                                   (= src-peer-id id)))
                         (mapcat (fn [[{:keys [dst-task-id msg-slot-id*]} short-id]]
                                   (let [dst-peer-ids (get-in allocations [job-id dst-task-id])]
                                     (map (fn [dst-peer-id]
                                            {:site (peer-sites dst-peer-id)
                                             :job-id job-id
                                             :dst-peer-id dst-peer-id
                                             :dst-task-id dst-task-id
                                             :short-id short-id
                                             :slot-id (common/messenger-slot-id replica 
                                                                                job-id 
                                                                                dst-task-id 
                                                                                dst-peer-id)})
                                          dst-peer-ids))))
                         (group-by (juxt :site :dst-task-id :slot-id :short-id))
                         (map (partial build-final-publication-info peer-opts job-id id serialized-task))
                         set)
        sources-peers (filter (fn [[k _]]
                                (and (= job-id (:job-id k))
                                     (= task (:dst-task-id k))))
                              message-short-ids)
        ingress-sub {:sources (mapv (fn [[k short-id]]
                                      {:site (peer-sites (:src-peer-id k))
                                       :dst-task-id [job-id this-task-id]
                                       :short-id short-id
                                       :slot-id (common/messenger-slot-id replica job-id this-task-id id)
                                       ;; Hacky manual workaround for coordinator
                                       :src-peer-id (if (= :coordinator (:src-peer-type k)) 
                                                      [:coordinator (:src-peer-id k)]
                                                      (:src-peer-id k))})
                                    sources-peers)
                     :batch-size (:onyx/batch-size task-map)
                     :job-id job-id
                     ;; move dst task id to only task-id
                     :dst-task-id [job-id this-task-id]
                     :site (peer-sites id)
                     :slot-id (common/messenger-slot-id replica job-id this-task-id id)}]
    {:pubs egress-pubs
     :sub ingress-sub}))

(defn transition-messenger [messenger new-replica-version new-pub-subs]
  (-> messenger
      (m/update-subscriber (:sub new-pub-subs))
      (m/update-publishers (:pubs new-pub-subs))
      (m/set-replica-version! new-replica-version))) 

(defn next-messenger-state! [messenger {:keys [onyx.core/job-id] :as event} old-replica new-replica]
  (assert (map? old-replica))
  (assert (map? new-replica))
  (assert (not= old-replica new-replica))
  (let [new-version (get-in new-replica [:allocation-version job-id])
        new-pub-subs (messenger-connections new-replica event)
        new-messenger (transition-messenger messenger new-version new-pub-subs)]
    new-messenger))
