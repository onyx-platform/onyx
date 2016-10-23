(ns onyx.messaging.messenger-state
  (:require [clojure.set :refer [intersection union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.scheduling.common-task-scheduler :as cts]
            [onyx.log.commands.common :as common]
            [onyx.messaging.messenger :as m]
            [onyx.extensions :as extensions]
            [taoensso.timbre :refer [info warn]]
            [onyx.static.planning :as planning]
            [onyx.static.default-vals :refer [defaults arg-or-default]]))

(def all-slots -1)

(defn state-task? [replica job-id task-id]
  (get-in replica [:state-tasks job-id task-id]))

(defn find-physically-task-peers
  "Takes replica and a peer. Returns a set of peers, excluding this peer,
   that reside on the same physical machine."
  [replica peer-config peer job-id task-id]
  (let [peer-site (m/get-peer-site peer-config)
        task-peers (set (get-in replica [:allocations job-id task-id]))]
    (->> (:peers replica) 
         (filter (fn [p]
                   (= (get-in replica [:peer-sites p]) 
                      peer-site)))
         (filter task-peers))))

(defn get-slot-id [replica job-id task-id peer-id]
  (if (state-task? replica job-id task-id)
    (get-in replica [:task-slot-ids job-id task-id peer-id])
    all-slots))

;; Maybe set shared tickets somewhere?
;; Put with replica version, each one will run it, if any have set then don't reset for that replica version, job-id task-id
(defn messenger-connections 
  [{:keys [peer-state allocations peer-sites] :as replica} 
   {:keys [workflow catalog task serialized-task job-id id peer-opts] :as event}]
  (let [task-map (planning/find-task catalog task)
        {:keys [egress-tasks ingress-tasks]} serialized-task
        receivable-peers (fn [task-id] (get-in allocations [job-id task-id] []))
        this-task-id (:task-id event)
        egress-pubs (->> egress-tasks 
                         (mapcat (fn [task-id] 
                                   (let [peers (receivable-peers task-id)]
                                     (map (fn [peer-id]
                                            {:src-peer-id id
                                             ;; Refactor dst-task-id to include job-id too
                                             :dst-task-id [job-id task-id]
                                              ;; lookup the slot-id I'm sending to
                                             :slot-id (get-slot-id replica job-id task-id peer-id)
                                             :site (peer-sites peer-id)})
                                          peers))))
                         set)
        ingress-subs (->> ingress-tasks 
                          (mapcat (fn [task-id] 
                                    (let [peers (receivable-peers task-id)]
                                      (map (fn [peer-id]
                                             {:src-peer-id peer-id
                                              :dst-task-id [job-id this-task-id]
                                              ;; lookup my slot-id
                                              :slot-id (get-slot-id replica job-id this-task-id id)
                                              :site (peer-sites id)
                                              :aligned-peers (if (state-task? replica job-id this-task-id)
                                                               [id]
                                                               (find-physically-task-peers replica peer-opts id job-id this-task-id))})
                                           peers))))
                          set)
        coordinator-subs (if (= (:onyx/type task-map) :input) 
                           (if-let [coordinator-id (get-in replica [:coordinators job-id])]
                             #{{;; Should we allocate a coordinator a unique uuid?
                                :src-peer-id [:coordinator coordinator-id]
                                :dst-task-id [job-id this-task-id]
                                :site (peer-sites id)
                                :slot-id all-slots}}  
                             #{})
                           #{})]
    {:pubs egress-pubs
     :subs (into coordinator-subs ingress-subs)}))

(defn transition-messenger [messenger new-replica-version old-pub-subs new-pub-subs]
  (let [remove-pubs (difference (:pubs old-pub-subs) (:pubs new-pub-subs))
        add-pubs (difference (:pubs new-pub-subs) (:pubs old-pub-subs))
        remove-subs (difference (:subs old-pub-subs) (:subs new-pub-subs))
        add-subs (difference (:subs new-pub-subs) (:subs old-pub-subs))]
    ;; Maybe initialise the subs and pubs with the right epoch messenger id?
    ;; That way you don't get -1 type things
    (as-> messenger m
      (reduce m/add-publication m add-pubs)
      (reduce m/add-subscription m add-subs)
      (reduce m/remove-publication m remove-pubs)
      (reduce m/remove-subscription m remove-subs)
      (m/set-replica-version! m new-replica-version))))

(defn assert-consistent-messenger-state [messenger pub-subs pre-post]
  (assert (= (count (:pubs pub-subs))
             (count (m/publications messenger)))
          "Incorrect publications")
  (assert (= (count (:subs pub-subs))
             (count (m/subscriptions messenger)))
          (str "Incorrect subscriptions")))

(defn next-messenger-state! [messenger {:keys [job-id] :as event} old-replica new-replica]
  (assert (map? old-replica))
  (assert (map? new-replica))
  (assert (not= old-replica new-replica))
  (let [new-version (get-in new-replica [:allocation-version job-id])
        old-pub-subs (messenger-connections old-replica event)
        _ (assert-consistent-messenger-state messenger old-pub-subs :pre)
        new-pub-subs (messenger-connections new-replica event)
        new-messenger (transition-messenger messenger new-version old-pub-subs new-pub-subs)]
    (assert-consistent-messenger-state new-messenger new-pub-subs :post)
    new-messenger))
