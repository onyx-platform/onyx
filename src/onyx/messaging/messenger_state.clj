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
  "Takes replica and a peer. Returns a set of peers, exluding this peer,
   that reside on the same physical machine."
  [replica peer-config peer job-id task-id]
  (let [peer-site (m/get-peer-site peer-config)
        task-peers (set (get-in replica [:allocations job-id task-id]))]
    (->> (:peers replica) 
         (filter (fn [p]
                   (= (get-in replica [:peer-sites p]) 
                      peer-site)))
         (filter task-peers))))

;; Maybe set shared tickets somewhere?
;; Put with replica version, each one will run it, if any have set then don't reset for that replica version, job-id task-id
(defn messenger-connections 
  [{:keys [peer-state allocations peer-sites task-slot-ids] :as replica} 
   {:keys [workflow catalog task serialized-task job-id id peer-opts] :as event}]
  (let [task-map (planning/find-task catalog task)
        {:keys [egress-tasks ingress-tasks]} serialized-task
        receivable-peers (fn [task-id] (get-in allocations [job-id task-id] []))
        this-task-id (:task-id event)
        egress-pubs (->> egress-tasks 
                         (mapcat (fn [task-id] 
                                   (let [peers (receivable-peers task-id)]
                                     (map (fn [peer-id]
                                            (let [slot-id (if (state-task? replica job-id task-id)
                                                            (get-in task-slot-ids [job-id task-id peer-id])
                                                            all-slots)] 
                                              (assert slot-id)
                                              {:type :message
                                               :src-peer-id id
                                               ;; Refactor dst-task-id to include job-id too
                                               :dst-task-id [job-id task-id]
                                               :slot-id slot-id
                                               :site (peer-sites peer-id)}))
                                          peers))))
                         set)
        ack-pubs (if (= (:onyx/type task-map) :output) 
                   (->> (common/root-tasks (:workflow event) (:task event))
                        (mapcat (fn [task-id] 
                                  (let [peers (receivable-peers task-id)]
                                    (map (fn [peer-id]
                                           {:type :ack
                                            :src-peer-id id
                                            :dst-task-id [job-id task-id]
                                            :slot-id (get-in task-slot-ids [job-id task-id peer-id])
                                            :site (peer-sites peer-id)})
                                         peers))))
                        set)
                   #{})
        ingress-subs (->> ingress-tasks 
                          (mapcat (fn [task-id] 
                                    (let [peers (receivable-peers task-id)]
                                      (map (fn [peer-id]
                                             (let [slot-id (if (state-task? replica job-id this-task-id)
                                                             (get-in task-slot-ids [job-id this-task-id id])
                                                             all-slots)] 
                                               (assert slot-id)
                                               {:type :message
                                                :src-peer-id peer-id
                                                :dst-task-id [job-id this-task-id]
                                                :slot-id slot-id
                                                :site (peer-sites id)
                                                :aligned-peers (if (state-task? replica job-id this-task-id)
                                                                 [id]
                                                                 (find-physically-task-peers replica peer-opts id job-id this-task-id))}))
                                           peers))))
                          set)
        ack-subs (if (= (:onyx/type task-map) :input) 
                   (->> (common/leaf-tasks (:workflow event) (:task event))
                        (mapcat (fn [task-id] 
                                  (let [peers (receivable-peers task-id)]
                                    (map (fn [peer-id]
                                           {:type :ack
                                            :src-peer-id peer-id
                                            :dst-task-id [job-id this-task-id]
                                            :site (peer-sites id)
                                            :slot-id (get-in task-slot-ids [job-id this-task-id id])})
                                         peers))))
                        set)
                   #{})
        coordinator-subs (if (= (:onyx/type task-map) :input) 
                           (if-let [coordinator-id (get-in replica [:coordinators job-id])]
                             #{{:type :message
                                ;; Should we allocate a coordinator a unique uuid?
                                :src-peer-id [:coordinator coordinator-id]
                                :dst-task-id [job-id this-task-id]
                                :site (peer-sites id)
                                :slot-id all-slots}}  
                             #{})
                           #{})]
    {:pubs (into ack-pubs egress-pubs)
     :acker-subs ack-subs
     :subs (into coordinator-subs ingress-subs)}))

(defn transition-messenger [messenger old-pub-subs new-pub-subs]
  (let [remove-pubs (difference (:pubs old-pub-subs) (:pubs new-pub-subs))
        add-pubs (difference (:pubs new-pub-subs) (:pubs old-pub-subs))
        remove-subs (difference (:subs old-pub-subs) (:subs new-pub-subs))
        add-subs (difference (:subs new-pub-subs) (:subs old-pub-subs))
        remove-acker-subs (difference (:acker-subs old-pub-subs) (:acker-subs new-pub-subs))
        add-acker-subs (difference (:acker-subs new-pub-subs) (:acker-subs old-pub-subs))]
    ;; Maybe initialise the subs and pubs with the right epoch messenger id?
    ;; That way you don't get -1 type things
    (as-> messenger m
      (reduce m/add-publication m add-pubs)
      (reduce m/add-subscription m add-subs)
      (reduce m/add-ack-subscription m add-acker-subs)
      (reduce m/remove-publication m remove-pubs)
      (reduce m/remove-subscription m remove-subs)
      (reduce m/remove-ack-subscription m remove-acker-subs))))

(defn assert-consistent-messenger-state [messenger pub-subs pre-post]
  (assert (= (count (:pubs pub-subs))
             (count (m/publications messenger)))
          "Incorrect publications")
  (assert (= (count (:subs pub-subs))
             (count (m/subscriptions messenger)))
          (str "Incorrect subscriptions"))
  (assert (= (count (:acker-subs pub-subs))
             (count (m/ack-subscriptions messenger)))
          "Incorrect acker subscriptions"))

(defn next-messenger-state! [messenger {:keys [job-id] :as event} old-replica new-replica]
  (assert (map? old-replica))
  (assert (map? new-replica))
  (assert (not= old-replica new-replica))
  (let [new-version (get-in new-replica [:allocation-version job-id])]
    (let [old-pub-subs (messenger-connections old-replica event)
          _ (assert-consistent-messenger-state messenger old-pub-subs :pre)
          new-pub-subs (messenger-connections new-replica event)
          new-messenger (-> messenger
                            (transition-messenger old-pub-subs new-pub-subs)
                            (m/set-replica-version! new-version))]
      (assert-consistent-messenger-state new-messenger new-pub-subs :post)
      new-messenger)))
