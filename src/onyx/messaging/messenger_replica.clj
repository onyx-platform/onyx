(ns onyx.messaging.messenger-replica
  ;; FIXME: rename to messenger-state!
  (:require [clojure.set :refer [intersection union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.scheduling.common-task-scheduler :as cts]
            [onyx.log.commands.common :as common]
            [onyx.messaging.messenger :as m]
            [onyx.extensions :as extensions]
            [taoensso.timbre :refer [info warn]]
            [onyx.static.planning :as planning]
            [onyx.static.default-vals :refer [defaults arg-or-default]]))

; (defn build-pick-peer-fn
;   [replica job-id my-peer-id task-id task-map egress-peers slot-id->peer-id peer-config]
;   (let [out-peers (egress-peers task-id)
;         choose-f (cts/choose-downstream-peers replica job-id peer-config my-peer-id out-peers)]
;     (cond (empty? out-peers)
;           (fn [_] nil)

;           (and (planning/grouping-task? task-map) (#{:continue :kill} (:onyx/flux-policy task-map)))
;           (fn [hash-group] 
;             (nth out-peers
;                  (mod hash-group
;                       (count out-peers))))

;           (and (planning/grouping-task? task-map) (= :recover (:onyx/flux-policy task-map)))
;           (let [n-peers (or (:onyx/n-peers task-map)
;                             (:onyx/max-peers task-map))] 
;             (fn [hash-group] 
;               (slot-id->peer-id (mod hash-group n-peers))))

;           (planning/grouping-task? task-map) 
;           (throw (ex-info "Unhandled grouping-task flux-policy." task-map))

;           :else
;           (fn [hash-group]
;             (choose-f hash-group)))))

; (defn task-egress-publications [egress-tasks receivable-peers peer-sites site->publication]
;   (->> egress-tasks
;        (map (fn [[task-name task-id]]
;               (let [;slot-id->peer-id (map-invert (get slot-ids task-id))
;                     egress-peers (receivable-peers task-id)
;                     publications (->> egress-peers 
;                                       (map (comp :pub site->publication peer-sites))
;                                       vec)] 
;                 [task-id publications])))
;        (into {})))

(defn messenger-details 
  [{:keys [peer-state allocations peer-sites] :as replica} 
   {:keys [workflow catalog task serialized-task job-id id] :as event}]
  (let [task-map (planning/find-task catalog task)
        {:keys [egress-ids ingress-ids]} serialized-task
        ;; messenger is currently buggy when only using receivable peers
        ;; if job isn't covered by signaled ready peers
        ;receivable-peers (job-receivable-peers peer-state allocations job-id)
        receivable-peers (fn [task-id] (get-in allocations [job-id task-id] []))
        egress-pubs (->> (vals egress-ids) 
                         (mapcat (fn [task-id] 
                                   (let [peers (receivable-peers task-id)]
                                     (map (fn [peer-id]
                                            {:src-peer-id id
                                             :dst-task-id [job-id task-id]
                                             :site (peer-sites peer-id)})
                                          peers))))
                         set)
        ack-pubs (if (= (:onyx/type task-map) :output) 
                   (->> (common/root-tasks (:workflow event) (:task event))
                       (mapcat (fn [task-id] 
                                 (let [peers (receivable-peers task-id)]
                                   (map (fn [peer-id]
                                          {:src-peer-id id
                                           :dst-task-id [job-id task-id]
                                           :site (peer-sites peer-id)})
                                        peers))))
                       set)
                   #{})
        ingress-subs (->> (vals ingress-ids) 
                          (mapcat (fn [task-id] 
                                    (let [peers (receivable-peers task-id)]
                                      (map (fn [peer-id]
                                             {:src-peer-id peer-id
                                              :dst-task-id [job-id (:task-id event)]
                                              :site (peer-sites id)})
                                           peers))))
                          set)
        ack-subs (if (= (:onyx/type task-map) :input) 
                   (->> (common/leaf-tasks (:workflow event) (:task event))
                        (mapcat (fn [task-id] 
                                  (let [peers (receivable-peers task-id)]
                                    (map (fn [peer-id]
                                           {:src-peer-id peer-id
                                            :dst-task-id [job-id (:task-id event)]
                                            :site (peer-sites id)})
                                         peers))))
                        set)
                   #{})]
    {:pubs (into ack-pubs egress-pubs)
     :subs (into ack-subs ingress-subs)}))

(defn update-messenger [messenger old-pub-subs new-pub-subs]
  (let [remove-pubs (difference (:pubs old-pub-subs) (:pubs new-pub-subs))
        add-pubs (difference (:pubs new-pub-subs) (:pubs old-pub-subs))
        remove-subs (difference (:subs old-pub-subs) (:subs new-pub-subs))
        add-subs (difference (:subs new-pub-subs) (:subs old-pub-subs))]
    (as-> messenger m
      (reduce (fn [m pub] 
                ;(println "Should be removing pub" pub)
                (m/remove-publication m pub)) 
              m
              remove-pubs)
      (reduce (fn [m pub] 
                ;(println "Should be adding pub " pub)
                (m/add-publication m pub)) 
              m
              add-pubs)
      (reduce (fn [m sub] 
                ;(println "should be removing sub" sub)
                (m/remove-subscription m sub)) 
              m
              remove-subs)
      (reduce (fn [m sub] 
                ;(println "should be adding sub" sub)
                (m/add-subscription m sub)) 
              m
              add-subs))))

(defn assert-consistent-messenger-state [messenger pub-subs pre-post]
  (assert (= (count (:pubs pub-subs))
             (count (m/publications messenger)))
          (pr-str "Incorrect publications, peer-id:"
                  (:peer-id messenger)
                  pre-post
                  " expected: "
                  (:pubs pub-subs)
                  " actual: "
                  (m/publications messenger)))
  (assert (= (count (:subs pub-subs))
             (count (m/subscriptions messenger)))
          (pr-str "Incorrect subscriptions, peer-id:"
                  (:peer-id messenger)
                  pre-post
                  " expected: "
                  (:subs pub-subs) 
                  " actual: "
                  (m/subscriptions messenger))))

(defn new-messenger-state! [messenger {:keys [job-id] :as event} old-replica new-replica]
  (assert (map? old-replica))
  (assert (map? new-replica))
  (assert (not= old-replica new-replica))
  (let [new-version (get-in new-replica [:allocation-version job-id])]
    (let [old-pub-subs (messenger-details old-replica event)
          _ (assert-consistent-messenger-state messenger old-pub-subs :pre)
          new-pub-subs (messenger-details new-replica event)
          ;_ (println "Transitioning" (:peer-id messenger (m/replica-version messenger)) new-version)
          new-messenger (-> messenger
                            (m/set-replica-version new-version)
                            (update-messenger old-pub-subs new-pub-subs))]
      (assert-consistent-messenger-state new-messenger new-pub-subs :post)
      (if (= :input (:onyx/type (:task-map event)))
        ;; Emit initial barrier from input tasks upon new replica, 
        ;; essentially flushing everything out downstream
        (-> new-messenger m/emit-barrier)
        new-messenger))))
