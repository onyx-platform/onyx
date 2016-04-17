(ns onyx.log.commands.task-state
  (:require [clojure.set :refer [intersection union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.scheduling.common-task-scheduler :as cts]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]
            [onyx.messaging.aeron :as aeron]
            [taoensso.timbre :refer [info warn]]
            [onyx.static.planning :as planning]
            [onyx.static.default-vals :refer [defaults arg-or-default]]))

; (defn peer-site [task-state peer-id]
;   (get (:peer-sites @task-state) peer-id))

; (defrecord PeerReplicaView
;   [site->publication task-id->publications barrier-publications root-task-ids job-id task-id workflow catalog task])

(defn build-pick-peer-fn
  [replica job-id my-peer-id task-id task-map egress-peers slot-id->peer-id peer-config]
  (let [out-peers (egress-peers task-id)
        choose-f (cts/choose-downstream-peers replica job-id peer-config my-peer-id out-peers)]
    (cond (empty? out-peers)
          (fn [_] nil)

          (and (planning/grouping-task? task-map) (#{:continue :kill} (:onyx/flux-policy task-map)))
          (fn [hash-group] 
            (nth out-peers
                 (mod hash-group
                      (count out-peers))))

          (and (planning/grouping-task? task-map) (= :recover (:onyx/flux-policy task-map)))
          (let [n-peers (or (:onyx/n-peers task-map)
                            (:onyx/max-peers task-map))] 
            (fn [hash-group] 
              (slot-id->peer-id (mod hash-group n-peers))))

          (planning/grouping-task? task-map) 
          (throw (ex-info "Unhandled grouping-task flux-policy." task-map))

          :else
          (fn [hash-group]
            (choose-f hash-group)))))

(defn remove-publication [{:keys [pub conn]}]
  (info "stopping pub " pub)
  (.close pub)
  (.close conn))

(defn stop-task-state [task-state]
  (info "removing in stop task state " (vec (vals (:site->publication task-state))))
  (run! remove-publication 
        (vals (:site->publication task-state))))

(defn task-egress-publications [egress-tasks receivable-peers peer-sites site->publication]
  (->> egress-tasks
       (map (fn [[task-name task-id]]
              (let [;slot-id->peer-id (map-invert (get slot-ids task-id))
                    egress-peers (receivable-peers task-id)
                    publications (->> egress-peers 
                                      (map (comp :pub site->publication peer-sites))
                                      vec)] 
                [task-id publications])))
       (into {})))

(defn cache-task-information [task-state log replica job-id task-id]
  (let [task (if (= task-id (:task-id task-state)) 
               (:task task-state)
               (extensions/read-chunk log :task task-id))
        catalog (if (= job-id (:job-id task-state)) 
                  (:catalog task-state)
                  (extensions/read-chunk log :catalog job-id))
        workflow (if (= job-id (:job-id task-state)) 
                   (:workflow task-state)
                   (extensions/read-chunk log :workflow job-id))
        root-task-ids (mapv
                        (fn [root-task]
                          (get-in replica [:task-name->id job-id root-task]))
                        (common/root-tasks workflow (:name task)))]
    (assoc task-state 
           :root-task-ids root-task-ids :job-id job-id :task-id task-id 
           :workflow workflow :catalog catalog :task task)))

(defn transition-publications! 
  [{:keys [workflow catalog task root-task-ids] :as task-state}
   {:keys [peer-state peer-sites allocations] :as replica}
   job-id]
  (let [task-map (planning/find-task catalog (:name task))
        egress-ids (:egress-ids task)
        receivable-peers (common/job-receivable-peers peer-state allocations job-id)
        ack-peers (if (= (:onyx/type task-map) :output) 
                    (mapcat receivable-peers root-task-ids))
        out-peers (mapcat receivable-peers (vals egress-ids))
        pub-sites (set (map peer-sites (into (set ack-peers) out-peers)))
        old-pub-sites (set (keys (:site->publication task-state)))
        new-pub-sites (difference pub-sites old-pub-sites)
        stale-pub-sites (difference old-pub-sites pub-sites)
        cont-pub-sites (intersection pub-sites old-pub-sites)
        cont-site->publication (select-keys (:site->publication task-state) cont-pub-sites)
        new-site->publication (zipmap new-pub-sites (mapv aeron/new-publication new-pub-sites))
        site->publication (merge cont-site->publication new-site->publication) 
        _ (assert (= (count site->publication) (+ (count cont-site->publication) (count new-site->publication))))
        ;; FIXME: doesn't account for the number of peers on a host. Should probably use a backpressure mechanism anyway
        ;; task-id to publications, for quick lookup and selection many publications that would output to a task 
        task-id->publications (task-egress-publications egress-ids receivable-peers peer-sites site->publication)
        ;; all the publications that a barrier must be output to
        barrier-publications (mapv :pub (vals site->publication))
        ;; peer -> publication lookup used for barrier acking
        ack-publications (zipmap ack-peers 
                                 (map (comp :pub site->publication peer-sites) 
                                      ack-peers))
        stale-publications (map (:site->publication task-state) stale-pub-sites)]
    (run! remove-publication stale-publications)
    (assoc task-state 
           :site->publication site->publication 
           :task-id->publications task-id->publications 
           :barrier-publications barrier-publications 
           :ack-publications ack-publications 
           :root-task-ids root-task-ids)))


(defn transition-subscriptions! 
  [{:keys [task] :as task-state}
   {:keys [peer-state peer-sites allocations] :as replica}
   messenger
   job-id]
  task-state
  #_(let [ingress-ids (:ingress-ids task)
        subscribers (mapv
                      (fn [src-peer-id]
                        (extensions/new-partial-subscriber messenger job-id src-peer-id (:id task)))
                      (common/src-peers replica ingress-ids job-id))]
    (assoc task-state 
           :subscriptions subscribers)))

(defmethod extensions/new-task-state! :default 
  [log entry old-replica new-replica diff state old-task-state peer-config]
  (let [peer-id (:id state)
        allocated-job (common/peer->allocated-job (:allocations new-replica) peer-id)
        job-id (:job allocated-job)
        task-id (:task allocated-job)]
    (if job-id 
      (-> old-task-state 
          (cache-task-information log new-replica job-id task-id)
          (transition-publications! new-replica job-id)
          (transition-subscriptions! new-replica (:messenger state) job-id)) 
      (do
        (stop-task-state old-task-state)
        (assoc old-task-state 
               :site->publication nil :task-id->publications nil 
               :barrier-publications nil :root-task-ids nil :job-id nil 
               :task-id nil :workflow nil :catalog nil :task nil)))))
