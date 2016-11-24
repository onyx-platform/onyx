(ns onyx.peer.visualization)

(def task-monitoring (atom {}))

(defn reset-task-monitoring! []
  (reset! task-monitoring {}))

(defn strip-unknown-peers! [current-peers]
  (swap! task-monitoring 
         (fn [m]
           (->> m
                (filter (fn [[[peer-id task-name] _]]
                          (get current-peers peer-id)))
                (into {})))))

(defn update-monitoring! [state]
  (let [event (get-event state)
        peer-id (:id event)
        task-name (:task event)]
    (swap! task-monitoring 
           assoc 
           [peer-id task-name] 
           {:id peer-id
            :task task-name
            :messenger-slot-id (:messenger-slot-id event)
            :slot-id (:slot-id event)
            :messenger (m/info (get-messenger state))})))

(defn build-edges [mon]
  (let [;; Should we have some broken links when replicas aren't on same version and peer-id -> task is out of date?
        peer-id->task (into {} (keys mon))
        peer-id->slot-id (into {} (map (juxt :id :messenger-slot-id) (vals mon)))
        subscribers (->> mon
                         vals
                         (mapcat (fn [peer]
                                   (map (fn [{:keys [subscription status-pub]}]
                                          (let [src-channel (:channel status-pub)
                                                src-task (peer-id->task (:src-peer-id subscription))
                                                from (str [src-channel src-task (get peer-id->slot-id (:src-peer-id subscription))])
                                                to (str [(:channel (:messenger peer)) (:task peer) (:slot-id subscription)])]
                                            {:from from 
                                             :to to
                                             :src-peer-id (:src-peer-id subscription)
                                             :sub subscription})) 
                                        (:subscribers (:messenger peer))))))
        publishers (->> mon
                        vals
                        (mapcat (fn [peer]
                                  (map (fn [publisher]
                                         (let [src-task (:task peer)
                                               from (str [(:channel (:messenger peer)) src-task (:messenger-slot-id peer)])
                                               dst-channel (:dst-channel publisher)
                                               to (str [dst-channel (second (:dst-task-id publisher)) (:slot-id publisher)])]
                                           {:from from 
                                            :to to
                                            :src-peer-id (:id peer)
                                            :pub publisher})) 
                                       (:publishers (:messenger peer))))))]
    (->> (merge-with merge 
                     (->> subscribers
                          (map (fn [s]
                                 [(select-keys s [:from :to :src-peer-id]) s]))
                          (into {}))
                     (->> publishers
                          (map (fn [p]
                                 [(select-keys p [:from :to :src-peer-id]) p]))
                          (into {})))
         vals
         (map (fn [edge]
                [(:from edge) 
                 (:to edge) 
                 {:label 
                  [:TABLE {:BORDER 0}
                   [:TR [:TD "PUB-RDY?"] [:TD (:ready? (:pub edge))]]
                   [:TR [:TD "PUB-POS"] [:TD (:pos (:pub edge))]]
                   [:TR [:TD "SUB-BLK?"] [:TD (:blocked? (:sub edge))]]
                   [:TR [:TD "SUB-POS"] [:TD (:pos (:rev-image (:sub edge)))]]
                   [:TR [:TD "SRC-PEER"] [:TD (apply str (take 8 (str (:src-peer-id edge))))]]]}])))))

(defn build-graph [filename]
  (let [mon @task-monitoring
        chan-task-grouped (->> mon
                               vals
                               (group-by 
                                (fn [v]
                                  [(:channel (:messenger v)) (:task v) (:messenger-slot-id v)])))
        nodes (mapv (fn [[chan-task peers]]
                      {:id (str chan-task)
                       :color "blue" 
                       :label (into [:TABLE {:BORDER 0 :style {:font-size 6}}
                                     [:TR [:TD {:colspan 2} (str chan-task)]]]
                                    (map (fn [v]
                                           [:TR [:TD (:id v)]
                                            [:TD (:slot-id v)]
                                            [:TD (:replica-version (:messenger v))]
                                            [:TD (:epoch (:messenger v))]])
                                         peers))})
                    chan-task-grouped)
        edges (build-edges mon)
        dot (g/graph->dot nodes 
                          edges 
                          {:node {:shape :box :fontsize 8}
                           :edge {:shape :box :fontsize 8}
                           :node->id (fn [n] (if (keyword? n) (name n) (:id n)))
                           :node->descriptor (fn [n] (when-not (keyword? n) n))})]
    nodes
    ;{:nodes nodes :edges edges}
    (spit filename (g/dot->svg dot))))


