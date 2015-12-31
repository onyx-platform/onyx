(ns onyx.scheduling.acker-scheduler
  (:require [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]))

(defn exempt-from-acker? [replica job task]
  (or (some #{task} (get-in replica [:exempt-tasks job]))
      (and (get-in replica [:acker-exclude-inputs job])
           (some #{task} (get-in replica [:input-tasks job])))
      (and (get-in replica [:acker-exclude-outputs job])
           (some #{task} (get-in replica [:output-tasks job])))))

(defn find-physically-colocated-peers
  "Takes replica and a peer. Returns a set of peers, exluding this peer,
   that reside on the same physical machine."
  [replica peer]
  (let [peers (remove (fn [p] (= p peer)) (:peers replica))
        peer-site (extensions/get-peer-site replica peer)]
    (filter
     (fn [p]
       (= (extensions/get-peer-site replica p) peer-site))
     peers)))

(defn sort-acker-candidates
  "We try to be smart about which ackers we pick. If we can avoid
   colocating an acker and any peers executing an exempt task,
   we try to. It's a best effort, though, so if it's not possible
   we proceed anyway."
  [replica peers]
  (let [preferences (map (fn [peer]
                           (let [colocated-peers (find-physically-colocated-peers replica peer)
                                 statuses (map #(let [{:keys [job task]} (common/peer->allocated-job (:allocations replica) %)]
                                                  (exempt-from-acker? replica job task))
                                               colocated-peers)]
                             (some #{true} statuses)))
                         peers)]
    (->> peers
         (map list preferences)
         (sort-by first)
         (map second))))

(defn choose-acker-candidates [replica peers]
  (sort-acker-candidates
   replica
   (remove
    (fn [p]
      (let [{:keys [job task]} (common/peer->allocated-job (:allocations replica) p)]
        (exempt-from-acker? replica job task)))
    peers)))

(defn choose-ackers [replica jobs]
  (reduce
   (fn [result job]
     (let [peers (sort (common/replica->job-peers replica job))
           pct (or (get-in result [:acker-percentage job]) 10)
           n (int (Math/ceil (* 0.01 pct (count peers))))
           candidates (choose-acker-candidates result peers)]
       (assoc-in result [:ackers job] (vec (take n candidates)))))
   replica
   jobs))
