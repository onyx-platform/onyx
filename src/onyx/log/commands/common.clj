(ns onyx.log.commands.common
  (:require [clojure.core.async :refer [chan close!]]
            [clojure.data :refer [diff]]
            [clojure.set :refer [map-invert]]
            [com.stuartsierra.component :as component]
            [onyx.extensions :as extensions]
            [clj-tuple :as t]
            [taoensso.timbre :refer [info]]))

(defn job->peers [replica]
  (reduce-kv
   (fn [all job tasks]
     (assoc all job (apply concat (vals tasks))))
   {} (:allocations replica)))

(defn peer->allocated-job [allocations id]
  (get
   (reduce-kv
    (fn [all job tasks]
      (->> tasks
           (mapcat (fn [[t ps]] (map (fn [p] {p {:job job :task t}}) ps)))
           (into {})
           (merge all)))
    {} allocations)
   id))

(defn allocations->peers [allocations]
   (reduce-kv
     (fn [all job tasks]
       (merge all
              (reduce-kv
                (fn [all task allocations]
                  (->> allocations
                       (map (fn [peer] {peer {:job job :task task}}))
                       (into {})
                       (merge all)))
                {}
                tasks)))
     {}
     allocations))

(defn job-allocations->peer-ids 
  [allocations job-id]
  (->> job-id
       allocations
       vals
       (reduce into (t/vector))))

(defn job-backpressuring? [replica job-id]
  (let [peers (job-allocations->peer-ids (:allocations replica) job-id)]
    (boolean 
      (first 
        (filter #(= % :backpressure) 
                (map (:peer-state replica) 
                     peers))))))

(defn remove-peers [replica id]
  (let [prev (get (allocations->peers (:allocations replica)) id)]
    (if (and (:job prev) (:task prev))
      (let [remove-f #(vec (remove (partial = id) %))]
        (update-in replica [:allocations (:job prev) (:task prev)] remove-f))
      replica)))

(defn all-inputs-exhausted? [replica job]
  (let [all (get-in replica [:input-tasks job])
        exhausted (get-in replica [:exhausted-inputs job])]
    (= (into #{} all) (into #{} exhausted))))

(defn executing-output-task? [replica id]
  (let [{:keys [job task]} (peer->allocated-job (:allocations replica) id)]
    (some #{task} (get-in replica [:output-tasks job]))))

(defn elected-sealer? [replica message-id id]
  (let [{:keys [job task]} (peer->allocated-job (:allocations replica) id)
        peers (get-in replica [:allocations job task])]
    (when (pos? (count peers))
      (let [n (mod message-id (count peers))]
        (= (nth peers n) id)))))

(defn should-seal? [replica args state message-id]
  (and (all-inputs-exhausted? replica (:job args))
       (executing-output-task? replica (:id state))
       (elected-sealer? replica message-id (:id state))))

(defn at-least-one-active? [replica peers]
  (->> peers
       (map #(get-in replica [:peer-state %]))
       (filter (partial = :active))
       (seq)))

(defn any-ackers? [replica job-id]
  (> (count (get-in replica [:ackers job-id])) 0))

(defn job-covered? [replica job]
  (let [tasks (get-in replica [:tasks job])
        active? (partial at-least-one-active? replica)]
    (every? identity (map #(active? (get-in replica [:allocations job %])) tasks))))

(defrecord PeerReplicaView [backpressure active-peers])

(defn transform-job-allocations [peer-state allocations]
  (into (t/hash-map) 
        (map (fn [[task-id peers]]
               (t/vector task-id
                         (into (t/vector) 
                               (filter (fn [peer] 
                                         (let [ps (peer-state peer)] 
                                           (or (= ps :active)
                                               (= ps :backpressure)))) 
                                       peers))))
             allocations)))


(defn peer-replica-view [replica peer-id]
  ;; This should be smarter about making a more personalised view
  ;; e.g. only calculate receivable peers for job the task is on and for downstream ids
  (let [allocations (:allocations replica)
        backpressure (into (t/hash-map) 
                           (map (fn [job-id] 
                                  (t/vector job-id 
                                            (job-backpressuring? replica job-id)))
                                (keys allocations)))
        peer-state (:peer-state replica)
        receivable-peers (into (t/hash-map)
                               (map (fn [[job-id job-allocations]]
                                      (t/vector job-id 
                                                (transform-job-allocations peer-state job-allocations)))
                                    allocations))] 
    (->PeerReplicaView backpressure receivable-peers)))

(defn start-new-lifecycle [old new diff state]
  (let [old-allocation (peer->allocated-job (:allocations old) (:id state))
        new-allocation (peer->allocated-job (:allocations new) (:id state))]
    (if (not= old-allocation new-allocation)
      (do (when (:lifecycle state)
            (close! (:task-kill-ch state))
            (component/stop @(:lifecycle state)))
          (if (not (nil? new-allocation))
            (let [seal-ch (chan)
                  task-kill-ch (chan)
                  new-state (assoc state :job (:job new-allocation) :task (:task new-allocation)
                                   :seal-ch seal-ch :task-kill-ch task-kill-ch)
                  new-lifecycle (future (component/start ((:task-lifecycle-fn state)
                                                          (select-keys new-allocation [:job :task]) new-state)))]
              (assoc new-state :lifecycle new-lifecycle))
            (assoc state :lifecycle nil :seal-ch nil :task-kill-ch nil :job nil :task nil)))
      state)))
