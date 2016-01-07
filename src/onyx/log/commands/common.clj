(ns onyx.log.commands.common
  (:require [clojure.core.async :refer [chan close!]]
            [clojure.data :refer [diff]]
            [clojure.set :refer [map-invert]]
            [com.stuartsierra.component :as component]
            [onyx.extensions :as extensions]
            [clj-tuple :as t]
            [taoensso.timbre :refer [info]]))

(defn peer-slot-id 
  [event]
  (let [replica (:onyx.core/replica event)
        job-id (:onyx.core/job-id event)
        peer-id (:onyx.core/id event)
        task-id (:onyx.core/task-id event)] 
    (get-in @replica [:task-slot-ids job-id task-id peer-id])))

(defn job->peers [replica]
  (reduce-kv
   (fn [all job tasks]
     (assoc all job (reduce into [] (vals tasks))))
   {} (:allocations replica)))

(defn replica->job-peers [replica job-id]
  (apply concat (vals (get-in replica [:allocations job-id]))))

(defn job-peer-count [replica job]
  (apply + (map count (vals (get-in replica [:allocations job])))))

(defn peer->allocated-job [allocations id]
  (if-let [[job-id [task-id]] 
           (first 
             (remove (comp empty? second) 
                     (map (fn [[job-id task-peers]]
                            [job-id (first (filter (fn [[task-id peers]]
                                                     (get (set peers) id))
                                                   task-peers))])
                          allocations)))]
    {:job job-id :task task-id}))

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

(defn backpressure? [replica job-id]
  (let [peers (job-allocations->peer-ids (:allocations replica) job-id)]
    (boolean
      (first
        (filter #(= % :backpressure)
                (map (:peer-state replica)
                     peers))))))

(defn remove-peers [replica id]
  (let [prev (get (allocations->peers (:allocations replica)) id)]
    (if (and (:job prev) (:task prev))
      (let [remove-f #(vec (remove (partial = id) %))
            deallocated (update-in replica [:allocations (:job prev) (:task prev)] remove-f)]
        ;; Avoids making a key path to nil if there was no task slot
        ;; for this peer.
        (if (get-in replica [:task-slot-ids (:jobs prev) (:task-prev id)])
          (update-in deallocated [:task-slot-ids (:job prev) (:task prev)] dissoc id)
          deallocated))
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

(defn should-seal? [replica job-id state message-id]
  (let [allocated-to-job? (= job-id (:job (peer->allocated-job (:allocations replica) (:id state))))]
    (and allocated-to-job?
         (all-inputs-exhausted? replica job-id)
         (executing-output-task? replica (:id state))
         (elected-sealer? replica message-id (:id state)))))

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

(defn job-receivable-peers [peer-state allocations job-id]
  (into (t/hash-map)
        (map (fn [[task-id peers]]
               (t/vector task-id
                         (into (t/vector)
                               (filter (fn [peer]
                                         (let [ps (peer-state peer)]
                                           (or (= ps :active)
                                               (= ps :backpressure))))
                                       peers))))
             (allocations job-id))))

(defn start-new-lifecycle [old new diff state]
  (let [old-allocation (peer->allocated-job (:allocations old) (:id state))
        new-allocation (peer->allocated-job (:allocations new) (:id state))]
    (if (not= old-allocation new-allocation)
      (do (when (:lifecycle state)
            (close! (:task-kill-ch (:task-state state)))
            (component/stop @(:lifecycle state)))
          (if (not (nil? new-allocation))
            (let [seal-ch (chan)
                  task-kill-ch (chan)
                  peer-site (get-in new [:peer-sites (:id state)])
                  task-state {:job-id (:job new-allocation) :task-id (:task new-allocation) 
                              :peer-site peer-site :seal-ch seal-ch :task-kill-ch task-kill-ch}
                  new-lifecycle (future (component/start ((:task-component-fn state) state task-state)))]
              (assoc state :lifecycle new-lifecycle :task-state task-state))
            (assoc state :lifecycle nil :task-state nil)))
      state)))
