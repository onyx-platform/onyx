(ns onyx.log.commands.common
  (:require [clojure.core.async :refer [chan close!]]
            [clojure.data :refer [diff]]
            [clojure.set :refer [map-invert]]
            [schema.core :as s]
            [onyx.schema :as os]
            [com.stuartsierra.component :as component]
            [com.stuartsierra.dependency :as dep]
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

(defn src-peers [replica ingress-ids job-id]
  (reduce
   (fn [result task-id]
     (into result (get-in replica [:allocations job-id task-id])))
   []
   ingress-ids))

(defn to-graph [workflow]
  (reduce
   (fn [g [from to]]
     (dep/depend g to from))
   (dep/graph)
   workflow))

(defn root-tasks [workflow leaf-task]
  (let [graph (to-graph workflow)]
    (filter
     (fn [dep]
       (not (seq (dep/immediate-dependencies graph dep))))
     (dep/transitive-dependencies graph leaf-task))))

(defn leaf-tasks [workflow root-task]
  (let [graph (to-graph workflow)]
    (filter
     (fn [dep]
       (not (seq (dep/immediate-dependents graph dep))))
     (dep/transitive-dependents graph root-task))))

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
        (if (get-in replica [:task-slot-ids (:job prev) (:task prev) id])
          (update-in deallocated [:task-slot-ids (:job prev) (:task prev)] dissoc id)
          deallocated))
      replica)))

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

(s/defn start-new-lifecycle [old :- os/Replica new :- os/Replica diff state scheduler-event :- os/PeerSchedulerEvent]
  (let [old-allocation (peer->allocated-job (:allocations old) (:id state))
        new-allocation (peer->allocated-job (:allocations new) (:id state))]
    (if (not= old-allocation new-allocation)
      (do (when (:lifecycle state)
            (close! (:task-kill-ch (:task-state state)))
            (component/stop (assoc-in @(:lifecycle state) [:task-lifecycle :scheduler-event] scheduler-event)))
          (if (not (nil? new-allocation))
            (let [task-kill-ch (chan)
                  peer-site (get-in new [:peer-sites (:id state)])
                  task-state {:job-id (:job new-allocation) :task-id (:task new-allocation) 
                              :peer-site peer-site :task-kill-ch task-kill-ch}
                  lifecycle (assoc-in ((:task-component-fn state) state task-state) 
                                      [:task-lifecycle :scheduler-event] 
                                      scheduler-event)
                  new-lifecycle (future (component/start lifecycle))]
              (assoc state :lifecycle new-lifecycle :task-state task-state))
            (assoc state :lifecycle nil :task-state nil)))
      state)))
