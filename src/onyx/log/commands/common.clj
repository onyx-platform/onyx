(ns onyx.log.commands.common
  (:require [clojure.core.async :refer [chan promise-chan close! thread <!! >!! alts!!]]
            [clojure.data :refer [diff]]
            [clojure.set :refer [map-invert]]
            [schema.core :as s]
            [onyx.schema :as os]
            [onyx.peer.constants :refer [load-balance-slot-id]]
            [com.stuartsierra.component :as component]
            [com.stuartsierra.dependency :as dep]
            [onyx.extensions :as extensions]
            [onyx.static.default-vals :refer [arg-or-default]]
            [clj-tuple :as t]
            [taoensso.timbre :refer [info warn]]))

(defn state-task? [replica job-id task-id]
  (get-in replica [:state-tasks job-id task-id]))

(defn grouped-task? [replica job-id task-id]
  (get-in replica [:grouped-tasks job-id task-id]))

(defn input-task? [replica job-id task-id]
  (get-in replica [:input-tasks job-id task-id]))

(defn messenger-slot-id [replica job-id task-id peer-id]
  ;; grouped tasks need to receive on their own slot
  ;; input tasks only receive barriers can all can use same channel
  (if (and (grouped-task? replica job-id task-id)
           (not (input-task? replica job-id task-id)))
    (get-in replica [:task-slot-ids job-id task-id peer-id])
    load-balance-slot-id))

(defn upstream-peers [replica ingress-tasks job-id]
  (reduce
   (fn [result task-id]
     (into result (get-in replica [:allocations job-id task-id])))
   []
   ingress-tasks))

(defn job->peers [replica]
  (reduce-kv (fn [all job tasks]
               (assoc all job (reduce into [] (vals tasks))))
             {} 
             (:allocations replica)))

(defn replica->job-peers [replica job-id]
  (apply concat (vals (get-in replica [:allocations job-id]))))

(defn job-peer-count [replica job]
  (apply + (map count (vals (get-in replica [:allocations job])))))

(defn src-peers [replica ingress-tasks job-id]
  (reduce
   (fn [result task-id]
     (into result (get-in replica [:allocations job-id task-id])))
   []
   ingress-tasks))

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

(s/defn remove-peers [replica :- os/Replica id :- os/PeerId] :- os/Replica
  (let [prev (get (allocations->peers (:allocations replica)) id)]
    (if (and (:job prev) (:task prev))
      (let [remove-f #(vec (remove (partial = id) %))
            deallocated (-> replica 
                            (update-in [:allocations (:job prev) (:task prev)] remove-f)
                            (update-in [:coordinators] dissoc (get (map-invert (:coordinators replica)) id)))]
        ;; Avoids making a key path to nil if there was no task slot
        ;; for this peer.
        (if (get-in replica [:task-slot-ids (:job prev) (:task prev) id])
          (update-in deallocated [:task-slot-ids (:job prev) (:task prev)] dissoc id)
          deallocated))
      replica)))

(defn start-task! [lifecycle]
  (thread (component/start lifecycle)))

(defn build-stop-task-fn [external-kill-flag started-task-ch]
  (fn [scheduler-event]
    (reset! external-kill-flag true)
    (let [started (<!! started-task-ch)] 
      (component/stop (assoc-in started [:task-lifecycle :scheduler-event] scheduler-event)))))

(defn stop-lifecycle-peer-group! [state scheduler-event]
  (when (:lifecycle-stop-fn state)
    ;; stop lifecycle in future, and send future to peer group for monitoring.
    (>!! (:group-ch state) 
         [:stop-task-lifecycle [(:id state) (System/nanoTime) (future ((:lifecycle-stop-fn state) scheduler-event))]])))

(s/defn start-new-lifecycle [old :- os/Replica new :- os/Replica diff state scheduler-event :- os/PeerSchedulerEvent]
  (let [old-allocation (peer->allocated-job (:allocations old) (:id state))
        new-allocation (peer->allocated-job (:allocations new) (:id state))
        old-allocation-version (get-in old [:allocation-version (:job old-allocation)])
        new-allocation-version (get-in new [:allocation-version (:job new-allocation)])]
    (if (not= old-allocation new-allocation)
      (do 
       (stop-lifecycle-peer-group! state scheduler-event)  
       (if (not (nil? new-allocation))
         (let [internal-kill-flag (atom false)
               external-kill-flag (atom false)
               peer-site (get-in new [:peer-sites (:id state)])
               task-state {:job-id (:job new-allocation)
                           :task-id (:task new-allocation)
                           :peer-site peer-site
                           :kill-flag external-kill-flag
                           :task-kill-flag internal-kill-flag}
               lifecycle (-> ((:task-component-fn state) state task-state)
                             (assoc-in [:task-lifecycle :scheduler-event] scheduler-event)
                             (assoc-in [:task-lifecycle :replica-origin] new))
               started-task-ch (start-task! lifecycle)
               lifecycle-stop-fn (build-stop-task-fn external-kill-flag started-task-ch)]
           (assoc state
                  :lifecycle lifecycle
                  :started-task-ch started-task-ch
                  :lifecycle-stop-fn lifecycle-stop-fn
                  :task-state task-state))
         (assoc state :lifecycle nil :lifecycle-stop-fn nil :started-task-ch nil :task-state nil)))
      state)))

(defn promote-orphans [replica group-id]
  (assert group-id)
  (let [orphans (get-in replica [:orphaned-peers group-id])]
    (-> replica
        (update-in [:peers] into orphans)
        (update-in [:peers] vec)
        (update-in [:orphaned-peers] dissoc group-id))))
