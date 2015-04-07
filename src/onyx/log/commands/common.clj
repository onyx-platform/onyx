(ns onyx.log.commands.common
  (:require [clojure.core.async :refer [chan]]
            [clojure.data :refer [diff]]
            [clojure.set :refer [map-invert]]
            [com.stuartsierra.component :as component]
            [onyx.extensions :as extensions]
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

(defn start-new-lifecycle [old new diff state]
  (let [old-allocation (peer->allocated-job (:allocations old) (:id state))
        new-allocation (peer->allocated-job (:allocations new) (:id state))]
    (if-not (= old-allocation new-allocation)
      (do (when (:lifecycle state)
            (component/stop @(:lifecycle state)))
          (let [seal-ch (chan)
                new-state (assoc state :job (:job new-allocation) :task (:task new-allocation) :seal-ch seal-ch)
                new-lifecycle (future (component/start ((:task-lifecycle-fn state) (select-keys new-allocation [:job :task]) new-state)))]
            (assoc new-state :lifecycle new-lifecycle :seal-response-ch seal-ch :job (:job new-allocation) :task (:task new-allocation))))
      state)))
