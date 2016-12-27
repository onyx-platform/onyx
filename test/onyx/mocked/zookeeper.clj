(ns onyx.mocked.zookeeper
  (:require [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info error warn fatal]]
            [onyx.log.replica]
            [onyx.extensions :as extensions])
  (:import [org.apache.zookeeper KeeperException$BadVersionException]))

(defrecord FakeZooKeeper [config]
  component/Lifecycle
  (start [component] component)
  (stop [component] component))

(defn fake-zookeeper [entries store checkpoints config]
  (map->FakeZooKeeper {:entries entries
                       :store store
                       :checkpoints checkpoints
                       :entry-num 0
                       :config config}))

(defmethod extensions/write-log-entry FakeZooKeeper
  [log data]
  (swap! (:entries log) 
         (fn [entries] 
           (conj (vec entries)
                 (assoc data :message-id (count entries)))))
  log)

(defmethod extensions/read-log-entry FakeZooKeeper
  [{:keys [entries]} n]
  (get @entries n))

(defmethod extensions/register-pulse FakeZooKeeper
  [& all])

(defmethod extensions/on-delete FakeZooKeeper
  [& all])

(defmethod extensions/group-exists? FakeZooKeeper
  [& all]
  ;; Always show true - we will always manually leave
  true)

(defmethod extensions/subscribe-to-log FakeZooKeeper
  [log & _]
  (onyx.log.replica/starting-replica (:config log)))

(defmethod extensions/write-chunk :default
  [log kw chunk id]
  (cond 
   (= :task kw)
   (swap! (:store log) assoc [kw id (:id chunk)] chunk)
   (= :exception kw)
   (do (info "Task Exception:" chunk)
       (throw chunk))
   :else
   (swap! (:store log) assoc [kw id] chunk))
  log)

(defmethod extensions/read-chunk :default
  [log kw id & rst]
  (if (= :task kw)
    (get @(:store log) [kw id (first rst)])
    (get @(:store log) [kw id])))

(defmethod extensions/write-checkpoint FakeZooKeeper
  [log tenancy-id job-id replica-version epoch task-id slot-id checkpoint-type checkpoint]
  (info "Writing checkpoint:" replica-version epoch task-id slot-id)
  (swap! (:checkpoints log)
         assoc-in 
         [tenancy-id :checkpoints job-id [replica-version epoch] [task-id slot-id checkpoint-type]]
         checkpoint))

; (defmethod extensions/latest-full-checkpoint FakeZooKeeper
;   [log job-id required-checkpoints] 
;   (println "Checkpoints required" required-checkpoints)
;   ;(println "CHECKPOINTS HAS?" (get @(:checkpoints log) job-id))
;   (->> (get @(:checkpoints log) job-id)
;        (filterv (fn [[k v]]
;                   (= (set required-checkpoints) (set (keys v)))))
;        (sort-by key)
;        last
;        first))


(defmethod extensions/write-checkpoint-coordinate FakeZooKeeper
  [log tenancy-id job-id coordinate version]
  (let [path [tenancy-id :latest job-id]]
    (-> (swap! (:checkpoints log) 
               update-in 
               path
               (fn [v]
                 (if (= (or (:version v) 0) version)
                   {:version (inc version)
                    :coordinate coordinate}
                   (throw (KeeperException$BadVersionException. 
                           (str "failed write "
                                version
                                " vs "
                                (:version v)))))))

        (get-in path))))

(defmethod extensions/assume-checkpoint-coordinate FakeZooKeeper
  [log tenancy-id job-id]
  (let [exists (get-in @(:checkpoints log) [tenancy-id :latest job-id])
        version (get exists :version 0)
        coordinate (get exists :coordinate)]
    (:version (extensions/write-checkpoint-coordinate log tenancy-id job-id 
                                                      coordinate version))))

(defmethod extensions/read-checkpoint-coordinate FakeZooKeeper
  [log tenancy-id job-id]
  (if-let [coord (get-in @(:checkpoints log) [tenancy-id :latest job-id :coordinate])]
    [tenancy-id job-id coord]))

(defmethod extensions/read-checkpoint FakeZooKeeper
  [log tenancy-id job-id replica-version epoch task-id slot-id checkpoint-type]
  (println "RECOVER IS:" replica-version epoch)
  (-> @(:checkpoints log) 
      :checkpoints
      (get tenancy-id)
      (get job-id)
      (get [replica-version epoch])
      (get [task-id slot-id checkpoint-type])))
