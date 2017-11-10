(ns onyx.gc
  (:require [clojure.core.async :refer [chan <!!]]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.extensions :as extensions]
            [onyx.checkpoint :as checkpoint]
            [onyx.static.default-vals :refer [arg-or-default]]))

(defn gc-log [onyx-client]
  (let [id (java.util.UUID/randomUUID)
        entry (create-log-entry :gc {:id id})
        ch (chan 1000)]
    (extensions/write-log-entry (:log onyx-client) entry)
    (loop [replica (extensions/subscribe-to-log (:log onyx-client) ch)]
      (let [entry (<!! ch)
            new-replica (extensions/apply-log-entry entry (assoc replica :version (:message-id entry)))]
        (if (and (= (:fn entry) :gc) (= (:id (:args entry)) id))
          (let [diff (extensions/replica-diff entry replica new-replica)
                args {:id id :type :client :log (:log onyx-client)}]
            (extensions/fire-side-effects! entry replica new-replica diff args))
          (recur new-replica))))
    true))

(defn build-checkpoint-targets [log job-id max-rv max-epoch]
  (let [watermarks (checkpoint/read-all-replica-epoch-watermarks log job-id)
        sorted-watermarks (sort-by :replica-version watermarks)]
    (reduce
     (fn [all {:keys [replica-version epoch task-data]}]
       (cond (< replica-version max-rv)
             (conj all {:replica-version replica-version
                        :epoch-range (range 1 (inc epoch))
                        :task-data task-data})

             (= replica-version max-rv)
             (conj all {:replica-version replica-version
                        :epoch-range (range 1 epoch)
                        :task-data task-data})

             :else all))
     []
     sorted-watermarks)))

(defn storage-connection [peer-config log]
  (if (= :zookeeper (arg-or-default :onyx.peer/storage peer-config))
    log
    (checkpoint/storage peer-config nil))) ;; TODO: monitoring component?

(defn gc-checkpoints [{:keys [log peer-config] :as onyx-client} job-id coordinates]
  (let [tenancy-id (:prefix log)
        max-rv (:replica-version coordinates)
        max-epoch (:epoch coordinates)
        targets (build-checkpoint-targets log job-id max-rv max-epoch)
        storage (storage-connection peer-config log)
        gc-f (partial checkpoint/gc-checkpoint! storage tenancy-id job-id)]
    (reduce
     (fn [result {:keys [replica-version epoch-range task-data]}]
       (doseq [epoch epoch-range]
         (doseq [[p-type task-id->slots] task-data]
           (doseq [[task-id slots] task-id->slots]
             (doseq [slot (range (inc slots))]
               (gc-f replica-version epoch task-id slot p-type)))))
       (checkpoint/gc-replica-epoch-watermark! storage tenancy-id job-id replica-version))
     {:checkpoints-deleted 0}
     targets)))
