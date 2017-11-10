(ns onyx.gc
  (:require [clojure.core.async :refer [chan <!!]]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.extensions :as extensions]
            [onyx.checkpoint :as checkpoint]
            [onyx.static.default-vals :refer [arg-or-default]]
            [taoensso.timbre :refer [info]]))

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
    (info "Garbage collecting across targets: " targets)
    (reduce
     (fn [result {:keys [replica-version epoch-range task-data]}]
       (let [rets
             (reduce
              (fn [result epoch]
                (reduce-kv
                 (fn [result p-type task-id->slots]
                   (reduce-kv
                    (fn [result task-id slots]
                      (reduce
                       (fn [result slot]
                         (gc-f replica-version epoch task-id slot p-type)
                         (update result :checkpoints-deleted inc))
                       result
                       (range (inc slots))))
                    result
                    task-id->slots))
                 result
                 task-data))
              result
              epoch-range)]
         (checkpoint/gc-replica-epoch-watermark! log tenancy-id job-id replica-version)
         rets))
     {:checkpoints-deleted 0}
     targets)))
