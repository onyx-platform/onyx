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

;; It is possible that a crash will cause the coordinates to not be written,
;; therefore we attempt to delete past the final checkpoint in cases where
;; we know this is not the final replica-version.
;; It is safe to do so, as we know there is a next replica-version with a successful checkpoint.
(def attempt-deletion-past-successful-checkpoint-count 5)

(defn build-checkpoint-targets [log tenancy-id job-id delete-all?]
  (let [watermarks (checkpoint/read-all-replica-epoch-watermarks log tenancy-id job-id)
        sorted-watermarks (sort-by :replica-version watermarks)]
    (if (empty? sorted-watermarks)
      []
      (let [max-rv (apply max (map :replica-version watermarks))] 
        (reduce
         (fn [all {:keys [replica-version epoch task-data]}]
           (cond (< replica-version max-rv)
                 (conj all {:replica-version replica-version
                            :delete? true
                            :epoch-range (range 1 (+ (inc epoch) attempt-deletion-past-successful-checkpoint-count))
                            :task-data task-data})

                 (= replica-version max-rv)
                 (conj all {:replica-version replica-version
                            :delete? delete-all?
                            :epoch-range (range 1 epoch)
                            :task-data task-data})

                 :else all))
         []
         sorted-watermarks)))))

(defn storage-connection [peer-config log]
  (if (= :zookeeper (arg-or-default :onyx.peer/storage peer-config))
    log
    (checkpoint/storage peer-config nil))) ;; TODO: monitoring component?

(defn gc-checkpoints [{:keys [log peer-config] :as onyx-client} tenancy-id job-id delete-all?]
  (let [targets (build-checkpoint-targets log tenancy-id job-id delete-all?)
        storage (storage-connection peer-config log)]
    (info "Garbage collecting tenancy-id:" tenancy-id "job-id:" job-id "targets:" targets)
    (reduce
     (fn [result {:keys [replica-version epoch-range task-data delete?]}]
       (let [rets
             (reduce
              (fn [result epoch]
                (reduce-kv
                 (fn [result cp-type task-id->slots]
                   (reduce-kv
                    (fn [result task-id slots]
                      (reduce
                       (fn [result slot]
                         (checkpoint/gc-checkpoint! storage tenancy-id job-id
                                                    replica-version epoch task-id 
                                                    slot cp-type)
                         (update result :checkpoints-deleted inc))
                       result
                       (range (inc slots))))
                    result
                    task-id->slots))
                 result
                 task-data))
              result
              epoch-range)]
         (when delete?
           (checkpoint/gc-replica-epoch-watermark! log tenancy-id job-id replica-version))
         rets))
     {:checkpoints-deleted 0}
     targets)))
