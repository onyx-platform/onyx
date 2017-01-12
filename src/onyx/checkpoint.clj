(ns onyx.checkpoint
  "Onyx checkpoint interfaces.")

(defmulti storage :onyx.peer/storage)

; State storage interfaces
(defmulti write-checkpoint 
  (fn [log tenancy-id job-id replica-version epoch task-id slot-id checkpoint-type checkpoint]
    (type log)))

(defmulti read-checkpoint 
  (fn [log tenancy-id job-id replica-version epoch task-id slot-id checkpoint-type] 
    (type log)))

; Consistent coordinate write interfaces
(defmulti write-checkpoint-coordinate 
  (fn [log tenancy-id job-id coordinate version]
    (type log)))

(defmulti read-checkpoint-coordinate 
  (fn [log tenancy-id job-id] 
    (type log)))

(defmulti assume-checkpoint-coordinate
  (fn [log tenancy-id job-id] 
    (type log)))

