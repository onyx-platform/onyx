(ns onyx.checkpoint
  "Onyx checkpoint interfaces.")

(defmulti storage :onyx.peer/storage)

; State storage interfaces
(defmulti write-checkpoint
  (fn [storage tenancy-id job-id replica-version epoch task-id slot-id checkpoint-type checkpoint]
    (type storage)))

(defmulti cancel!
  (fn [storage]
    (type storage)))

(defmulti stop
  (fn [storage]
    (type storage)))

(defmulti complete?
  (fn [storage]
    (type storage)))

(defmulti read-checkpoint
  (fn [storage tenancy-id job-id replica-version epoch task-id slot-id checkpoint-type]
    (type storage)))

; Consistent coordinate write interfaces
(defmulti write-checkpoint-coordinate
  (fn [storage tenancy-id job-id coordinate version]
    (type storage)))

(defmulti watch-checkpoint-coordinate
  (fn [storage tenancy-id job-id watcher]
    (type storage)))

(defmulti read-checkpoint-coordinate
  (fn [storage tenancy-id job-id]
    (type storage)))

(defmulti assume-checkpoint-coordinate
  (fn [storage tenancy-id job-id]
    (type storage)))

