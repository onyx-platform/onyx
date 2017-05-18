(ns ^{:no-doc true} onyx.messaging.serializers.helpers)

;; These functions are a hacky workaround for the fact that coordinator peer-ids take the form:
;; [:coordinator #uuid ...]
;; and peer peer-ids takes the form:
;; #uuid ...
;; A future refactor will ensure they both take the vector form

(def coordinator (byte 0))
(def peer (byte 1))

(def byte->type
  {coordinator :coordinator
   peer :task})

(def type->byte
  {:coordinator coordinator
   :task peer})

(defn coerce-peer-id [peer-id]
  (if (vector? peer-id) peer-id
    [:task peer-id]))

(defn uncoerce-peer-id [peer-id]
  (if (= :task (first peer-id))
    (second peer-id)
    peer-id))


