(ns onyx.state.protocol.db)

(defprotocol State
  (put-extent! [this window-id group extent v])
  (put-extent-entry! [this window-id group extent entry])
  (get-extent-entries [this window-id group extent])
  (get-extent [this window-id group extent])
  (delete-extent! [this window-id group extent])
  (put-trigger! [this trigger-id group v])
  (group-id [this group-key])
  (group-key [this group-id])
  (get-trigger [this trigger-id group])
  (groups [this window-id])
  (group-extents [this window-id group])
  (trigger-keys [this trigger-id])
  (drop! [this])
  (close! [this])
  (export [this encoder])
  (restore! [this decoder mapping])
  (export-reader [this]))

(defmulti create-db 
  (fn [peer-config db-name serializers]
    (or (:onyx.peer/state-store-impl peer-config) :memory)))

(defmulti open-db-reader 
  (fn [peer-config definition serializers]
    (or (:onyx.peer/state-store-impl peer-config) :memory)))
