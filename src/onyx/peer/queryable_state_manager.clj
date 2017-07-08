(ns onyx.peer.queryable-state-manager
  (:require [clojure.core.async :refer [chan close! poll! >!!]]
            [onyx.state.serializers.utils]
            [com.stuartsierra.component :as component]
            [onyx.peer.window-state :as ws]
            [onyx.state.protocol.db :as db]
            [taoensso.timbre :refer [fatal info]])
  (:import [java.util.concurrent.locks LockSupport]))

;; COMMANDS
;; need a way to read only open
;; need a way to to close it
;; peer group replica maybe would do it...

;; QUERY
;; need a way to query it.
;; 

;; put by job-id, task-id, slot-id, replica-version READ ONLY PARAM DEFINITIONS
;; will need window serializers too, to get extents, will need brand new ones too.

(defn processing-loop [peer-config shutdown state ch]
  (loop []
    (when-not @shutdown
      (if-let [cmd (poll! ch)]
        (do
         (println "COINLG" cmd)
         (case (first cmd)
          :created-db
          (let [[_ replica-version event exported] cmd
                serializers (onyx.state.serializers.utils/event->state-serializers event)]
            (swap! state 
                   assoc 
                   [(:onyx.core/job-id event)
                    replica-version
                    (:onyx.core/task event)
                    (:onyx.core/slot-id event)] 
                   {:state-indexes (ws/state-indexes event)
                    :db (db/open-db-reader peer-config exported serializers)})
            (println "CMD" @state))))
        (do
         (let [v (first (vals @state))]
           (doseq [idx (vals (:state-indexes v))]
             (doseq [g (db/groups (:db v) idx)]
               ;; TODO, need window extensions in here
               (do
                (println "IDX" idx)
                (println "GRPS" g)
                (println "EXTENTS" (db/group-extents (:db v) idx g))))))
         (LockSupport/parkNanos (* 10 1000000))))
      (recur))))

(defrecord OnyxStateStoreGroup [peer-config ch state shutdown]
  component/Lifecycle
  (start [this]
    (let [shutdown (atom false)
          state (atom {})
          ch (chan 1000)
          fut (future (processing-loop peer-config shutdown state ch))]
      (assoc this 
             :fut fut
             :shutdown shutdown
             :ch ch 
             :state state)))
  (stop [this]
    (close! ch)
    (reset! shutdown true)
    (future-cancel (:fut this))
    (assoc this :ch nil :state nil :state nil :fut nil)))

(defn new-state-store-group [peer-config]
  (map->OnyxStateStoreGroup {:peer-config peer-config}))
