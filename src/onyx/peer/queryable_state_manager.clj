(ns onyx.peer.queryable-state-manager
  (:require [clojure.core.async :refer [chan close! poll! >!!]]
            [onyx.state.serializers.utils]
            [com.stuartsierra.component :as component]
            [onyx.peer.window-state :as ws]
            [onyx.state.protocol.db :as db]
            [onyx.peer.grouping :as g]
            [taoensso.timbre :refer [fatal info]])
  (:import [java.util.concurrent.locks LockSupport]))

(def required-event-keys
  [:onyx.core/job-id :onyx.core/task 
   :onyx.core/slot-id :onyx.core/task-map 
   :onyx.core/windows :onyx.core/triggers])

(defn state-key [replica-version event]
  [(:onyx.core/job-id event)
   (:onyx.core/task event)
   (:onyx.core/slot-id event)
   replica-version])

(defmulti process-store 
  (fn [[cmd] _ _]
    cmd)) 

; (defn drop-prior-db [st event]
;   (update-in st
;              [(:onyx.core/job-id event)
;               (:onyx.core/task event)]
;              remove-db
;              (:onyx.core/slot-id event)))

(defn remove-db [state k-rem]
  (swap! state 
         (fn [m]
           (->> m
                (remove (fn [[k v]]
                          (= k k-rem)))
                (into {})))))

(defn add-new-db [st event replica-version peer-config exported]
  (let [serializers (onyx.state.serializers.utils/event->state-serializers event)]
    (assoc st
           (state-key replica-version event)
           {:state-indices (ws/state-indices event)
            :grouped? (g/grouped-task? (:onyx.core/task-map event))
            :db (db/open-db-reader peer-config exported serializers)})))

(defmethod process-store :created-db
  [[_ replica-version event exported] state peer-config]
  (swap! state add-new-db event replica-version peer-config exported))

(defmethod process-store :drop-job-dbs 
  [[_ deallocated] state peer-config]
  (run! (fn [[job-id replica-version]] 
          (let [[k store] (first (filter (fn [[[j _ _ r] _]]
                                           (and (= job-id j) 
                                                (= replica-version r)))
                                         @state))]
            (remove-db state k)
            (db/close! (:db store)))) 
        deallocated))

(defn processing-loop [peer-config shutdown state ch]
  (loop []
    (when-not @shutdown
      (if-let [cmd (poll! ch)]
        (do
         (process-store cmd state peer-config))
        (LockSupport/parkNanos (* 10 1000000)))
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
