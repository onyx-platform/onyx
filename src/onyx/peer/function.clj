(ns ^:no-doc onyx.peer.function
    (:require [clojure.core.async :refer [chan >! go alts!! close! timeout]]
              [onyx.static.planning :refer [find-task]]
              [onyx.messaging.acking-daemon :as acker]
              [onyx.peer.task-lifecycle-extensions :as l-ext]
              [onyx.peer.pipeline-extensions :as p-ext]
              [onyx.peer.operation :as operation]
              [onyx.extensions :as extensions]
              [taoensso.timbre :as timbre :refer [debug info]]
              [dire.core :refer [with-post-hook!]])
    (:import [java.util UUID]))

(defmethod l-ext/start-lifecycle? :function
  [_ event]
  {:onyx.core/start-lifecycle? (operation/start-lifecycle? event)})

(defmethod l-ext/inject-lifecycle-resources :function
  [_ {:keys [onyx.core/task-map]}]
  {:onyx.function/fn (operation/resolve-fn task-map)})

(defmethod p-ext/read-batch :default
  [{:keys [onyx.core/messenger] :as event}]
  {:onyx.core/batch (onyx.extensions/receive-messages messenger event)})

(defn apply-fn
  [{:keys [onyx.core/params] :as event} segment]
  (if-let [f (:onyx.function/fn event)]
    (operation/apply-function f params segment)
    segment))

(defn filter-by-route [messages task-name]
  (->> messages
       (filter (fn [msg] (some #{task-name} (:flow (:routes msg)))))
       (map #(dissoc % :routes :hash-group))))

(defn build-segments-to-send [leaves]
  (reduce
   (fn [all {:keys [routes ack-vals hash-group message] :as leaf}]
     (into
      all
      (map
       (fn [route ack-val]
         {:id (:id leaf)
          :acker-id (:acker-id leaf)
          :completion-id (:completion-id leaf)
          :message (:message leaf)
          :ack-val ack-val
          :route route
          :hash-group (get hash-group route)})
       (:flow routes) ack-vals)))
   []
   (remove (fn [leaf] (= :retry (:action (:routes leaf)))) leaves)))

(defn strip-message [segment]
  (select-keys segment [:id :acker-id :completion-id :ack-val :message]))

(defn pick-peer [active-peers hash-group]
  (when (seq active-peers)
    (if (nil? hash-group)
      (rand-nth active-peers)
      (nth active-peers
           (mod (hash hash-group)
                (count active-peers))))))

(defmethod p-ext/write-batch :default
  [{:keys [onyx.core/results onyx.core/messenger onyx.core/job-id] :as event}]
  (let [leaves (mapcat :leaves results)
        egress-tasks (:egress-ids (:onyx.core/serialized-task event))]
    (when (seq leaves)
      (let [replica @(:onyx.core/replica event)
            segments (build-segments-to-send leaves)
            groups (group-by #(select-keys % [:route :hash-group]) segments)]
        (doseq [[{:keys [route hash-group]} segs] groups]
          (let [peers (get-in replica [:allocations job-id (get egress-tasks route)])
                active-peers (filter #(= (get-in replica [:peer-state %]) :active) peers)
                target (pick-peer active-peers hash-group)]
            (when target
              (let [link (operation/peer-link event target)]
                (onyx.extensions/send-messages messenger event link (map strip-message segs))))))
        {}))))
