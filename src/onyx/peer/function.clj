(ns ^:no-doc onyx.peer.function
    (:require [clojure.core.async :refer [chan >! go alts!! close! timeout]]
              [onyx.static.planning :refer [find-task]]
              [onyx.messaging.acking-daemon :as acker]
              [onyx.compression.nippy :refer [compress decompress]]
              [onyx.peer.task-lifecycle-extensions :as l-ext]
              [onyx.peer.pipeline-extensions :as p-ext]
              [onyx.peer.operation :as operation]
              [onyx.extensions :as extensions]
              [taoensso.timbre :as timbre :refer [debug]]
              [dire.core :refer [with-post-hook!]])
    (:import [java.util UUID]
             [java.security MessageDigest]))

(defn hash-value [x]
  (let [md5 (MessageDigest/getInstance "MD5")]
    (apply str (.digest md5 (.getBytes (pr-str x) "UTF-8")))))

(defn group-message [segment catalog task]
  (let [t (find-task catalog task)]
    (if-let [k (:onyx/group-by-key t)]
      (hash-value (get segment k))
      (when-let [f (:onyx/group-by-fn t)]
        (hash-value ((operation/resolve-fn {:onyx/fn f}) segment))))))

(defn compress-segments [next-tasks catalog result event]
  (assoc result
    :leaves
    (mapv
     (fn [leaf]
       (let [msg (if (and (operation/exception? (:message leaf))
                          (:post-transformation (:routes leaf)))
                   (operation/apply-fn
                    (operation/kw->fn (:post-transformation (:routes leaf)))
                    [event] (:message leaf))
                   (:message leaf))]
         (assoc leaf
           :hash-group
           (reduce (fn [groups t]
                     (assoc groups t (group-message msg catalog t)))
                   {} next-tasks)
           :message (apply dissoc msg (:exclusions (:routes leaf))))))
     (:leaves result))))

(defmethod l-ext/start-lifecycle? :function
  [_ event]
  {:onyx.core/start-lifecycle? (operation/start-lifecycle? event)})

(defmethod l-ext/inject-lifecycle-resources :function
  [_ {:keys [onyx.core/task-map]}]
  {:onyx.function/fn (operation/resolve-fn task-map)})

(defmethod p-ext/read-batch :default
  [{:keys [onyx.core/messenger] :as event}]
  {:onyx.core/batch (onyx.extensions/receive-messages messenger event)})

(defmethod p-ext/apply-fn :default
  [{:keys [onyx.core/params] :as event} segment]
  (operation/apply-fn (:onyx.function/fn event) params segment))

(defmethod p-ext/compress-batch :default
  [{:keys [onyx.core/results onyx.core/catalog onyx.core/serialized-task]
    :as event}]
  (let [next-tasks (keys (:egress-ids serialized-task))
        compressed-msgs (map #(compress-segments next-tasks catalog % event) results)]
    (merge event {:onyx.core/compressed compressed-msgs})))

(defn filter-by-route [messages task-name]
  (->> messages
       (filter (fn [msg] (some #{task-name} (:flow (:routes msg)))))
       (map #(dissoc % :routes :hash-group))))

(defn build-segments-to-send [leaves]
  (reduce
   (fn [all {:keys [routes ack-vals hash-group message] :as leaf}]
     (concat
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
   leaves))

(defn strip-message [segment]
  (select-keys segment [:id :acker-id :completion-id :ack-val :message]))

(defn pick-peer [active-peers hash-group]
  (if (nil? hash-group)
    (rand-nth active-peers)
    (nth active-peers
         (mod (.hashCode hash-group)
              (count active-peers)))))

(defmethod p-ext/write-batch :default
  [{:keys [onyx.core/messenger onyx.core/job-id] :as event}]
  (let [leaves (mapcat :leaves (:onyx.core/compressed event))
        egress-tasks (:egress-ids (:onyx.core/serialized-task event))]
    (when (seq leaves)
      (let [replica @(:onyx.core/replica event)
            segments (build-segments-to-send leaves)
            groups (group-by #(select-keys % [:route :hash-group]) segments)]
        (doseq [[{:keys [route hash-group]} segs] groups]
          (let [peers (get-in replica [:allocations job-id (get egress-tasks route)])
                active-peers (filter #(= (get-in replica [:peer-state %]) :active) peers)
                target (pick-peer active-peers hash-group)
                link (operation/peer-link event target)]
            (onyx.extensions/send-messages messenger event link (map strip-message segs))))
        {}))))

