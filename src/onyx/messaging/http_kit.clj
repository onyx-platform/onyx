(ns ^:no-doc onyx.messaging.http-kit
    (:require [clojure.core.async :refer [>!!]]
              [com.stuartsierra.component :as component]
              [org.httpkit.server :as server]
              [org.httpkit.client :as client]
              [taoensso.timbre :as timbre]
              [onyx.planning :refer [find-downstream-tasks]]
              [onyx.extensions :as extensions])
    (:import [java.nio ByteBuffer]))

(defn app [inbound-ch request]
  (>!! inbound-ch (:body request))
  {:status 200
   :headers {"Content-Type" "text/html"}
   :body ""})

(defrecord HttpKit [opts]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting HTTP Kit")

    (let [ch (:inbound-ch (:messenger-buffer component))
          ip "0.0.0.0"
          server (server/run-server (partial app ch) {:ip ip :port 0 :thread 1})]
      (assoc component :server server :ip ip :port (:local-port (meta server)))))

  (stop [component]
    (taoensso.timbre/info "Stopping HTTP Kit")

    ((:server component))
    component))

(defn http-kit [opts]
  (map->HttpKit {:opts opts}))

(defmethod extensions/peer-site HttpKit
  [messenger]
  {:url (format "%s:%s" (:ip messenger) (:port messenger))})

(defmethod extensions/receive-messages HttpKit
  [messenger event])

(defmethod extensions/send-messages HttpKit
  [messenger {:keys [onyx.core/job-id onyx.core/task onyx.core/workflow] :as event}]
  (let [replica @(:onyx.core/replica event)
        tasks (find-downstream-tasks workflow task)]
    (doseq [task tasks]
      (let [peers (get-in [replica :allocations job-id task-id])
            url (get-in replica [:peer-site (rand-nth peers)])]
        (doseq [c (map :compressed (:onyx.core/compressed event))]
          (client/post url {:body (ByteBuffer/wrap c)}))))
    {}))

