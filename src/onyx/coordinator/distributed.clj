(ns ^:no-doc onyx.coordinator.distributed
  (:require [clojure.core.async :refer [>!! <!! chan]]
            [com.stuartsierra.component :as component]          
            [taoensso.timbre :as timbre]
            [ring.adapter.jetty :as jetty]
            [onyx.system :refer [onyx-coordinator]]))

(defmulti dispatch-request
  (fn [coordinator request] (:uri request)))

(defmethod dispatch-request "/submit-job"
  [coordinator request]
  (let [data (read-string (slurp (:body request)))
        ch (chan 1)]
    (>!! (:planning-ch-head (:coordinator coordinator)) [data ch])
    (<!! ch)))

(defmethod dispatch-request "/register-peer"
  [coordinator request]
  (let [data (read-string (slurp (:body request)))]
    (>!! (:born-peer-ch-head (:coordinator coordinator)) data)))

(defn handler [coordinator]
  (fn [request]
    (dispatch-request coordinator request)
    {:status 200
     :headers {"content-type" "text/text"}
     :body "ok"}))

(defrecord CoordinatorServer [opts]
  component/Lifecycle
  (start [component]
    (let [coordinator (component/start (onyx-coordinator opts))]
      (taoensso.timbre/info "Starting Coordinator Netty server")
      (assoc component
        :coordinator coordinator
        :server (jetty/run-jetty (handler coordinator) {:port (:onyx-port opts)
                                                        :join? false}))))
  
  (stop [component]
    (taoensso.timbre/info "Stopping Coordinator Netty server")

    (component/stop (:coordinator component))
    (.stop (:server component))

    component))

(defn coordinator-server [opts]
  (map->CoordinatorServer {:opts opts}))

(defn start-distributed-coordinator [opts]
  (component/start (coordinator-server opts)))

(defn stop-distributed-coordinator [coordinator]
  (component/stop coordinator))

