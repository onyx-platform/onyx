(ns ^:no-doc onyx.coordinator.distributed
  (:require [clojure.core.async :refer [>!! <!! chan]]
            [com.stuartsierra.component :as component]          
            [taoensso.timbre :as timbre]
            [ring.adapter.jetty :as jetty]
            [onyx.extensions :as extensions]))

(defmulti dispatch-request
  (fn [coordinator request] (:uri request)))

(defmethod dispatch-request "/submit-job"
  [coordinator request]
  (let [job (read-string (slurp (:body request)))
        ch (chan 1)
        node (extensions/create (:sync coordinator) :plan)
        cb #(>!! ch (extensions/read-node (:sync coordinator) (:path %)))]
    (extensions/on-change (:sync coordinator) (:node node) cb)
    (extensions/create (:sync coordinator) :planning-log {:job job :node (:node node)})
    (>!! (:planning-ch-head coordinator) true)
    (<!! ch)))

(defmethod dispatch-request "/register-peer"
  [coordinator request]
  (let [data (read-string (slurp (:body request)))]
    (extensions/create (:sync coordinator) :born-log data)
    (>!! (:born-peer-ch-head coordinator) true)
    :ok))

(defn handler [coordinator]
  (fn [request]
    (let [resp (dispatch-request coordinator request)]
      {:status 200
       :headers {"content-type" "text/text"}
       :body (pr-str resp)})))

(defrecord CoordinatorServer [opts]
  component/Lifecycle
  (start [component]
    (taoensso.timbre/info "Starting Coordinator Netty server")

    (assoc component
      :server (jetty/run-jetty
               (handler (:coordinator component))
               {:port (:onyx.coordinator/port opts)
                :join? false})))
  
  (stop [component]
    (taoensso.timbre/info "Stopping Coordinator Netty server")
    (.stop (:server component))
    component))

(defn coordinator-server [opts]
  (map->CoordinatorServer {:opts opts}))

