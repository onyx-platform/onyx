(ns ^:no-doc onyx.coordinator.distributed
  (:require [clojure.core.async :refer [>!! <!! chan]]
            [com.stuartsierra.component :as component]          
            [taoensso.timbre :as timbre]
            [ring.adapter.jetty :as jetty]
            [onyx.extensions :as extensions]
            [onyx.coordinator.election :as election]
            [onyx.system :refer [onyx-coordinator]]))

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
    (>!! (:planning-ch-head (:coordinator coordinator)) true)
    (<!! ch)))

(defmethod dispatch-request "/register-peer"
  [coordinator request]
  (let [data (read-string (slurp (:body request)))]
    (extensions/create (:sync coordinator) :born-log data)
    (>!! (:born-peer-ch-head (:coordinator coordinator)) true)
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
    (let [coordinator (component/start (onyx-coordinator opts))]
      (taoensso.timbre/info "Starting Coordinator Netty server")
      
      #_(election/block-until-leader!
       (:sync coordinator)
       {:host (:onyx.coordinator/host opts)
        :port (:onyx.coordinator/port opts)})

      (assoc component
        :coordinator coordinator
        :server (jetty/run-jetty (handler coordinator) {:port (:onyx.coordinator/port opts)
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

