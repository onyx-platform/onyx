(ns onyx.query
  (:require [onyx.static.default-vals :refer [arg-or-default]]
            [ring.component.jetty :refer [jetty-server]]
            [ring.middleware.params :refer [wrap-params]]
            [com.stuartsierra.component :as component]
            [cheshire.core :refer [generate-string]]
            [taoensso.timbre :refer [info infof]]))

(def default-serializer "application/edn")

(def parse-uuid #(java.util.UUID/fromString %))

(def endpoints
  {{:uri "/replica"
    :request-method :get}
   {:doc "Returns a snapshot of the replica"
    :f (fn [request replica] replica)}

   {:uri "/replica/peers"
    :request-method :get}
   {:doc "Lists all the peer ids"
    :f (fn [request replica]
         (:peers replica))}

   {:uri "/replica/jobs"
    :request-method :get}
   {:doc "Lists all non-killed, non-completed job ids."
    :f (fn [request replica]
         (:jobs replica))}

   {:uri "/replica/killed-jobs"
    :request-method :get}
   {:doc "Lists all the job ids that have been killed."
    :f (fn [request replica]
         (:killed-jobs replica))}

   {:uri "/replica/completed-jobs"
    :request-method :get}
   {:doc "Lists all the job ids that have been completed."
    :f (fn [request replica]
         (:completed-jobs replica))}

   {:uri "/replica/tasks"
    :request-method :get}
   {:doc "Given a job id, returns all the task ids for this job."
    :query-params-schema {"job-id" String}
    :f (fn [request replica]
         (let [job-id (parse-uuid (get-in request [:query-params "job-id"]))]
           (get-in replicate [:tasks job-id])))}

   {:uri "/replica/job-allocations"
    :request-method :get}
   {:doc "Returns a map of job id -> task id -> peer ids, denoting which peers are assigned to which tasks."
    :f (fn [request replica]
         (:allocations replica))}

   {:uri "/replica/task-allocations"
    :request-method :get}
   {:doc "Given a job id, returns a map of task id -> peer ids, denoting which peers are assigned to which tasks for this job only."
    :f (fn [request replica]
         (let [job-id (parse-uuid (get-in request [:query-params "job-id"]))]
           (get-in replicate [:allocations job-id])))}

   {:uri "/replica/peer-site"
    :request-method :get}
   {:doc "Given a peer id, returns the Aeron hostname and port that this peer advertises to the rest of the cluster."
    :query-params-schema {"peer-id" String}
    :f (fn [request replica]
         (let [peer-id (parse-uuid (get-in request [:query-params "peer-id"]))]
           (get-in replica [:peer-sites peer-id])))}

   {:uri "/replica/peer-state"
    :request-method :get}
   {:doc "Given a peer id, returns its current execution state (e.g. :idle, :active, etc)."
    :query-params-schema {"peer-id" String}
    :f (fn [request replica]
         (let [peer-id (parse-uuid (get-in request [:query-params "peer-id"]))]
           (get-in replica [:peer-state peer-id])))}

   {:uri "/replica/job-scheduler"
    :request-method :get}
   {:doc "Returns the job scheduler for this tenancy of the cluster."
    :f (fn [request replica]
         (:job-scheduler replica))}

   {:uri "/replica/task-scheduler"
    :request-method :get}
   {:doc "Given a job id, returns the task scheduler for this job."
    :query-params-schema
    {"job-id" String}
    :f (fn [request replica]
         (let [job-id (parse-uuid (get-in request [:query-params "job-id"]))]
           (get-in replica [:task-schedulers job-id])))}})


(def serializers
  {"application/edn" pr-str
   "application/json" generate-string})

(defn ^{:no-doc true} serializer-name
  [content-type]
  (if (serializers content-type)
    content-type
    default-serializer))

(defn ^{:no-doc true} get-serializer
  [content-type]
  (get serializers content-type
       (get serializers default-serializer)))

(defn handler [replica {:keys [content-type] :as request}]
  (let [serialize (get-serializer content-type)
        f (:f (get endpoints (select-keys request [:request-method :uri])))]
    (if-not f
      {:status 404
       :headers {"Content-Type" (serializer-name content-type)}
       :body (serialize {:status :failed :message "Endpoint not found."})}
      (let [result (f request @replica)]
        {:status 200
         :headers {"Content-Type" (serializer-name content-type)}
         :body (serialize {:status :success
                           :result result})}))))

(defn app [replica]
  {:handler (wrap-params (partial handler replica))})

(defrecord QueryServer [replica server peer-config]
  component/Lifecycle
  (start [this]
    (let [ip (arg-or-default :onyx.query.server/ip peer-config )
          port (arg-or-default :onyx.query.server/port peer-config)
          replica (atom {})
          server-component (jetty-server {:app (app replica) :host ip :port port})]
      (infof "Starting http query server on %s:%s" ip port)
      (assoc this 
             :replica replica
             :server (component/start server-component))))
  (stop [this]
    (info "Stopping http query server")
    (assoc this 
           :replica nil
           :server (component/stop server)
           :loggin-config nil
           :peer-config nil)))

(defrecord DummyServer [replica loggin-config peer-config]
  component/Lifecycle
  (start [this]
    (assoc this :replica (atom nil)))
  (stop [this]
    (assoc this :replica nil)))

(defn query-server [peer-config]
  (if (:onyx.query/server? peer-config)
    (map->QueryServer {:peer-config peer-config})
    (map->DummyServer {})))
