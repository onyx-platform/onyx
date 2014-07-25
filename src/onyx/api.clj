(ns onyx.api
  (:require [clojure.string :refer [split]]
            [clojure.core.async :refer [chan alts!! >!! <!!]]
            [com.stuartsierra.component :as component]
            [clj-http.client :refer [post]]
            [taoensso.timbre :refer [warn]]
            [onyx.coordinator.impl :as impl]
            [onyx.system :as system]
            [onyx.extensions :as extensions]))

(defprotocol ISubmit
  "Protocol for sending a job to the coordinator for execution."
  (submit-job [this job]))

(defprotocol IRegister
  "Protocol for registering a virtual peer with the coordinator.
   Registering allows the virtual peer to accept tasks."
  (register-peer [this peer-node]))

(defprotocol IAwait
  "Protocol for waiting for completion of Onyx internals"
  (await-job-completion [this job-id]))

(defprotocol IShutdown
  "Protocol for stopping a virtual peer's task and no longer allowing
   it to accept new tasks. Releases all resources that were previously
   acquired."
  (shutdown [this]))

(defn await-job-completion* [sync job-id]
  (future
    (loop []
      (let [ch (chan 1)
            task-path (extensions/resolve-node sync :task job-id)]
        (extensions/on-child-change sync task-path (fn [_] (>!! ch true)))
        (let [task-nodes (extensions/bucket-at sync :task job-id)
              task-nodes (filter #(not (impl/completed-task? %)) task-nodes)]
          (if (every? (partial impl/task-complete? sync) task-nodes)
            true
            (do (<!! ch)
                (recur))))))))

;; A coordinator that runs strictly in memory. Peers communicate with
;; the coordinator by directly accessing its channels.
(deftype InMemoryCoordinator [onyx-coord]
  ISubmit
  (submit-job [this job]
    (let [ch (chan 1)
          node (extensions/create (:sync onyx-coord) :plan)
          cb #(>!! ch (extensions/read-node (:sync onyx-coord) (:path %)))]
      (extensions/on-change (:sync onyx-coord) (:node node) cb)
      (extensions/create (:sync onyx-coord) :planning-log {:job job :node (:node node)})
      (>!! (:planning-ch-head (:coordinator onyx-coord)) true)
      (<!! ch)))

  IAwait
  (await-job-completion [this job-id]
    (await-job-completion* (:sync onyx-coord) job-id))

  IRegister
  (register-peer [this peer-node]
    (extensions/create (:sync onyx-coord) :born-log peer-node)
    (>!! (:born-peer-ch-head (:coordinator onyx-coord)) true))

  IShutdown
  (shutdown [this] (component/stop onyx-coord)))

;; A coordinator runs remotely. Peers communicate with the
;; coordinator by submitting HTTP requests and parsing the responses.
(deftype HttpCoordinator [conn]
  ISubmit
  (submit-job [this job]
    (let [leader (extensions/leader (:sync conn) :election)
          data (extensions/read-node (:sync conn) leader)
          uri (format "http://%s:%s/submit-job" (:host data) (:port data))
          response (post uri {:body (pr-str job)})]
      (:job-id (read-string (:body response)))))

  IRegister
  (register-peer [this peer-node]
    (let [leader (extensions/leader (:sync conn) :election)
          data (extensions/read-node (:sync conn) leader)
          uri (format "http://%s:%s/register-peer" (:host data) (:port data))
          response (post uri {:body (pr-str peer-node)})]
      (read-string (:body response))))

  IShutdown
  (shutdown [this]))

(defmulti connect
  "Establish a communication channel with the coordinator.
   kw can be :memory or :distributed."
  (fn [kw opts] kw))

(defmethod connect :memory
  [kw opts]
  (let [c (system/onyx-coordinator opts)]
    (InMemoryCoordinator. (component/start c))))

(defmethod connect :distributed
  [kw opts]
  (let [c (system/onyx-coordinator-connection opts)]
    (HttpCoordinator. c)))

(defn start-peers
  "Launches n virtual peers. Starts a payload thread for each vpeer.
   Takes a coordinator type to connect to and a virtual peer configuration.
   Each peer may be stopped by invoking the fn returned by :shutdown-fn."
  [coord n config]
  (doall
   (map
    (fn [_]
      (let [stop-ch (chan)
            v-peer (system/onyx-peer config)]
        {:runner
         (future
           (loop []
             (let [rets
                   (try
                     (let [live (component/start v-peer)]
                       (register-peer coord (:node (:peer (:peer live))))

                       (let [restart-ch (:dead-restart-tail-ch (:peer live))
                             [v ch] (alts!! [stop-ch restart-ch] :priority true)]
                         (if (= ch stop-ch)
                           (try
                             (component/stop live)
                             :stopped
                             (catch Exception e
                               (warn e)
                               :stopped))
                           (try
                             (component/stop live)
                             nil
                             (catch Exception e
                               (warn e)
                               :stopped)))))
                     (catch Exception e
                       (warn e)
                       (warn "Virtual peer failed, backing off and rebooting...")
                       (Thread/sleep (or (:onyx.peer/retry-start-interval config) 2000))
                       nil))]
               (or rets (recur)))))
         :shutdown-fn (fn [] (>!! stop-ch true))}))
    (range n))))

