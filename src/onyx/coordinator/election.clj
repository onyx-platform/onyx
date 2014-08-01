(ns ^:no-doc onyx.coordinator.election
    (:require [clojure.core.async :refer [chan >!! <!!]]
              [com.stuartsierra.component :as component]
              [taoensso.timbre :refer [info]]
              [onyx.extensions :as extensions]))

(defn block-until-leader! [sync content]
  (try
    (prn "Coordinator announced")
    (let [node (:node (extensions/create sync :election content))]
      (prn "Node is " node)
      (loop []
        (let [ch (chan)
              predecessor (extensions/previous-node sync :election node)]
          (prn "Pred is: " predecessor)
          (if predecessor
            (do
              (extensions/on-delete sync predecessor #(>!! ch (:path %)))

              (prn node " is blocking")
              (<!! ch)
              (prn node " is unblocking")
              (let [children (extensions/bucket sync :election)]
                (let [s? (extensions/smallest? sync :election node)]
                  (prn "Am I the leader? " node "::" s?)
                  (when-not s?
                    (recur)))))
            (prn "Not recurring")))))
    (catch Exception e
      (.printStackTrace e))))

(defrecord Election [opts]
  component/Lifecycle
  (start [component]
    (prn "Election")
    (taoensso.timbre/info "Starting Election process")

    (block-until-leader!
     (:sync component)
     {:host (:onyx.coordinator/host opts)
      :port (:onyx.coordinator/port opts)})

    (prn "H:" (:onyx.coordinator/host opts))
    (prn "P:" (:onyx.coordinator/port opts))

    (taoensso.timbre/info "Done with Election process")
    component)
  
  (stop [component]
    (taoensso.timbre/info "Stopping Election process")
    component))

(defn election [opts]
  (map->Election {:opts opts}))

