(ns ^:no-doc onyx.coordinator.election
    (:require [clojure.core.async :refer [chan >!! <!!]]
              [com.stuartsierra.component :as component]
              [taoensso.timbre :refer [info]]
              [onyx.extensions :as extensions]))

(defn block-until-leader! [sync content]
  (let [node (:node (extensions/create sync :election content))]
    (loop []
      (let [ch (chan)
            predecessor (extensions/previous-node sync node)]
        (when predecessor
          (extensions/on-delete sync predecessor #(>!! ch (:path %)))
          (<!! ch)
          (let [children (extensions/bucket sync :election)]
            (let [s? (extensions/smallest? sync :election node)]
              (when-not s?
                (recur)))))))))

(defrecord Election [opts]
  component/Lifecycle
  (start [component]
    (taoensso.timbre/info "Starting Election process")

    (block-until-leader!
     (:sync component)
     {:host (:onyx.coordinator/host opts)
      :port (:onyx.coordinator/port opts)})
    component)
  
  (stop [component]
    (taoensso.timbre/info "Stopping Election process")
    component))

(defn election [opts]
  (map->Election {:opts opts}))

