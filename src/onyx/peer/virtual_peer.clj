(ns onyx.peer.virtual-peer
  (:require [clojure.core.async :refer [chan alts!! >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.extensions :as extensions]
            [onyx.peer.task-pipeline :refer [task-pipeline]]))

(defn payload-loop [payload-ch shutdown-ch status-ch]
  (loop [pipeline nil]
    (when-let [[v ch] (alts!! [payload-ch shutdown-ch])]
      (when-not (nil? pipeline)
        (component/stop pipeline))
      (when (= ch payload-ch)
        (comment "Block on status-ch")
        (let [new-pipeline (task-pipeline v)]
          (recur new-pipeline))))))

(defrecord VirtualPeer []
  component/Lifecycle

  (start [{:keys [sync queue] :as component}]
    (prn "Starting Task Pipeline")

    (let [peer (extensions/create sync :peer)
          payload (extensions/create sync :payload)
          pulse (extensions/create sync :pulse)
          shutdown (extensions/create sync :shutdown)

          payload-ch (chan 1)
          shutdown-ch (chan 1)
          status-ch (chan 1)]
      
      (extensions/write-place sync peer {:pulse pulse :shutdown shutdown :payload payload})
      (extensions/on-change sync payload #(>!! payload-ch %))

      (assoc component
        :payload-ch payload-ch
        :shutdown-ch shutdown-ch
        :status-ch status-ch

        :payload-thread (future (payload-loop payload-ch shutdown-ch status-ch)))))

  (stop [component]
    (prn "Stopping Task Pipeline")

    (close! (:payload-ch component))
    (close! (:shutdown-ch component))
    (close! (:status-ch component))

    (future-cancel (:payload-thread component))
    
    component))

(defn virtual-peer []
  (map->VirtualPeer {}))

