(ns onyx.peer.virtual-peer
  (:require [clojure.core.async :refer [chan alts!! >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.extensions :as extensions]
            [onyx.peer.task-pipeline :refer [task-pipeline]]))

(defn payload-loop [sync payload-ch shutdown-ch status-ch]
  (loop [pipeline nil]
    (when-let [[v ch] (alts!! [payload-ch shutdown-ch])]
      
      (when-not (nil? pipeline)
        (component/stop pipeline))
      
      (when (= ch payload-ch)
        (let [payload-node (:path v)
              payload (extensions/read-place sync payload-node)
              status-ch (chan 1)]
          
          (clojure.pprint/pprint payload)
          
          (extensions/on-change sync (:status (:nodes payload)) #(>!! status-ch %))
          (extensions/touch-place (:ack (:nodes payload)))
          (<!! status-ch)
          
          (let [new-pipeline (task-pipeline payload)]
            (recur new-pipeline)))))))

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
        :peer-node peer
        :payload-node payload
        :pulse-node pulse
        :shutdown-ndoe shutdown
        
        :payload-ch payload-ch
        :shutdown-ch shutdown-ch
        :status-ch status-ch

        :payload-thread (future (payload-loop sync payload-ch shutdown-ch status-ch)))))

  (stop [component]
    (prn "Stopping Task Pipeline")

    (close! (:payload-ch component))
    (close! (:shutdown-ch component))
    (close! (:status-ch component))

    (future-cancel (:payload-thread component))
    
    component))

(defn virtual-peer []
  (map->VirtualPeer {}))

