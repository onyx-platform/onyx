(ns onyx.peer.virtual-peer
  (:require [clojure.core.async :refer [chan alts!! >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [dire.core :as dire]
            [onyx.extensions :as extensions]
            [onyx.peer.task-pipeline :refer [task-pipeline]]))

(defn payload-loop [sync queue payload-ch shutdown-ch status-ch dead-ch pulse fn-params]
  (let [complete-ch (chan 1)]
    (loop [pipeline nil]
      (when-let [[v ch] (alts!! [shutdown-ch complete-ch payload-ch] :priority true)]
        (when-not (nil? pipeline)
          (component/stop pipeline))

        (cond (nil? v) (extensions/delete sync pulse)
              (= ch complete-ch) (recur nil)
              (= ch payload-ch)
              (let [payload-node (:path v)
                    payload (extensions/read-place sync payload-node)
                    status-ch (chan 1)]

                (extensions/on-change sync (:status (:nodes payload)) #(>!! status-ch %))
                (extensions/touch-place sync (:ack (:nodes payload)))

                (<!! status-ch)

                (let [new-pipeline (task-pipeline payload sync queue payload-ch complete-ch fn-params)]
                  (recur (component/start new-pipeline)))))))
    (>!! dead-ch true)))

(defrecord VirtualPeer [fn-params]
  component/Lifecycle

  (start [{:keys [sync queue] :as component}]
    (taoensso.timbre/info "Starting Virtual Peer")

    (let [peer (extensions/create sync :peer)
          payload (extensions/create sync :payload)
          pulse (extensions/create sync :pulse)
          shutdown (extensions/create sync :shutdown)

          payload-ch (chan 1)
          shutdown-ch (chan 1)
          status-ch (chan 1)
          dead-ch (chan)]
      
      (extensions/write-place sync peer {:pulse pulse :shutdown shutdown :payload payload})
      (extensions/on-change sync payload #(>!! payload-ch %))

      (dire/with-handler! #'payload-loop
        java.lang.Exception
        (fn [e & _] (.printStackTrace e)))

      (assoc component
        :peer-node peer
        :payload-node payload
        :pulse-node pulse
        :shutdown-node shutdown
        
        :payload-ch payload-ch
        :shutdown-ch shutdown-ch
        :status-ch status-ch
        :dead-ch dead-ch

        :payload-thread (future (payload-loop sync queue payload-ch shutdown-ch status-ch dead-ch pulse fn-params)))))

  (stop [component]
    (taoensso.timbre/info "Stopping Virtual Peer")

    (close! (:payload-ch component))
    (close! (:shutdown-ch component))
    (close! (:status-ch component))

    (<!! (:dead-ch component))
    
    component))

(defn virtual-peer [fn-params]
  (map->VirtualPeer {:fn-params fn-params}))

