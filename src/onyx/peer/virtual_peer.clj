(ns ^:no-doc onyx.peer.virtual-peer
  (:require [clojure.core.async :refer [chan alts!! >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :as timbre]
            [dire.core :as dire]
            [onyx.extensions :as extensions]
            [onyx.peer.task-lifecycle :refer [task-lifecycle]]))

(defn payload-loop [id sync queue payload-ch shutdown-ch status-ch dead-ch pulse fn-params]
  (let [complete-ch (chan 1)]
    (loop [pipeline nil]
      (when-let [[v ch] (alts!! [shutdown-ch complete-ch payload-ch] :priority true)]
        (when-not (nil? pipeline)
          (component/stop pipeline))

        (cond (nil? v) (extensions/delete sync (:node pulse))
              (= ch complete-ch) (recur nil)
              (= ch payload-ch)
              (let [payload-node (:path v)
                    payload (extensions/read-node sync payload-node)
                    status-ch (chan 1)]

                (extensions/on-change sync (:node/status (:nodes payload)) #(>!! status-ch %))
                (extensions/touch-node sync (:node/ack (:nodes payload)))

                (<!! status-ch)

                (let [new-pipeline (task-lifecycle id payload sync queue payload-ch complete-ch fn-params)]
                  (recur (component/start new-pipeline))))
              :else (recur nil))))
    (>!! dead-ch true)))

(defrecord VirtualPeer [fn-params]
  component/Lifecycle

  (start [{:keys [sync queue] :as component}]
    (let [peer (extensions/create sync :peer)
          payload (extensions/create sync :payload)
          pulse (extensions/create sync :pulse)
          shutdown (extensions/create sync :shutdown)

          payload-ch (chan 1)
          shutdown-ch (chan 1)
          status-ch (chan 1)
          dead-ch (chan)]

      (taoensso.timbre/info (format "Starting Virtual Peer %s" (:uuid peer)))
      
      (extensions/write-node sync (:node peer)
                              {:id (:uuid peer)
                               :peer-node (:node peer)
                               :pulse-node (:node pulse)
                               :shutdown-node (:node shutdown)
                               :payload-node (:node payload)})
      
      (extensions/write-node sync (:node pulse) {:id (:uuid peer)})
      (extensions/write-node sync (:node shutdown) {:id (:uuid peer)})
      (extensions/on-change sync (:node payload) #(>!! payload-ch %))

      (dire/with-handler! #'payload-loop
        java.lang.Exception
        (fn [e & _] (timbre/info e)))

      (assoc component
        :peer peer
        :payload payload
        :pulse pulse
        :shutdown shutdown
        
        :payload-ch payload-ch
        :shutdown-ch shutdown-ch
        :status-ch status-ch
        :dead-ch dead-ch

        :payload-thread (future (payload-loop (:uuid peer) sync queue payload-ch
                                              shutdown-ch status-ch dead-ch pulse fn-params)))))

  (stop [component]
    (taoensso.timbre/info (format "Stopping Virtual Peer %s" (:uuid (:peer component))))

    (close! (:payload-ch component))
    (close! (:shutdown-ch component))
    (close! (:status-ch component))

    (<!! (:dead-ch component))
    
    component))

(defn virtual-peer [fn-params]
  (map->VirtualPeer {:fn-params fn-params}))

