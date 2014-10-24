(ns ^:no-doc onyx.peer.virtual-peer
  (:require [clojure.core.async :refer [chan mult tap alts!! >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [warn] :as timbre]
            [dire.core :as dire]
            [onyx.extensions :as extensions]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.peer.task-lifecycle :refer [task-lifecycle]]))

(defn force-close-pipeline! [pipeline]
  (try
    (l-ext/close-lifecycle-resources* (:pipeline-data pipeline))
    (catch Exception e
      (timbre/warn e))))

(defn payload-loop [id sync queue payload-ch shutdown-ch status-ch dead-ch pulse opts]
  (try
    (let [complete-ch (chan 1)
          err-ch (chan 1)]
      (loop [pipeline nil]
        (when-let [[v ch] (alts!! [shutdown-ch err-ch complete-ch payload-ch] :priority true)]
          (when (and (not (nil? pipeline)) (not= ch err-ch))
            (component/stop pipeline))

          (cond (nil? v) (extensions/delete sync (:node pulse))
                (= ch complete-ch) (recur nil)
                (= ch shutdown-ch) (recur nil)
                (= ch err-ch) (force-close-pipeline! pipeline)
                (= ch payload-ch)
                (let [payload-node (:path v)
                      payload (extensions/read-node sync payload-node)
                      status-ch (chan 1)]

                  (extensions/on-change sync (:node/status (:nodes payload)) #(>!! status-ch %))
                  (extensions/touch-node sync (:node/ack (:nodes payload)))

                  (<!! status-ch)

                  (let [new-pipeline (task-lifecycle id payload sync queue payload-ch complete-ch err-ch opts)]
                    (recur (component/start new-pipeline))))))))
    (catch Exception e
      (warn e))
    (finally
     (>!! dead-ch true))))

(defrecord VirtualPeer [opts]
  component/Lifecycle

  (start [{:keys [sync queue] :as component}]
    (let [peer (extensions/create sync :peer)
          payload (extensions/create sync :payload)
          pulse (extensions/create sync :pulse)
          shutdown (extensions/create sync :shutdown)

          payload-ch (chan 1)
          shutdown-ch (chan 1)
          status-ch (chan 1)
          
          dead-head-ch (chan 1)
          dead-close-tail-ch (chan 1)
          dead-restart-tail-ch (chan 1)
          dead-mult (mult dead-head-ch)]

      (tap dead-mult dead-close-tail-ch)
      (tap dead-mult dead-restart-tail-ch)

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
        :dead-head-ch dead-head-ch

        :dead-close-tail-ch dead-close-tail-ch
        :dead-restart-tail-ch dead-restart-tail-ch

        :payload-thread (future (payload-loop (:uuid peer) sync queue payload-ch
                                              shutdown-ch status-ch dead-head-ch pulse opts)))))

  (stop [component]
    (taoensso.timbre/info (format "Stopping Virtual Peer %s" (:uuid (:peer component))))

    (close! (:payload-ch component))
    (close! (:shutdown-ch component))
    (close! (:status-ch component))
    
    (<!! (:dead-close-tail-ch component))

    (close! (:dead-head-ch component))
    
    component))

(defn virtual-peer
  [{:keys [onyx.peer/fn-params onyx.peer/sequential-back-off
           onyx.peer/drained-back-off]}]
  (map->VirtualPeer {:opts {:fn-params fn-params
                            :back-off sequential-back-off
                            :drained-back-off drained-back-off}}))

