(ns ^:no-doc onyx.messaging.aeron.publication-group
  (:require [onyx.messaging.aeron.publication-manager :as pubm]
            [clojure.core.async :refer [alts!! alt!! <!! >!! <! >! timeout chan close! thread go]]
            [taoensso.timbre :refer [fatal info] :as timbre]
            [onyx.static.default-vals :refer [defaults arg-or-default]]
            [com.stuartsierra.component :as component]))

(def command-channel-size 1000)

(defrecord TrackedPub [publication last-used])

(defprotocol PPublicationGroup
  (add-publication! [this conn-spec])
  (remove-publication! [this conn-spec])
  (get-publication [this conn-spec])
  (gc-publications! [this]))

;; Ensures all proper modifications to publication-group are done on a single thread
(defn start-manager-thread! [publication-group opts command-ch shutdown-ch]
  (let [gc-interval (arg-or-default :onyx.messaging/peer-link-gc-interval opts)
        idle-timeout ^long (arg-or-default :onyx.messaging/peer-link-idle-timeout opts)]
    (thread 
      (loop []
        (let [timeout-ch (timeout gc-interval)
              [v ch] (alts!! [shutdown-ch timeout-ch command-ch] :priority true)] 
          (cond (= ch timeout-ch)
                (do (try
                      (let [t (System/currentTimeMillis)
                            snapshot @(:publications publication-group)
                            to-remove (map first
                                           (filter (fn [[k v]] (>= (- t ^long @(:last-used v)) idle-timeout))
                                                   snapshot))]
                        (doseq [k to-remove]
                          (pubm/stop (:publication (snapshot k)))))
                      (catch InterruptedException e
                        (throw e))
                      (catch Throwable e
                        (fatal e)))
                    (recur))
                (= ch command-ch)
                (do (case (first v)
                      :add-publication 
                      (let [[_ conn-spec promised] v] 
                        ;; First check if we've already added it.
                        ;; Since we are the single writer this is a safe point to check
                        (if-let [pub (get @(:publications publication-group) (:channel conn-spec))]
                          (deliver promised pub)
                          (deliver promised (add-publication! publication-group conn-spec))))
                      ;; Remove publication command is currently disabled, but it may be necessary
                      ;; to prevent publications from being stopped multiple times
                      ;:remove-publication 
                      ;(let [[_ conn-spec] v] 
                      ;  (when-let [pub (get @(:publications publication))]
                      ;    (pubm/stop (:publication pub))
                      ;    (swap! publications dissoc channel)))
                      )
                    (recur)))))
      (info "Shutdown Publication Group Manager Thread"))))


(defrecord PublicationGroup [opts send-idle-strategy publications manager-thread command-ch shutdown-ch]
  PPublicationGroup
  (add-publication! [this {:keys [stream-id channel] :as conn-spec}]
    (let [write-buffer-size (arg-or-default :onyx.messaging.aeron/write-buffer-size opts)
          pub-manager (-> (pubm/new-publication-manager channel 
                                                        stream-id 
                                                        send-idle-strategy
                                                        write-buffer-size
                                                        (fn [] (remove-publication! this conn-spec))) 
                          (pubm/connect) 
                          (pubm/start))
          tracked-pub (->TrackedPub pub-manager (atom (System/currentTimeMillis)))]
      (swap! publications assoc channel tracked-pub)
      tracked-pub))
  (remove-publication! [this {:keys [stream-id channel] :as conn-spec}]
    (swap! publications dissoc channel))
  (get-publication [this conn-spec]
    (if-let [pub (get @publications (:channel conn-spec))]
      (do
        (reset! (:last-used pub) (System/currentTimeMillis))
        (:publication pub))
      (let [tracked-pub (promise)] 
        (>!! (:command-ch this) [:add-publication conn-spec tracked-pub])
        (:publication (deref tracked-pub 2000 nil)))))
  component/Lifecycle
  (component/start [this]
    (assoc this :manager-thread (start-manager-thread! this opts command-ch shutdown-ch)))
  (component/stop [this]
    (close! (:shutdown-ch this))
    (<!! manager-thread)
    (doseq [pub (vals @publications)]
      (pubm/stop (:publication pub)))
    (reset! publications {})
    this))

(defn new-publication-group [opts send-idle-strategy]
  (->PublicationGroup opts send-idle-strategy (atom {}) nil (chan command-channel-size) (chan)))
