(ns ^:no-doc onyx.messaging.aeron.publication-pool
  (:require [clojure.core.async :refer [alts!! <!! >!! timeout chan close! thread]]
            [com.stuartsierra.component :as component]
            [onyx.messaging.aeron.publication-manager :as pubm]
            [onyx.static.default-vals :refer [defaults arg-or-default]]
            [taoensso.timbre :refer [fatal info] :as timbre]))

(def command-channel-size 1000)

(defrecord TrackedPub [publication last-used])

(defprotocol PPublicationPool
  (add-publication! [this conn-spec])
  (remove-publication! [this conn-spec])
  (get-publication [this conn-spec])
  (gc-publications! [this]))

;; Ensures all proper modifications to publication-pool are done on a single thread
(defn start-manager-thread! [publication-pool opts command-ch shutdown-ch]
  (let [gc-interval (arg-or-default :onyx.messaging/peer-link-gc-interval opts)
        idle-timeout ^long (arg-or-default :onyx.messaging/peer-link-idle-timeout opts)]
    (thread
      (loop []
        (let [timeout-ch (timeout gc-interval)
              [v ch] (alts!! [shutdown-ch timeout-ch command-ch] :priority true)]
          (cond (= ch timeout-ch)
                (do (try
                      (let [t (System/currentTimeMillis)
                            snapshot @(:publications publication-pool)
                            to-remove (map first
                                           (filter (fn [[k v]] (>= (- t ^long @(:last-used v)) idle-timeout))
                                                   snapshot))]
                        (doseq [k to-remove]
                          (info (format "Publication %s GC'd after non use within %s" k idle-timeout))
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
                        (if-let [pub (get @(:publications publication-pool) (:channel conn-spec))]
                          (deliver promised pub)
                          (deliver promised (add-publication! publication-pool conn-spec)))))
                    (recur)))))
      (info "Shutdown Publication Pool Manager Thread"))))

(defrecord PublicationPool [opts send-idle-strategy publications manager-thread command-ch shutdown-ch]
  PPublicationPool

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
      (let [publication-creation-timeout (arg-or-default :onyx.messaging.aeron/publication-creation-timeout opts)
            tracked-pub (promise)]
        (info "Creating publication at:" (into {} conn-spec))
        (>!! (:command-ch this) [:add-publication conn-spec tracked-pub])
        (:publication (deref tracked-pub publication-creation-timeout nil)))))

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

(defn new-publication-pool [opts send-idle-strategy]
  (->PublicationPool opts send-idle-strategy (atom {}) nil (chan command-channel-size) (chan)))
