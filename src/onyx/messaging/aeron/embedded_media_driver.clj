(ns onyx.messaging.aeron.embedded-media-driver
  (:require [taoensso.timbre :refer [fatal info debug warn] :as timbre]
            [com.stuartsierra.component :as component]
            [onyx.static.default-vals :refer [defaults arg-or-default]])
  (:import [io.aeron.driver MediaDriver MediaDriver$Context ThreadingMode]))

(defn delete-aeron-directory-safe [^MediaDriver$Context media-driver-context]
  (try (.deleteAeronDirectory media-driver-context)
       (catch java.nio.file.NoSuchFileException nsfe
         (info "Couldn't delete aeron media dir. May have been already deleted by shutdown hook." nsfe))))

(defn get-threading-model
  [media-driver]
  (cond (= media-driver :dedicated) ThreadingMode/DEDICATED
        (= media-driver :shared) ThreadingMode/SHARED
        (= media-driver :shared-network) ThreadingMode/SHARED_NETWORK))

(defrecord EmbeddedMediaDriver [peer-config]
  component/Lifecycle
  (start [component]
    (let [embedded-driver? (arg-or-default :onyx.messaging.aeron/embedded-driver? peer-config)
          threading-mode (get-threading-model (arg-or-default :onyx.messaging.aeron/embedded-media-driver-threading peer-config))
          media-driver-context (if embedded-driver?
                                 (-> (MediaDriver$Context.) 
                                     (.threadingMode threading-mode)
                                     (.dirsDeleteOnStart true)))
          media-driver (if embedded-driver?
                         (MediaDriver/launch media-driver-context))]
      (when embedded-driver? 
        (.addShutdownHook (Runtime/getRuntime) 
                          (Thread. (partial delete-aeron-directory-safe media-driver-context))))
      (assoc component 
             :media-driver media-driver 
             :media-driver-context media-driver-context)))
  (stop [{:keys [media-driver media-driver-context subscribers] :as component}]
    (when media-driver 
      (.close ^MediaDriver media-driver))
    (when media-driver-context 
      (delete-aeron-directory-safe media-driver-context))
    (assoc component :media-driver nil :media-driver-context nil)))

(defn new-embedded-media-driver [peer-config]
  (->EmbeddedMediaDriver peer-config))
