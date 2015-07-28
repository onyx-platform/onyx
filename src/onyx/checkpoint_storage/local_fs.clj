(ns ^:no-doc onyx.checkpoint-storage.local-fs
    (:require [com.stuartsierra.component :as component]
              [taoensso.timbre :as timbre]
              [onyx.extensions :as extensions]))

(defrecord LocalFileSystemStorage [root-path]
  component/Lifecycle
  (start [component]
    (timbre/info "Starting Local File System storage")
    component)
  (stop [component]
    (timbre/info "Stopping Local File System storage")
    component)

  extensions/ICheckpointStorage
  (read-content [this location]
    (let [lines (clojure.string/split (slurp (str root-path "/" location ".edn")) #"\n")]
      (map read-string lines)))
  (write-content [this content]
    (let [location (java.util.UUID/randomUUID)]
      (doseq [c content]
        (spit (str root-path "/" location ".edn") (str (pr-str c) "\n") :append true))
      location))
  (delete-content [this location]
    (clojure.java.io/delete-file (str root-path "/" location ".edn"))))

(defn local-fs-storage [root-path]
  (LocalFileSystemStorage. root-path))
