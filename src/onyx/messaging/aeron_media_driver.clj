(ns onyx.messaging.aeron-media-driver
  (:gen-class)
  (:require [clojure.core.async :refer [chan <!!]])
  (:import [io.aeron Aeron$Context]
           [io.aeron.driver MediaDriver MediaDriver$Context ThreadingMode]))

(defn -main [& args]
  (let [ctx (MediaDriver$Context.)
        media-driver (MediaDriver/launch ctx)]
    (println "Launched the Media Driver. Blocking forever...")
    (<!! (chan))))
