(ns onyx.messaging.aeron-media-driver
  (:require [clojure.core.async :refer [chan <!!]])
  (:import [uk.co.real_logic.aeron Aeron$Context]
           [uk.co.real_logic.aeron.driver MediaDriver MediaDriver$Context ThreadingMode]))

(defn -main [& args]
  (let [ctx (MediaDriver$Context.)
        media-driver (MediaDriver/launch ctx)]
    (println "Launched the Media Driver. Blocking forever...")
    (<!! (chan))))
