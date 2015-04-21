(ns onyx.messaging.aeron-media-driver
  (:import [uk.co.real_logic.aeron Aeron FragmentAssemblyAdapter]
           [uk.co.real_logic.aeron Aeron$Context]
           [uk.co.real_logic.aeron.driver MediaDriver MediaDriver$Context ThreadingMode]))

(defn -main [& args]
  (let [ctx (doto (MediaDriver$Context.) 
              (.threadingMode ThreadingMode/SHARED)
              ;(.threadingMode ThreadingMode/DEDICATED)
              (.dirsDeleteOnExit true))
        media-driver (MediaDriver/launch ctx)]
    media-driver)
  (while true
    (Thread/sleep 1000000)))

