(ns onyx.test-helpers
  (:require [onyx.api]))

(defn with-env-peers [env-config peer-config n-peers f]
  (let [env (onyx.api/start-env env-config)
        v-peers (onyx.api/start-peers! n-peers peer-config)]
    (try
      (f)
      (finally
        (doseq [v-peer v-peers]
          (onyx.api/shutdown-peer v-peer))       
        (onyx.api/shutdown-env env)))))

