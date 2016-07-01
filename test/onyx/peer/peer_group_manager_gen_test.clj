(ns onyx.peer.peer-group-manager-gen-test
  (:require [clojure.core.async :refer [>!! <!! alts!! promise-chan close! chan thread poll!]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info error warn fatal]]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.static.logging-configuration :as logging-config]
            [onyx.test-helper :refer [load-config with-test-env playback-log]]
            [onyx.log.zookeeper :refer [zookeeper]]
            [onyx.extensions :as extensions]
            [onyx.system :as system]
            [onyx.static.default-vals :refer [arg-or-default]]
            [clojure.test :refer [deftest is]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]))

(def add-peer-gen
  (gen/fmap (fn [oid] 
              [:add-peer (keyword (str "po" oid))]) 
            (gen/resize 20 gen/pos-int)))

(def remove-peer-gen
  (gen/fmap (fn [oid] 
              [:remove-peer (keyword (str "po" oid))]) 
            (gen/resize 20 gen/pos-int)))

(def restart-peer-group-gen 
  (gen/return [:restart-peer-group]))

(def break-conn
  (gen/return [:break-conn]))

(deftest peergroup-gen-test
  (checking
    "Checking percentages allocation causes peers to be evenly split"
    (times 5)
    [n-commands (gen/fmap #(+ 10 %) gen/pos-int)
     commands (gen/vector (gen/one-of [add-peer-gen remove-peer-gen break-conn restart-peer-group-gen]) n-commands)]
    (let [onyx-id (java.util.UUID/randomUUID)
          config (load-config)
          env-config (assoc (:env-config config) :onyx/tenancy-id onyx-id)
          peer-config (assoc (:peer-config config) :onyx/tenancy-id onyx-id)
          env (onyx.api/start-env env-config)
          peer-group (onyx.api/start-peer-group peer-config)
          group-ch (:group-ch (:peer-group-manager peer-group))
          ch (chan 10000)]
      (try
       (run! (partial >!! group-ch) commands)
       (let [final-replica (playback-log (:log env) (extensions/subscribe-to-log (:log env) ch) ch 6000)
             model (reduce (fn [model [cmd & args]]
                             (case cmd
                               :add-peer
                               (update model :peers conj (first args))
                               :remove-peer
                               (update model :peers disj (first args))
                               model))
                           {:peers #{}}
                           commands)]
         (is (= 1 (count (:groups final-replica))))
         (is (= (count (:peers final-replica)) (count (:peers model)))))
       (finally
        (onyx.api/shutdown-peer-group peer-group)
        (onyx.api/shutdown-env env))))))
