(ns onyx.peer.peer-group-manager-pure-gen-test
  (:require [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info error warn fatal]]
            [onyx.system :as system]
            [onyx.generative.peer-model :as g]
            [clojure.test :refer [deftest is]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]))

(deftest peer-group-gen-test
  (checking
    "Checking peer group manager operation"
    (times 5000)
    [n-commands gen/pos-int
     commands (gen/vector (gen/one-of [g/add-peer-gen g/remove-peer-gen g/add-peer-group-gen 
                                       ;g/remove-peer-group-gen #_g/restart-peer-group-gen 
                                       g/apply-log-entries g/write-entries g/play-group-commands]) 
                          n-commands)]
    (println "comm" commands)
    (let [model (reduce (fn [model [gen-cmd g arg]]
                          (case gen-cmd
                            :add-peer-group 
                            (update model :groups conj g)
                            :group-command 
                            (if (get (:groups model) g)
                              (let [[grp-cmd & args] arg] 
                                (case grp-cmd
                                  :add-peer
                                  (update model :peers conj (first args))
                                  :remove-peer
                                  (update model :peers disj (first args))
                                  model))
                              model)
                            model))
                        {:groups #{}
                         :peers #{}}
                        commands)
          {:keys [replica groups]} (g/play-commands commands)]
      (println 
        (count (:groups model)) (count (:groups replica)) "groups check"
        (count (:peers model)) (count (:peers replica)) "peers"
        )
      (is (= (count (:groups model)) (count (:groups replica))) "groups check")
      (is (= (count (:peers model)) (count (:peers replica))) "peers"))))
