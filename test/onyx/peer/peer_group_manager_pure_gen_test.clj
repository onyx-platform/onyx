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
    (times 50)
    [n-commands gen/pos-int
     commands (gen/vector (gen/one-of [g/add-peer-gen g/remove-peer-gen g/add-peer-group-gen 
                                       ;g/remove-peer-group-gen #_g/restart-peer-group-gen 
                                       g/apply-log-entries g/write-outbox-entries g/play-group-commands]) 
                          n-commands)]
    (let [unique-groups (set (map second commands))
          finish-commands (take (* 50 (count unique-groups)) 
                                (cycle 
                                 (mapcat 
                                  (fn [g] 
                                    [[:play-group-commands g 10]
                                     [:write-outbox-entries g 10]
                                     [:apply-log-entries g 10]])
                                  unique-groups)))
          all-commands (into (vec commands) finish-commands)
          model (g/model-commands all-commands)
          {:keys [replica groups]} (g/play-events all-commands)]
      (is (= (count (:groups model)) (count (:groups replica))) "groups check")
      (is (= (count (:peers model)) (count (:peers replica))) "peers"))))
