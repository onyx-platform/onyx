(defproject com.mdrogalis/onyx "0.6.0-SNAPSHOT"
  :description "Distributed, masterless, fault tolerant data processing for Clojure"
  :url "https://github.com/MichaelDrogalis/onyx"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :jvm-opts ["-Xmx4g"]
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [org.apache.curator/curator-test "2.6.0"]
                 [com.stuartsierra/dependency "0.1.1"]
                 [com.stuartsierra/component "0.2.1"]
                 [com.taoensso/timbre "3.0.1"]
                 [com.taoensso/nippy "2.8.0"]
                 [uk.co.real-logic/Agrona "0.2.1-SNAPSHOT"]
                 [uk.co.real-logic/aeron-client "0.1-SNAPSHOT"]
                 [uk.co.real-logic/aeron-driver "0.1-SNAPSHOT"]
                 [uk.co.real-logic/aeron-common "0.1-SNAPSHOT"]
                 [stylefruits/gniazdo "0.3.1"]
                 [prismatic/schema "0.3.1"]
                 [zookeeper-clj "0.9.1" :exclusions [io.netty/netty]]
                 [io.netty/netty-all "4.0.26.Final"]
                 [http-kit "2.1.17"]
                 [gloss "0.2.4" :exclusions [manifold]]
                 [aleph "0.4.0-beta3"]
                 [dire "0.5.1"]]
  :profiles {:dev {:dependencies [[midje "1.6.3"]
                                  [org.clojure/data.generators "0.1.2"]
                                  [com.datomic/datomic-free "0.9.4755"
                                   :exclusions [com.fasterxml.jackson.core/jackson-core io.netty/netty]]
                                  [com.datomic/simulant "0.1.6"]
                                  [org.clojure/tools.nrepl "0.2.3"]]
                   :plugins [[lein-midje "3.1.1"]
                             [codox "0.8.8"]]}
             :circle-ci {:jvm-opts ["-Xmx4g"]}}
  :codox {:output-dir "doc/api"})

