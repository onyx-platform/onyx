(defproject com.mdrogalis/onyx "0.5.0-SNAPSHOT"
  :description "Distributed data processing"
  :url "https://github.com/MichaelDrogalis/onyx"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :jvm-opts ["-Xmx4g" "-Djava.net.preferIPv4Stack=true"]
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/core.async "0.1.267.0-0d7780-alpha"]
                 [org.clojure/data.generators "0.1.2"]
                 [org.clojure/data.fressian "0.2.0"]
                 [org.hornetq/hornetq-commons "2.4.0.Final"]
                 [org.hornetq/hornetq-core-client "2.4.0.Final"]
                 [org.hornetq/hornetq-server "2.4.0.Final"]
                 [org.apache.curator/curator-test "2.6.0"]
                 [com.stuartsierra/dependency "0.1.1"]
                 [com.stuartsierra/component "0.2.1"]
                 [com.taoensso/timbre "3.0.1"]
                 [com.postspectacular/rotor "0.1.0"]
                 [javax.servlet/servlet-api "2.5"]
                 [zookeeper-clj "0.9.1" :exclusions [commons-codec]]
                 [clj-http "0.9.1"]
                 [ring "1.2.2" :exclusions [joda-time]]
                 [dire "0.5.1"]
                 [prismatic/schema "0.3.1"]]
  :profiles {:dev {:dependencies [[midje "1.6.3"]
                                  [com.datomic/datomic-free "0.9.4755"
                                   :exclusions [com.fasterxml.jackson.core/jackson-core]
                                   :exclusions [org.fressian/fressian]]
                                  [com.datomic/simulant "0.1.6"]
                                  [org.clojure/tools.nrepl "0.2.3"]]
                   :plugins [[lein-midje "3.1.1"]
                             [codox "0.8.8"]]}
             :circle-ci {:jvm-opts ["-Xmx4g" "-Djava.net.preferIPv4Stack=true"]}}
  :codox {:output-dir "doc/api"})

