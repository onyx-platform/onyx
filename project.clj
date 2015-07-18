(defproject org.onyxplatform/onyx "0.7.0-SNAPSHOT"
  :description "Distributed, masterless, high performance, fault tolerant data processing for Clojure"
  :url "https://github.com/onyx-platform/onyx"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :jvm-opts ["-Xmx4g"]
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [org.apache.curator/curator-framework "2.8.0"]
                 [org.apache.curator/curator-test "2.8.0"]
                 [clj-tuple "0.2.2"]
                 [com.mdrogalis/rotating-seq "0.1.3"]
                 [com.stuartsierra/dependency "0.1.1"]
                 [com.stuartsierra/component "0.2.1"]
                 [com.taoensso/timbre "4.0.2"]
                 [com.taoensso/nippy "2.9.0"]
                 [uk.co.real-logic/Agrona "0.4"]
                 [uk.co.real-logic/aeron-client "0.1"]
                 [uk.co.real-logic/aeron-driver "0.1"]
                 [prismatic/schema "0.4.0"]
                 [org.apache.zookeeper/zookeeper "3.4.1" :exclusions [org.slf4j/slf4j-log4j12 io.netty/netty]]
                 [log4j/log4j "1.2.17"]
                 [org.slf4j/slf4j-api "1.7.12"]
                 [org.slf4j/slf4j-nop "1.7.12"]
                 [io.netty/netty-all "4.0.26.Final"]]
  :aot [onyx.interop]
  :profiles {:dev {:aot ^:replace []
                   :dependencies [[midje "1.6.3"]
                                  [org.clojars.czan/stateful-check "0.3.0-20150522.062325-1"]
                                  [org.clojure/test.check "0.7.0"]
                                  [com.gfredericks/test.chuck "0.1.16"]
                                  [org.clojure/data.generators "0.1.2"]
                                  [org.clojure/tools.nrepl "0.2.3"]
                                  [yeller-timbre-appender "0.4.1"]]
                   :plugins [[lein-midje "3.1.1"]
                             [lein-jammin "0.1.1"]
                             [codox "0.8.8"]]}
             :circle-ci {:jvm-opts ["-Xmx2500M"
                                    "-XX:+UnlockCommercialFeatures"  
                                    "-XX:+FlightRecorder"  
                                    "-XX:StartFlightRecording=duration=1080s,filename=recording.jfr"]}}
  :codox {:output-dir "doc/api"})
