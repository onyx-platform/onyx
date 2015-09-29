(defproject org.onyxplatform/onyx "0.7.4-SNAPSHOT"
  :description "Distributed, masterless, high performance, fault tolerant data processing for Clojure"
  :url "https://github.com/onyx-platform/onyx"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"snapshots" {:url "https://clojars.org/repo"
                              :username :env
                              :password :env
                              :sign-releases false}
                 "releases" {:url "https://clojars.org/repo"
                             :username :env
                             :password :env
                             :sign-releases false}}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [org.apache.curator/curator-framework "2.9.0"]
                 [org.apache.curator/curator-test "2.9.0"]
                 [clj-tuple "0.2.2"]
                 [com.stuartsierra/dependency "0.2.0"]
                 [com.stuartsierra/component "0.3.0"]
                 [com.taoensso/timbre "4.1.2"]
                 [com.taoensso/nippy "2.9.1"]
                 [uk.co.real-logic/Agrona "0.4.4"]
                 [uk.co.real-logic/aeron-client "0.1.4"]
                 [uk.co.real-logic/aeron-driver "0.1.4"]
                 [prismatic/schema "1.0.1"]
                 [org.apache.zookeeper/zookeeper "3.4.6" :exclusions [org.slf4j/slf4j-log4j12]]
                 [log4j/log4j "1.2.17"]
                 [org.slf4j/slf4j-api "1.7.12"]
                 [org.slf4j/slf4j-nop "1.7.12"]]
  :aot [onyx.interop]
  :jvm-opts ["-Xmx4g"]
  :profiles {:dev {:aot ^:replace []
                   :dependencies [[yeller-timbre-appender "2.0.0"]
                                  [org.clojure/tools.nrepl "0.2.11"]
                                  [org.clojure/test.check "0.8.2"]
                                  [org.clojars.czan/stateful-check "0.3.1"]
                                  [com.gfredericks/test.chuck "0.2.0"]
                                  [joda-time/joda-time "2.8.2"]]
                   :plugins [[lein-jammin "0.1.1"]
                             [lein-set-version "0.4.1"]
                             [lonocloud/lein-unison "0.1.11"]
                             [codox "0.8.8"]]}
             :circle-ci {:jvm-opts ["-Xmx2500M"
                                    "-XX:+UnlockCommercialFeatures"
                                    "-XX:+FlightRecorder"
                                    "-XX:StartFlightRecording=duration=1080s,filename=recording.jfr"]}}
  :unison
  {:repos
   [{:git "git@onyx-kafka:onyx-platform/onyx-kafka.git"
     :branch "compatibility"
     :release-branch "master"
     :release-script "scripts/release.sh"
     :merge "master"}
    {:git "git@onyx-datomic:onyx-platform/onyx-datomic.git"
     :branch "compatibility"
     :release-branch "master"
     :release-script "scripts/release.sh"
     :merge "master"}
    {:git "git@onyx-sql:onyx-platform/onyx-sql.git"
     :branch "compatibility"
     :release-branch "master"
     :release-script "scripts/release.sh"
     :merge "master"}]}
  :codox {:output-dir "doc/api"})
