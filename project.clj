(defproject org.onyxplatform/onyx "0.9.7-SNAPSHOT"
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
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.2.374"]
                 [org.apache.curator/curator-framework "2.9.1"]
                 [org.apache.curator/curator-test "2.9.1"]
                 [org.apache.zookeeper/zookeeper "3.4.6"
                  :exclusions [org.slf4j/slf4j-log4j12]]
                 [org.apache.bookkeeper/bookkeeper-server "4.3.2"
                  :exclusions [org.slf4j/slf4j-log4j12]]
                 [org.rocksdb/rocksdbjni "4.0"]
                 [org.slf4j/slf4j-api "1.7.12"]
                 [org.slf4j/slf4j-nop "1.7.12"]
                 [org.btrplace/scheduler-api "0.46"]
                 [org.btrplace/scheduler-choco "0.46"]
                 [com.stuartsierra/dependency "0.2.0"]
                 [com.stuartsierra/component "0.3.1"]
                 [com.taoensso/timbre "4.1.4"]
                 [com.taoensso/nippy "2.11.1"]
                 [io.aviso/pretty "0.1.25"]
                 [uk.co.real-logic/aeron-all "0.9.1"]
                 [prismatic/schema "1.0.5"]
                 [log4j/log4j "1.2.17"]
                 [clj-tuple "0.2.2"]
                 [clj-fuzzy "0.3.1"]]
  :jvm-opts ["-Xmx4g" "-XX:-OmitStackTraceInFastThrow"]
  :profiles {:dev {:dependencies [[org.clojure/tools.nrepl "0.2.11"]
                                  [table "0.5.0"]
                                  [org.clojure/test.check "0.9.0"]
                                  [mdrogalis/stateful-check "0.3.2"]
                                  [com.gfredericks/test.chuck "0.2.3"]
                                  [joda-time/joda-time "2.8.2"]]
                   :plugins [[lein-jammin "0.1.1"]
                             [lein-set-version "0.4.1"]
                             [mdrogalis/lein-unison "0.1.14"]
                             [codox "0.8.8"]]}
             :reflection-check {:global-vars {*warn-on-reflection* true
                                              *assert* false
                                              *unchecked-math* :warn-on-boxed}}
             :circle-ci {:jvm-opts ["-Xmx2500M"
                                    "-XX:+UnlockCommercialFeatures"
                                    "-XX:+FlightRecorder"
                                    "-XX:StartFlightRecording=duration=1080s,filename=recording.jfr"]}
             :clojure-1.7 {:dependencies [[org.clojure/clojure "1.7.0"]]}
             :clojure-1.8 {:dependencies [[org.clojure/clojure "1.8.0"]]}}
  :test-selectors {:default (constantly true)
                   :smoke :smoke}
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
     :merge "master"}
    {:git "git@onyx-redis:onyx-platform/onyx-redis.git"
     :branch "compatibility"
     :release-branch "master"
     :release-script "scripts/release.sh"
     :merge "master"}
    {:git "git@onyx-seq:onyx-platform/onyx-seq.git"
     :branch "compatibility"
     :release-branch "master"
     :release-script "scripts/release.sh"
     :merge "master"}
    {:git "git@onyx-durable-queue:onyx-platform/onyx-durable-queue.git"
     :branch "compatibility"
     :release-branch "master"
     :release-script "scripts/release.sh"
     :merge "master"}
    {:git "git@onyx-metrics:onyx-platform/onyx-metrics.git"
     :branch "compatibility"
     :release-branch "master"
     :release-script "scripts/release.sh"
     :merge "master"}
    {:git "git@onyx-bookkeeper:onyx-platform/onyx-bookkeeper.git"
     :branch "compatibility"
     :release-branch "master"
     :release-script "scripts/release.sh"
     :merge "master"}
    {:git "git@onyx-http:onyx-platform/onyx-http.git"
     :branch "compatibility"
     :release-branch "master"
     :release-script "scripts/release.sh"
     :merge "master"}
    {:git "git@onyx-elasticsearch:onyx-platform/onyx-elasticsearch.git"
     :branch "compatibility"
     :release-branch "master"
     :release-script "scripts/release.sh"
     :merge "master"}
    {:git "git@onyx-amazon-sqs:onyx-platform/onyx-amazon-sqs.git"
     :branch "compatibility"
     :release-branch "master"
     :release-script "scripts/release.sh"
     :merge "master"}
    {:git "git@onyx-dashboard:onyx-platform/onyx-dashboard.git"
     :branch "compatibility"
     :release-branch "master"
     :release-script "scripts/release.sh"
     :merge "master"}
    {:git "git@onyx-starter:onyx-platform/onyx-starter.git"
     :branch "compatibility"
     :release-branch "master"
     :release-script "script/release.sh"
     :merge "master"}
    {:git "git@onyx-template:onyx-platform/onyx-template.git"
     :branch "compatibility"
     :release-branch "master"
     :release-script "scripts/release.sh"
     :skip-compatibility? true
     :merge "master"}
    {:git "git@learn-onyx:onyx-platform/learn-onyx.git"
     :branch "compatibility"
     :release-branch "master"
     :release-script "scripts/release.sh"
     :merge "master"}
    {:git "git@onyx-examples:onyx-platform/onyx-examples.git"
     :project-file :discover
     :branch "compatibility"
     :release-branch "master"
     :release-script "release.sh"
     :merge "master"}
    {:git "git@onyx-cheat-sheet:onyx-platform/onyx-cheat-sheet.git"
     :branch "compatibility"
     :release-branch "master"
     :release-script "script/release.sh"
     :skip-compatibility? true
     :merge "master"}
    {:git "git@onyx-platform.github.io:onyx-platform/onyx-platform.github.io.git"
     :branch "compatibility"
     :release-branch "master"
     :release-script "build-site.sh"
     :skip-compatibility? true
     :merge "master"}]}
  :codox {:output-dir "doc/api"})
