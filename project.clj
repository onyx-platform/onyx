(defproject org.onyxplatform/onyx "0.10.0-SNAPSHOT"
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
                 [org.clojure/core.async "0.3.442"]
                 [org.apache.curator/curator-framework "2.9.1"]
                 [org.apache.curator/curator-test "2.9.1"]
                 [org.apache.zookeeper/zookeeper "3.4.10" :exclusions [org.slf4j/slf4j-log4j12]]
                 [net.cgrand/xforms "0.9.0"]
                 [org.slf4j/slf4j-api "1.7.12"]
                 [org.slf4j/slf4j-nop "1.7.12"]
                 [org.btrplace/scheduler-api "0.46"]
                 [org.btrplace/scheduler-choco "0.46"]
                 [com.stuartsierra/dependency "0.2.0"]
                 [com.stuartsierra/component "0.3.2"]
                 [metrics-clojure "2.9.0"]
                 [com.taoensso/timbre "4.8.0"]
                 [com.taoensso/nippy "2.13.0"]
                 [io.aeron/aeron-all "1.2.5"]
                 [io.replikativ/hasch "0.3.3" 
                  :exclusions [org.clojure/clojurescript com.cognitect/transit-clj 
                               com.cognitect/transit-cljs org.clojure/data.fressian 
                               com.cemerick/austin]]
                 [prismatic/schema "1.0.5"]
                 [com.amazonaws/aws-java-sdk-s3 "1.11.58"]
                 [clj-tuple "0.2.2"]
                 [clj-fuzzy "0.3.1" :exclusions [org.clojure/clojurescript]]]
  :jvm-opts ^:replace ["-server"
                       "-Xmx2400M"
                       "-XX:+UseG1GC" 
                       "-Daeron.client.liveness.timeout=100000000000"
                       "-Daeron.image.liveness.timeout=100000000000"
                       "-XX:-OmitStackTraceInFastThrow" 
                       "-XX:+UnlockCommercialFeatures"
                       "-XX:+FlightRecorder"
                       "-XX:StartFlightRecording=duration=1080s,filename=recording.jfr"]
  :profiles {:dev {:global-vars {*warn-on-reflection* true}
                   :dependencies [[org.clojure/tools.nrepl "0.2.11"]
                                  [org.clojure/java.jmx "0.3.4"]
                                  [clojure-future-spec "1.9.0-alpha14"]
                                  [table "0.5.0"]
                                  [org.clojure/test.check "0.9.0"]
                                  [org.senatehouse/expect-call "0.1.0"]
                                  [macroz/tangle "0.1.9"]
                                  [mdrogalis/stateful-check "0.3.2"]
                                  ;; TODO, switch back to test.chuck mainline
                                  ;; once PR is accepted
                                  [lbradstreet/test.chuck "0.2.7-20160709.160608-2"]
                                  [joda-time/joda-time "2.8.2"]]
                   :plugins [[lein-jammin "0.1.1"]
                             [lein-set-version "0.4.1"]
                             [mdrogalis/lein-unison "0.1.17"]
                             [codox "0.8.8"]]
                   :resource-paths ["test-resources/"]}
             :circle-ci {:global-vars {*warn-on-reflection* true}
                         :jvm-opts ["-Xmx2500M"
                                    "-XX:+UnlockCommercialFeatures"
                                    "-XX:+FlightRecorder"
                                    "-XX:StartFlightRecording=duration=1080s,filename=recording.jfr"]}
             :clojure-1.7 {:dependencies [[org.clojure/clojure "1.7.0"]]}
             :clojure-1.8 {:dependencies [[org.clojure/clojure "1.8.0"]]}}
  :test-selectors {:default (fn [t] (if-not (or (:stress t) (:broken t))
                                      t)) 
                   :stress :stress
                   :broken :broken
                   :smoke :smoke}
  :unison
  {:repos
   [{:git "git@onyx-kafka:onyx-platform/onyx-kafka.git"
     :branch "compatibility"
     :release-branch "master"
     :release-script "scripts/release.sh"
     :merge "master"}
    {:git "git@onyx-peer-http-query:onyx-platform/onyx-peer-http-query.git"
     :branch "compatibility"
     :release-branch "master"
     :release-script "scripts/release.sh"
     :merge "master"}
    ; {:git "git@onyx-kafka-0.8:onyx-platform/onyx-kafka-0.8.git"
    ;  :branch "compatibility"
    ;  :release-branch "master"
    ;  :release-script "scripts/release.sh"
    ;  :merge "master"}
    {:git "git@onyx-datomic:onyx-platform/onyx-datomic.git"
     :branch "compatibility"
     :release-branch "master"
     :release-script "scripts/release.sh"
     :merge "master"}
    ; {:git "git@onyx-sql:onyx-platform/onyx-sql.git"
    ;  :branch "compatibility"
    ;  :release-branch "master"
    ;  :release-script "scripts/release.sh"
    ;  :merge "master"}
    {:git "git@onyx-redis:onyx-platform/onyx-redis.git"
     :branch "compatibility"
     :release-branch "master"
     :release-script "scripts/release.sh"
     :merge "master"}
    ; {:git "git@onyx-durable-queue:onyx-platform/onyx-durable-queue.git"
    ;  :branch "compatibility"
    ;  :release-branch "master"
    ;  :release-script "scripts/release.sh"
    ;  :merge "master"}
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
    ; {:git "git@onyx-http:onyx-platform/onyx-http.git"
    ;  :branch "compatibility"
    ;  :release-branch "master"
    ;  :release-script "scripts/release.sh"
    ;  :merge "master"}
    ; {:git "git@onyx-elasticsearch:onyx-platform/onyx-elasticsearch.git"
    ;  :branch "compatibility"
    ;  :release-branch "master"
    ;  :release-script "scripts/release.sh"
    ;  :merge "master"}
    {:git "git@onyx-amazon-sqs:onyx-platform/onyx-amazon-sqs.git"
     :branch "compatibility"
     :release-branch "master"
     :release-script "scripts/release.sh"
     :merge "master"}
    {:git "git@onyx-amazon-s3:onyx-platform/onyx-amazon-s3.git"
     :branch "compatibility"
     :release-branch "master"
     :release-script "scripts/release.sh"
     :merge "master"}
    {:git "git@onyx-dashboard:onyx-platform/onyx-dashboard.git"
     :branch "compatibility"
     :release-branch "master"
     :release-script "scripts/release.sh"
     :merge "master"}
    ; {:git "git@onyx-starter:onyx-platform/onyx-starter.git"
    ;  :branch "compatibility"
    ;  :release-branch "master"
    ;  :release-script "script/release.sh"
    ;  :merge "master"}
    {:git "git@onyx-template:onyx-platform/onyx-template.git"
     :branch "compatibility"
     :release-branch "master"
     :release-script "scripts/release.sh"
     :skip-compatibility? true
     :merge "master"}
    {:git "git@learn-onyx:onyx-platform/learn-onyx.git"
     :branch "compatibility"
     :release-branch "answers"
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
     :release-script "scripts/release.sh"
     :skip-compatibility? true
     :merge "master"}
    {:git "git@onyx-platform.github.io:onyx-platform/onyx-platform.github.io.git"
     :branch "compatibility"
     :release-branch "master"
     :release-script "build-site.sh"
     :skip-compatibility? true
     :merge "master"}]}
  :codox {:output-dir "doc/api"})
