(ns onyx.peer.log-version-test
  (:require [clojure.test :refer [deftest is testing]]
            [onyx.test-helper :refer [load-config with-test-env]]
            [onyx.peer.log-version :refer [version parse-semver check-compatible-log-versions!]]))

(deftest version-test
  (is (some? version))
  (is (some? (parse-semver version))))

(deftest parse-semver-test
  (is (= ["1" "2" "3" ""] (parse-semver "1.2.3")))
  (is (= ["1" "2" "3" "alpha4"] (parse-semver "1.2.3-alpha4")))
  (is (nil? (parse-semver "1.2")))
  (is (nil? (parse-semver nil))))

(deftest check-compatible-versions-test
  (is (thrown? clojure.lang.ExceptionInfo (check-compatible-log-versions! "0.1.0")))
  (is (thrown? clojure.lang.ExceptionInfo (check-compatible-log-versions! "0.99.0")))
  (is (nil? (check-compatible-log-versions! version)))
  (let [[major minor] (parse-semver version)
        future-version (str major "." minor ".99")]
    (is (nil? (check-compatible-log-versions! future-version)))))
