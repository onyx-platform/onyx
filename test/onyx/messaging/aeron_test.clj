(ns onyx.messaging.aeron-test
  (:require [clojure.test :refer [deftest is testing]]
            [onyx.messaging.aeron :as aeron])
  (:import (io.aeron.driver ThreadingMode))
  (:import [io.aeron Publication]))

(deftest get-dedicated-threading-model
  (is (= (aeron/get-threading-model :dedicated)
         ThreadingMode/DEDICATED)))

(deftest get-shared-threading-model
  (is (= (aeron/get-threading-model :shared)
         ThreadingMode/SHARED)))

(deftest get-shared-threading-model
  (is (= (aeron/get-threading-model :shared-network)
         ThreadingMode/SHARED_NETWORK)))
