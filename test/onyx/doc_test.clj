(ns onyx.doc-test
  (:require [taoensso.timbre :refer [info] :as timbre]
            [clojure.test :refer [deftest is testing]]
            [onyx.static.default-vals :refer [defaults]]
            [onyx.information-model :refer [model]]))

(deftest missing-documentation-test
  (is (empty? (remove (apply merge (map :model (vals model))) 
                      (keys defaults))))) 
