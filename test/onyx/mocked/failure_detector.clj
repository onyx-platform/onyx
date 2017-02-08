(ns onyx.mocked.failure-detector
  (:require [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info error warn fatal]]))

(defrecord FakeFailureDetector []
  component/Lifecycle
  (start [component] component)
  (stop [component] component))

(defn failure-detector [_ _ _]
  (map->FakeFailureDetector {}))
