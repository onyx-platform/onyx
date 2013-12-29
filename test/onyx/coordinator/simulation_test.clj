(ns onyx.coordinator.simulation-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan tap close! >!! <!!]]
            [onyx.coordinator.async :as async]
            [onyx.coordinator.extensions :as extensions]
            [onyx.coordinator.log.datomic :as datomic]
            [onyx.util :as u]
            [datomic.api :as d]))

(let [peer (extensions/create :zookeeper :peer)
      offer-ch-interceptor (chan 1)]
  (tap async/offer-mult offer-ch-interceptor)
  (>!! async/born-peer-ch-head peer)
  (let [result (<!! offer-ch-interceptor)]
    (d/q '[:find ?e :where [?e :peer/place]] (d/db @datomic/datomic-conn))
    (close! offer-ch-interceptor)))

