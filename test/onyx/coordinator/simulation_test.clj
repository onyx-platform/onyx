(ns onyx.coordinator.simulation-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan tap close! >!! <!!]]
            [onyx.coordinator.async :as async]
            [onyx.coordinator.extensions :as extensions]
            [onyx.coordinator.log.datomic :as datomic]
            [onyx.util :as u]
            [datomic.api :as d]))

(defn sandbox-db []
  (datomic/start-datomic!
   (str "datomic:mem://" (java.util.UUID/randomUUID))
   (datomic/log-schema)))

(deftest new-peer
  (with-redefs [datomic/datomic-conn (delay (sandbox-db))]
    (let [peer (extensions/create :zookeeper :peer)
          offer-ch-interceptor (chan 1)]
      (tap async/offer-mult offer-ch-interceptor)
      (>!! async/born-peer-ch-head peer)
      (let [_ (<!! offer-ch-interceptor)
            query '[:find ?p :where [?e :peer/place ?p]]
            result (d/q query (d/db @datomic/datomic-conn))]
        (is (= (count result) 1))
        (is (= (ffirst result) peer))
        (close! offer-ch-interceptor)))))

(run-tests)

