(ns onyx.log.curator-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.system :as system]
            [onyx.extensions :as extensions]
            [onyx.messaging.dummy-messenger]
            [onyx.test-helper :refer [load-config]]
            [onyx.log.curator :as cu]
            [onyx.compression.nippy :refer [compress decompress]]
            [taoensso.timbre :refer [fatal error warn trace info]]
            [midje.sweet :refer :all]
            [onyx.api]))

(def onyx-id (java.util.UUID/randomUUID))

(def config (load-config))

(def env-config
  (assoc (:env-config config) :onyx/id onyx-id))

(def base-path (str "/" onyx-id "/ab"))

(def base-path2 (str "/" onyx-id "/ab2"))

#_(let [env (onyx.api/start-env env-config)]
  (try
    (let [client (cu/connect (:zookeeper/address env-config) "onyx")
          value [1 3 48]]

      (cu/create client base-path :data (into-array Byte/TYPE value))

      (facts "Value is written and can be read"
             (fact (into [] (:data (cu/data client base-path))) => value))
      (cu/close client)

      (let [client2 (cu/connect (:zookeeper/address env-config) "onyx")
            watcher-sentinel (atom 0)]
        (facts "Test default ephemerality from previous test"
               (fact (into [] (:data (cu/data client2 base-path))) => (throws Exception)))

        ;; write out some sequential values with parent
        (cu/create-all client2 (str base-path "/zd/hi/entry-") :sequential? true :persistent? true)
        (cu/create-all client2 (str base-path "/zd/hi/entry-") :sequential? true)
        (cu/create client2 (str base-path "/zd/hi/entry-") :sequential? true :persistent? true)
        (cu/create client2 (str base-path "/zd/hi/entry-") :sequential? true)

        (facts "Check sequential children can be found"
               (fact
                (sort (cu/children client2 (str base-path "/zd/hi") :watcher (fn [_] (swap! watcher-sentinel inc))))
                =>
                (sort ["entry-0000000000"  "entry-0000000001"  "entry-0000000002"  "entry-0000000003"])))

        ;; add another child so watcher will be triggered
        (cu/create client2 (str base-path "/zd/hi/entry-") :sequential? true :persistent? true)

        ;; Give it a second before checking watch
        (Thread/sleep 1000)

        (facts "Check watcher triggered"
               (fact @watcher-sentinel => 1))

        (cu/close client2)

        (let [client3 (cu/connect (:zookeeper/address env-config) "onyx")]
          (facts "Check only sequential persistent children remain"
                 (fact
                  (sort (cu/children client3 (str base-path "/zd/hi"))) =>
                  (sort ["entry-0000000000" "entry-0000000002" "entry-0000000004"])))

          (cu/create client3 base-path2 :data (into-array Byte/TYPE value) :persistent? true)

          (facts "Check exists after add"
                 (fact (:aversion (cu/exists client3 base-path2)) => 0))

          (cu/delete client3 base-path2)

          (facts "Deleted value"
                 (fact
                  (cu/data client3 base-path2) => (throws Exception)))

          (facts "Check exists after delete"
                 (fact (cu/exists client3 base-path2) => nil))

          (cu/close client3))))
    (finally
     (onyx.api/shutdown-env env))))


(let [env (onyx.api/start-env env-config)]
  (try
    (let [client (cu/connect (:zookeeper/address env-config) "onyx")]
      (cu/create client base-path :data (compress {:v 0}))
      (cu/swap-data client base-path compress decompress (fn [v] (update v :v inc)))
      (cu/swap-data client base-path compress decompress (fn [v] (update v :v inc)))
      (let [last-val (cu/swap-data client base-path compress decompress (fn [v] (update v :v inc)))]
        (fact {:v 3} => last-val)))
    (finally
     (onyx.api/shutdown-env env))))

(let [env (onyx.api/start-env env-config)]
  (try
    (let [client (cu/connect (:zookeeper/address env-config) "onyx")]
      (cu/create client base-path :data (compress {:v 0}))
      (facts "Concurrency test" 
             (let [n-updates 1000
                   updates (pmap 
                             (fn [_]
                               (cu/swap-data client base-path compress decompress 
                                             (fn [v] (update v :v inc))))
                             
                             (range n-updates))
                   max-val (apply max (map :v updates))]
               (fact max-val => n-updates))))
    (finally
      (onyx.api/shutdown-env env))))
