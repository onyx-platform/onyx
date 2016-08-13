(ns onyx.log.curator-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.system :as system]
            [onyx.extensions :as extensions]
            [onyx.test-helper :refer [load-config with-test-env]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.log.curator :as cu]
            [onyx.compression.nippy :refer [zookeeper-compress zookeeper-decompress]]
            [taoensso.timbre :refer [fatal error warn trace info]]
            [clojure.test :refer [deftest is testing]]
            [onyx.api]))

(deftest curator-tests 
  (let [onyx-id (random-uuid)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id onyx-id)
        base-path (str "/" onyx-id "/ab")
        base-path2 (str "/" onyx-id "/ab2")
        env (onyx.api/start-env env-config)]
    (try
      (let [client (cu/connect (:zookeeper/address env-config) "onyx")
            value [1 3 48]]

        (cu/create client base-path :data (into-array Byte/TYPE value))

        (testing "Value is written and can be read"
          (is (= value (into [] (:data (cu/data client base-path))))))
        (cu/close client)

        (let [client2 (cu/connect (:zookeeper/address env-config) "onyx")
              watcher-sentinel (atom 0)]
          (testing "Test default ephemerality from previous test"
            (is (thrown? Exception (into [] (:data (cu/data client2 base-path))))))

          ;; write out some sequential values with parent
          (cu/create-all client2 (str base-path "/zd/hi/entry-") :sequential? true :persistent? true)
          (cu/create-all client2 (str base-path "/zd/hi/entry-") :sequential? true)
          (cu/create client2 (str base-path "/zd/hi/entry-") :sequential? true :persistent? true)
          (cu/create client2 (str base-path "/zd/hi/entry-") :sequential? true)

          (testing "Check sequential children can be found"
            (is (= (sort ["entry-0000000000"  "entry-0000000001"  "entry-0000000002"  "entry-0000000003"])
                   (sort (cu/children client2 (str base-path "/zd/hi") :watcher (fn [_] (swap! watcher-sentinel inc)))))))

          ;; add another child so watcher will be triggered
          (cu/create client2 (str base-path "/zd/hi/entry-") :sequential? true :persistent? true)

          ;; Give it a second before checking watch
          (Thread/sleep 1000)

          (testing "Check watcher triggered"
            (is (= 1 @watcher-sentinel)))

          (cu/close client2)

          (let [client3 (cu/connect (:zookeeper/address env-config) "onyx")]
            (testing "Check only sequential persistent children remain"
              (is (= (sort ["entry-0000000000" "entry-0000000002" "entry-0000000004"]) 
                     (sort (cu/children client3 (str base-path "/zd/hi"))))))

            (cu/create client3 base-path2 :data (into-array Byte/TYPE value) :persistent? true)

            (testing "Check exists after add"
              (is (= 0 (:aversion (cu/exists client3 base-path2)))))

            (cu/delete client3 base-path2)

            (testing "Deleted value"
              (is
                (thrown? Exception (cu/data client3 base-path2))))

            (testing "Check exists after delete"
              (is (= nil (cu/exists client3 base-path2))))

            (cu/close client3))))
      (finally
        (onyx.api/shutdown-env env)))))
