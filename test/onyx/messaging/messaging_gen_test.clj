(ns onyx.messaging.messaging-gen-test
  (:require [onyx.messaging.netty-tcp :as netty :refer [netty-tcp-sockets]]
            [onyx.messaging.protocol-netty :as protocol]
            [clojure.set :refer [intersection]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test :refer :all]
            [onyx.system :as system]
            [onyx.extensions :as ext]
            [onyx.api]
            [onyx.types :refer [map->Leaf]]
            [com.stuartsierra.component :as component]
            [onyx.test-helper :refer [load-config]]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]])
  (:import 
             [io.netty.buffer ByteBuf]
             [io.netty.channel Channel ChannelOption ChannelFuture ChannelInitializer ChannelPipeline
              MultithreadEventLoopGroup ChannelHandler ChannelHandlerContext ChannelInboundHandlerAdapter]
             )
  
  )

(def id (java.util.UUID/randomUUID))

(def config (load-config))

(def env-config (assoc (:env-config config) :onyx/id id))

(def peer-config (assoc (:peer-config config) :onyx/id id))

(defn create-server-handler-mock
  [received]
  (fn [messenger _ _ _] 
    (fn [^ChannelHandlerContext ctx ^ByteBuf buf]
      (try 
        (let [msg (protocol/read-buf (:decompress-f messenger) buf)]
          (let [t ^byte (:type msg)]
            (cond (= t protocol/messages-type-id) 
                  (doseq [message (:messages msg)]
                    (swap! received update-in [:messages] conj message))

                  (= t protocol/ack-type-id)
                  (swap! received update-in [:acks] into (:acks msg))

                  (= t protocol/completion-type-id)
                  (swap! received update-in [:complete] conj (:id msg))

                  (= t protocol/retry-type-id)
                  (swap! received update-in [:retry] conj (:id msg))

                  :else
                  (throw (ex-info "Unexpected message received from Netty" {:message msg})))))
        (catch Throwable e
          (taoensso.timbre/error e)
          (throw e))))))


(defn test-send-commands [env peer-group commands]
  (let [received (atom {})]
    (with-redefs [netty/create-server-handler (create-server-handler-mock received)] 
      (let [peers (onyx.api/start-peers 2 peer-group)] 
        (try 
          (let [server-port 53001
                _ (while (and (nil? @(:started-peer (first peers)))
                              (nil? @(:started-peer (second peers))))
                    (println "Waiting for full startup"))
                recv-peer @(:started-peer (first peers))
                send-peer @(:started-peer (second peers))
                recv-messenger (:messenger recv-peer)]
            (let [_ (ext/open-peer-site recv-messenger {:onyx.messaging/bind-addr "127.0.0.1" 
                                                        :netty/port server-port})
                  send-messenger (:messenger send-peer)
                  send-link (ext/connect-to-peer send-messenger nil {:netty/external-addr "127.0.0.1"
                                                                     :netty/port server-port})]
              (reduce (fn [_ command] 
                        (case (:command command)
                          :messages (ext/send-messages send-messenger nil send-link (:payload command))              
                          :complete (ext/internal-complete-message send-messenger nil (:payload command) send-link)              
                          :retry (ext/internal-retry-message send-messenger nil (:payload command) send-link)              
                          :acks (ext/internal-ack-messages send-messenger nil send-link (:payload command)))) 
                      nil commands)
              (Thread/sleep 3000)
              @received))
          (finally 
            (doseq [peer peers] 
              (onyx.api/shutdown-peer peer))))))))


(deftest start-on-messenger-test 
  (let [env (onyx.api/start-env env-config)
        peer-group (onyx.api/start-peer-group peer-config)
        commands [{:command :messages 
                   :payload [(map->Leaf {:id #uuid "010a1688-47ff-4055-8da5-1f02247351e1"
                                         :acker-id #uuid "bf8fd5fc-30fd-424c-af6a-0b32568581a4"
                                         :completion-id #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
                                         :message {}
                                         :ack-val 729233382010058362})]}
                  {:command :retry
                   :payload #uuid "8665d0eb-7905-4a5f-8704-28f7e67d369c"}

                  {:command :complete
                   :payload #uuid "2982b77a-849e-4964-8020-a2cee50756e7"}

                  {:command :acks
                   :payload [{:id #uuid "e2ba38dd-b523-4e63-ba74-645fb91c231a" 
                             :completion-id #uuid "b57f7be1-f2f9-4d0f-aa02-939b3d48dc23"
                             :ack-val 3323130347050513529}]}]
        grouped-model (group-by :command commands) 
        model-results (zipmap (keys grouped-model)
                              (map (fn [v] (sort (flatten (map :payload v)))) 
                                   (vals grouped-model)))] 
    (try 
      (is 
        (= model-results
           (into {} 
                 (map (juxt key (comp sort val)) 
                      (test-send-commands env peer-group commands)))))
      (finally (onyx.api/shutdown-peer-group peer-group)
               (onyx.api/shutdown-env env)))))


