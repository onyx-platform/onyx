(ns onyx.messaging.messaging-gen-test
  (:require [onyx.messaging.netty-tcp :as netty :refer [netty-tcp-sockets]]
            [onyx.messaging.aeron :as aeron :refer [aeron]]
            [onyx.messaging.protocol-netty :as protocol]
            [onyx.messaging.protocol-aeron :as protocol-aeron]
            [clojure.set :refer [intersection]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test :refer :all]
            [onyx.system :as system]
            [onyx.extensions :as ext]
            [onyx.api]
            [onyx.types :refer [map->Leaf map->Ack]]
            [com.stuartsierra.component :as component]
            [onyx.test-helper :refer [load-config]]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]])
  (:import [io.netty.buffer ByteBuf]
             [io.netty.channel Channel ChannelOption ChannelFuture ChannelInitializer ChannelPipeline
              MultithreadEventLoopGroup ChannelHandler ChannelHandlerContext ChannelInboundHandlerAdapter]))

(comment (def id (java.util.UUID/randomUUID))

(def config (load-config))

(def peer-config (assoc (:peer-config config) :onyx/id id))

(defn create-server-handler-mock
  [received]
  (fn [messenger _ _ _] 
    (fn [^ChannelHandlerContext ctx ^ByteBuf buf]
      (try 
        (let [t ^byte (protocol/read-msg-type buf)]
          (cond (= t protocol/messages-type-id) 
                (doseq [message (protocol/read-messages-buf (:decompress-f messenger) buf)]
                  (swap! received update-in [:messages] conj message))

                (= t protocol/ack-type-id)
                (swap! received update-in [:acks] into (protocol/read-acks-buf buf))

                (= t protocol/completion-type-id)
                (swap! received update-in [:complete] conj (protocol/read-completion-buf buf))

                (= t protocol/retry-type-id)
                (swap! received update-in [:retry] conj (protocol/read-retry-buf buf))

                :else
                (throw (ex-info "Unexpected message received from Netty" {:type t}))))
        (catch Throwable e
          (taoensso.timbre/error e)
          (throw e))))))


(defn test-send-commands [peer-group commands]
  (let [received (atom {})]
    (with-redefs [netty/create-server-handler (create-server-handler-mock received)] 

      (let [server-port 53001
            recv-messenger (component/start (netty-tcp-sockets peer-group))
            send-messenger (component/start (netty-tcp-sockets peer-group))]

        (try 
          (let [_ (ext/open-peer-site recv-messenger {:onyx.messaging/bind-addr "127.0.0.1" 
                                                      :netty/port server-port})
                send-link (ext/connect-to-peer send-messenger nil {:netty/external-addr "127.0.0.1"
                                                                   :netty/port server-port})]
            (reduce (fn [_ command] 
                      (case (:command command)
                        :messages (ext/send-messages send-messenger nil send-link (:payload command))              
                        :complete (ext/internal-complete-message send-messenger nil (:payload command) send-link)              
                        :retry (ext/internal-retry-message send-messenger nil (:payload command) send-link)              
                        :acks (ext/internal-ack-messages send-messenger nil send-link (:payload command)))) 
                    nil commands)

            (Thread/sleep 1000)
            (ext/close-peer-connection send-messenger nil send-link)
            @received)
          (finally 
            (component/stop recv-messenger)
            (component/stop send-messenger)))))))

(def gen-segment 
  (gen/hash-map :some-key gen/any-printable))

(def uuid-gen
  (gen/fmap (fn [_]
              (java.util.UUID/randomUUID))
            (gen/return nil)))

(def gen-messages
  (gen/hash-map :command 
                (gen/return :messages) 
                :payload 
                (gen/such-that not-empty 
                               (gen/vector 
                                 (gen/fmap 
                                   map->Leaf 
                                   (gen/hash-map 
                                     :id uuid-gen
                                     :acker-id uuid-gen
                                     :completion-id uuid-gen
                                     :message gen-segment
                                     :ack-val gen/int))))))

(def gen-acks
  (gen/hash-map :command 
                (gen/return :acks) 
                :payload 
                (gen/such-that not-empty 
                               (gen/vector 
                                 (gen/fmap
                                   map->Ack
                                   (gen/hash-map 
                                     :id uuid-gen
                                     :completion-id uuid-gen
                                     :ack-val gen/int))))))

(def gen-completion
  (gen/hash-map :command 
                (gen/return :complete) 
                :payload 
                uuid-gen))

(def gen-retry
  (gen/hash-map :command 
                (gen/return :retry) 
                :payload 
                uuid-gen))

(def gen-command
  (gen/one-of [gen-messages gen-acks gen-completion gen-retry]))

#_(deftest messenger-gen-test 
  (let [peer-group (onyx.api/start-peer-group peer-config)] 
    (try 
      (checking "all generated messages are received"
                (times 10)
                [commands (gen/vector gen-command)]
                (let [grouped-model (group-by :command commands) 
                      model-results (zipmap (keys grouped-model)
                                            (map (fn [v] (set (flatten (map :payload v)))) 
                                                 (vals grouped-model)))] 
                  (is 
                    (= model-results
                       (into {} 
                             (map (juxt key (comp set val)) 
                                  (test-send-commands peer-group commands)))))))

      (finally (onyx.api/shutdown-peer-group peer-group)))))


(defn handle-sent-message [received] 
  (fn [inbound-ch decompress-f buffer offset length header]
    (let [messages (protocol-aeron/read-messages-buf decompress-f buffer offset length)]
      (swap! received update-in [:messages] into messages))))

(defn handle-aux-message [received] 
  (fn [daemon release-ch retry-ch buffer offset length header]
    (let [msg-type (protocol-aeron/read-message-type buffer offset)
          offset-rest (long (inc offset))] 
      (cond (= msg-type protocol-aeron/ack-msg-id)
            (let [ack (protocol-aeron/read-acker-message buffer offset-rest)]
              (swap! received update-in [:acks] conj ack))

            (= msg-type protocol-aeron/completion-msg-id)
            (let [completion-id (protocol-aeron/read-completion buffer offset-rest)]
              (swap! received update-in [:complete] conj completion-id))

            (= msg-type protocol-aeron/retry-msg-id)
            (let [retry-id (protocol-aeron/read-retry buffer offset-rest)]
              (swap! received update-in [:retry] conj retry-id))))))


(defn test-send-commands-aeron [peer-group commands]
    (let [received (atom {})]
      (with-redefs [aeron/handle-sent-message (handle-sent-message received)
                    aeron/handle-aux-message (handle-aux-message received)] 
        (let [server-port 53001
              recv-messenger (component/start (aeron peer-group))
              send-messenger (component/start (aeron peer-group))]

          (try 
            (let [_ (ext/open-peer-site recv-messenger {:onyx.messaging/bind-addr "127.0.0.1" 
                                                        :aeron/port server-port})
                  send-link (ext/connect-to-peer send-messenger nil {:aeron/external-addr "127.0.0.1"
                                                                     :aeron/port server-port})]
              (reduce (fn [_ command] 
                        (case (:command command)
                          :messages (ext/send-messages send-messenger nil send-link (:payload command))              
                          :complete (ext/internal-complete-message send-messenger nil (:payload command) send-link)              
                          :retry (ext/internal-retry-message send-messenger nil (:payload command) send-link)              
                          :acks (ext/internal-ack-messages send-messenger nil send-link (:payload command)))) 
                      nil commands)

              (Thread/sleep 1500)
              (ext/close-peer-connection send-messenger nil send-link)
              @received)
            (finally 
              (component/stop recv-messenger)
              (component/stop send-messenger)))))))

(def peer-config-aeron (assoc (:peer-config config) :onyx/id id :onyx.messaging/impl :aeron))

#_(deftest aeron-gen-test 
  (let [peer-group (onyx.api/start-peer-group peer-config-aeron)] 
    (try 
      (checking "all generated messages are received"
                (times 10)
                [commands (gen/vector gen-command)]
                (let [grouped-model (group-by :command commands) 
                      model-results (zipmap (keys grouped-model)
                                            (map (fn [v] (set (flatten (map :payload v)))) 
                                                 (vals grouped-model)))] 
                  (is 
                    (= model-results
                       (into {} 
                             (map (juxt key (comp set val)) 
                                  (test-send-commands-aeron peer-group commands)))))))

      (finally (onyx.api/shutdown-peer-group peer-group))))))
