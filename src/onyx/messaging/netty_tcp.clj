(ns ^:no-doc onyx.messaging.netty-tcp
    (:require [clojure.core.async :refer [chan >!! >! <!! alts!! timeout close! go-loop]]
              [com.stuartsierra.component :as component]
              [org.httpkit.server :as server]
              [taoensso.timbre :as timbre]
              [gloss.io :as io]
              [onyx.messaging.acking-daemon :as acker]
              [onyx.messaging.protocol :as protocol]
              [onyx.compression.nippy :refer [compress decompress]]
              [onyx.extensions :as extensions]
              [manifold.deferred :as d]
              [manifold.stream :as s]
              [gloss.io :as io]
              [clojure.edn :as edn])
    (:import [java.net InetSocketAddress]
             [java.util.concurrent Executors]
             [io.netty.channel.epoll 
              Epoll 
              EpollEventLoopGroup
              EpollServerSocketChannel
              EpollSocketChannel]
             [java.util.concurrent TimeUnit Executors]
             [io.netty.buffer ByteBuf Unpooled UnpooledByteBufAllocator ByteBufAllocator]
             [java.nio ByteBuffer]
             [io.netty.channel.socket.nio 
              NioServerSocketChannel
              NioSocketChannel]
             [io.netty.util.internal SystemPropertyUtil]
             [io.netty.util.concurrent 
              Future
              EventExecutorGroup
              DefaultEventExecutorGroup
              ImmediateEventExecutor]
             [io.netty.channel.nio NioEventLoopGroup]
             [io.netty.channel.socket SocketChannel]
             [io.netty.channel
              Channel 
              ChannelFuture 
              ChannelOption
              ChannelPipeline 
              EventLoopGroup
              ChannelInitializer
              ChannelHandler
              ChannelInboundHandler
              ChannelOutboundHandler
              ChannelHandlerContext
              ChannelFutureListener
              ChannelInboundHandlerAdapter]
             [io.netty.handler.codec
              LengthFieldBasedFrameDecoder
              LengthFieldPrepender]
             [io.netty.channel.group 
              ChannelGroup
              DefaultChannelGroup]
             [io.netty.util.concurrent 
              GenericFutureListener 
              Future 
              DefaultThreadFactory]
             [io.netty.bootstrap Bootstrap ServerBootstrap]
             [io.netty.channel.socket.nio NioServerSocketChannel]
             [org.jboss.netty.buffer ChannelBuffers]))

(def protocol protocol/codec-protocol)

(defn epoll? []
  (Epoll/isAvailable))

(defn gen-tcp-handler
  [^ChannelGroup channel-group handler]
  (proxy [ChannelInboundHandlerAdapter] []
    (channelActive [ctx]
      (timbre/info "Channel active")
      (.add channel-group (.channel ctx)))
    (channelRead [^ChannelHandlerContext ctx ^Object message]
      (try
        (timbre/info "Channel read " message)
        (handler ctx message)
        (catch java.nio.channels.ClosedChannelException e
          (timbre/warn "channel closed"))))
    (exceptionCaught [^ChannelHandlerContext ctx ^Throwable cause]
      (println cause "TCP handler caught")
      (.close (.channel ctx)))
    (isSharable [] true)))

(defn event-executor
  "Creates a new netty execution handler for processing events. Defaults to 1
  thread per core."
  []
  (DefaultEventExecutorGroup. (.. Runtime getRuntime availableProcessors)))

(defonce ^DefaultEventExecutorGroup shared-event-executor
  (event-executor))

(defn int32-frame-decoder []
    ; Offset 0, 4 byte header, skip those 4 bytes.
    (LengthFieldBasedFrameDecoder. Integer/MAX_VALUE, 0, 4, 0, 4))

(defn int32-frame-encoder []
  (LengthFieldPrepender. 4))

(defn channel-initializer-done [handler]
  (proxy [ChannelInitializer] []
    (initChannel [ch]
      (let [pipeline (.pipeline ^Channel ch)]
        (doto pipeline 
          ;(.addLast pipeline-name nil "msg-encoder" msg-encoder)
          ; These could be shared?
          ;(.addLast nil "int-32-frame-decoder" (int32-frame-decoder))
          ;(.addLast nil "int-32-frame-encoder" (int32-frame-encoder))
          (.addLast shared-event-executor "handler" handler))))))

(defn handle [msg]
  ; FIXME: should read as many messages as possible 
  ; here given the number of bytes
  (timbre/info "Handling message " msg)
  (if-let [decompressed (protocol/read-buf msg)]
    (timbre/info "Read message " decompressed)
    (.resetReaderIndex msg))

  (timbre/info "Message now looks like: " msg)
  msg)

(defn tcp-handler
  "Given a core, a channel, and a message, applies the message to core and
  writes a response back on this channel."
  [^ChannelHandlerContext ctx ^Object message]
  (timbre/info "TCP HANDLER MESSAGE " message)

  (.. ctx
      ; Actually handle request
      (writeAndFlush (handle message))))

(def netty-implementation
  "Provide native implementation of Netty for improved performance on
  Linux only. Provide pure-Java implementation of Netty on all other
  platforms. See http://netty.io/wiki/native-transports.html"
  (if (and (.contains (. System getProperty "os.name") "Linux")
           (.contains (. System getProperty "os.arch") "amd64"))
    {:event-loop-group-fn #(EpollEventLoopGroup.)
     :channel EpollServerSocketChannel}
    {:event-loop-group-fn #(NioEventLoopGroup.)
     :channel NioServerSocketChannel}))

(defn derefable
  "A simple wrapper for a netty future which on deref just calls
  (syncUninterruptibly f), and returns the future's result."
  [^Future f]
  (reify clojure.lang.IDeref
    (deref [_]
      (.syncUninterruptibly f)
      (.get f))))

(defn ^Future shutdown-event-executor-group
  "Gracefully shut down an event executor group. Returns a derefable future."
  [^EventExecutorGroup g]
  ; 10ms quiet period, 10s timeout.
  (derefable (.shutdownGracefully g 10 1000 TimeUnit/MILLISECONDS)))

(defn start-netty-server
  [host port]
  (let [bootstrap (ServerBootstrap.)
        event-loop-group-fn (:event-loop-group-fn netty-implementation)
        boss-group (event-loop-group-fn)
        worker-group (event-loop-group-fn)
        channel-group (DefaultChannelGroup. (str "tcp-server " host ":" port)
                                            (ImmediateEventExecutor/INSTANCE))
        initializer (channel-initializer-done (gen-tcp-handler channel-group tcp-handler))] 
    ; Configure bootstrap
    (doto bootstrap
      (.group boss-group worker-group)
      (.channel (:channel netty-implementation))
      (.option ChannelOption/SO_REUSEADDR true)
      (.option ChannelOption/TCP_NODELAY true)
      (.childOption ChannelOption/SO_REUSEADDR true)
      (.childOption ChannelOption/TCP_NODELAY true)
      (.childOption ChannelOption/SO_KEEPALIVE true)
      (.childHandler initializer))

    ; Start bootstrap
    (->> (InetSocketAddress. host port)
         (.bind bootstrap)
         (.sync)
         (.channel)
         (.add channel-group))

    (timbre/info "TCP server" host port "online")

    (fn killer []
      (.. channel-group close awaitUninterruptibly)
      ; Shut down workers and boss concurrently.
      (let [w (shutdown-event-executor-group worker-group)
            b (shutdown-event-executor-group boss-group)]
        @w
        @b)
      (timbre/info "TCP server" host port "shut down"))))

(defn tcp-client-handler [])

(defn client-handler [msg]
  (timbre/info "TCP client handler"))

; (defn read-adaptor [handler]
;   (proxy [ChannelInboundHandlerAdapter] []
;     (channelRead [^ChannelHandlerContext ctx ^Object msg]
;       (timbre/info "Channel read?" msg)
;       )
;     (exceptionCaught 
;       [^ChannelHandlerContext ctx ^Throwable cause]
;       (timbre/error "ChannelInboundHandlerAdapter: " cause)
;       (.close ctx))))

; (defn new-handler []
;   (proxy [ChannelInboundByteHandlerAdapter] []
;     (inboundBufferUpdated [context, in]
;       (.clear in))
;     (exceptionCaught [context, cause]
;       (.printStackTrace cause)
;       (.close context))))

(defn new-client-handler []
  (proxy [ChannelHandler] []
    (handlerAdded [ctx]
      (timbre/info "Handler added"))
    (handlerRemoved [ctx]
       (timbre/info "Handler removed"))
    (exceptionCaught [context cause]
      (.printStackTrace cause)
      (.close context))))

(defn client-channel-initializer [handler]
  (proxy [ChannelInitializer] []
    (initChannel [ch]
      (timbre/info "Initializing client channel")
      (try (let [pipeline (.pipeline ^Channel ch)]
             (doto pipeline 
               (.addLast handler)))
           (catch Exception e
             (timbre/fatal e))))))

(defn create-client [host port]
  (let [channel (if (epoll?)
                  EpollSocketChannel
                  NioSocketChannel)
        ; replace?
        worker-group (NioEventLoopGroup.)]
    (try
      (let [b (doto (Bootstrap.)
                (.option ChannelOption/SO_REUSEADDR true)
                (.option ChannelOption/MAX_MESSAGES_PER_READ Integer/MAX_VALUE)
                (.group worker-group)
                (.channel channel)
                (.handler (client-channel-initializer (new-client-handler))))]
        (.channel ^ChannelFuture (.connect b host port))))))

; (def client 
;   (create-client "127.0.0.1" 10009))
; (.remoteAddress client)
; (.isActive client)
; (.writeAndFlush client
;                 (protocol/write-msg {:type 0 
;                                      :messages '({:id #uuid  "ac39bc62-8f06-46a0-945e-3a17642a619f"
;                                                   :acker-id #uuid  "11837bd7-2de5-4b62-888d-171c4c47845c"
;                                                   :completion-id #uuid  "b57f7be1-f2f9-4d0f-aa02-939b3d48dc23"
;                                                   :message {:n 1}
;                                                   :ack-val 8892143382010058362}
;                                                  {:id #uuid "010a1688-47ff-4055-8da5-1f02247351e1"
;                                                   :acker-id #uuid "bf8fd5fc-30fd-424c-af6a-0b32568581a4"
;                                                   :completion-id #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
;                                                   :message {:n 2}
;                                                   :ack-val 729233382010058362}
;                                                  {:id #uuid  "ac39bc62-8f06-46a0-945e-3a17642a619f"
;                                                   :acker-id #uuid  "11837bd7-2de5-4b62-888d-171c4c47845c"
;                                                   :completion-id #uuid  "b57f7be1-f2f9-4d0f-aa02-939b3d48dc23"
;                                                   :message {:n 1}
;                                                   :ack-val 8892143382010058362}
;                                                  {:id #uuid "010a1688-47ff-4055-8da5-1f02247351e1"
;                                                   :acker-id #uuid "bf8fd5fc-30fd-424c-af6a-0b32568581a4"
;                                                   :completion-id #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
;                                                   :message {:n 2}
;                                                   :ack-val 729233382010058362}
;                                                  {:id #uuid  "ac39bc62-8f06-46a0-945e-3a17642a619f"
;                                                   :acker-id #uuid  "11837bd7-2de5-4b62-888d-171c4c47845c"
;                                                   :completion-id #uuid  "b57f7be1-f2f9-4d0f-aa02-939b3d48dc23"
;                                                   :message {:n 1}
;                                                   :ack-val 8892143382010058362}
;                                                  {:id #uuid "010a1688-47ff-4055-8da5-1f02247351e1"
;                                                   :acker-id #uuid "bf8fd5fc-30fd-424c-af6a-0b32568581a4"
;                                                   :completion-id #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
;                                                   :message {:n 2}
;                                                   :ack-val 729233382010058362}
;                                                  {:id #uuid  "ac39bc62-8f06-46a0-945e-3a17642a619f"
;                                                   :acker-id #uuid  "11837bd7-2de5-4b62-888d-171c4c47845c"
;                                                   :completion-id #uuid  "b57f7be1-f2f9-4d0f-aa02-939b3d48dc23"
;                                                   :message {:n 1}
;                                                   :ack-val 8892143382010058362}
;                                                  {:id #uuid "010a1688-47ff-4055-8da5-1f02247351e1"
;                                                   :acker-id #uuid "bf8fd5fc-30fd-424c-af6a-0b32568581a4"
;                                                   :completion-id #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
;                                                   :message {:n 2}
;                                                   :ack-val 729233382010058362}
;                                                  {:id #uuid  "ac39bc62-8f06-46a0-945e-3a17642a619f"
;                                                   :acker-id #uuid  "11837bd7-2de5-4b62-888d-171c4c47845c"
;                                                   :completion-id #uuid  "b57f7be1-f2f9-4d0f-aa02-939b3d48dc23"
;                                                   :message {:n 1}
;                                                   :ack-val 8892143382010058362}
;                                                  {:id #uuid "010a1688-47ff-4055-8da5-1f02247351e1"
;                                                   :acker-id #uuid "bf8fd5fc-30fd-424c-af6a-0b32568581a4"
;                                                   :completion-id #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
;                                                   :message {:n 2}
;                                                   :ack-val 729233382010058362}
;                                                  {:id #uuid  "ac39bc62-8f06-46a0-945e-3a17642a619f"
;                                                   :acker-id #uuid  "11837bd7-2de5-4b62-888d-171c4c47845c"
;                                                   :completion-id #uuid  "b57f7be1-f2f9-4d0f-aa02-939b3d48dc23"
;                                                   :message {:n 1}
;                                                   :ack-val 8892143382010058362}
;                                                  {:id #uuid "010a1688-47ff-4055-8da5-1f02247351e1"
;                                                   :acker-id #uuid "bf8fd5fc-30fd-424c-af6a-0b32568581a4"
;                                                   :completion-id #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
;                                                   :message {:n 2}
;                                                   :ack-val 729233382010058362}
;                                                  {:id #uuid  "ac39bc62-8f06-46a0-945e-3a17642a619f"
;                                                   :acker-id #uuid  "11837bd7-2de5-4b62-888d-171c4c47845c"
;                                                   :completion-id #uuid  "b57f7be1-f2f9-4d0f-aa02-939b3d48dc23"
;                                                   :message {:n 1}
;                                                   :ack-val 8892143382010058362}
;                                                  {:id #uuid "010a1688-47ff-4055-8da5-1f02247351e1"
;                                                   :acker-id #uuid "bf8fd5fc-30fd-424c-af6a-0b32568581a4"
;                                                   :completion-id #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
;                                                   :message {:n 2}
;                                                   :ack-val 729233382010058362}
;                                                  {:id #uuid  "ac39bc62-8f06-46a0-945e-3a17642a619f"
;                                                   :acker-id #uuid  "11837bd7-2de5-4b62-888d-171c4c47845c"
;                                                   :completion-id #uuid  "b57f7be1-f2f9-4d0f-aa02-939b3d48dc23"
;                                                   :message {:n 1}
;                                                   :ack-val 8892143382010058362}
;                                                  {:id #uuid "010a1688-47ff-4055-8da5-1f02247351e1"
;                                                   :acker-id #uuid "bf8fd5fc-30fd-424c-af6a-0b32568581a4"
;                                                   :completion-id #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
;                                                   :message {:n 2}
;                                                   :ack-val 729233382010058362})})) 

(defn app [daemon net-ch inbound-ch release-ch]
  (go-loop []
           (try (let [{:keys [type] :as msg} (<!! net-ch)]
                  (cond (= type protocol/send-id) 
                        (doseq [message (:messages msg)]
                          (>!! inbound-ch message))

                        (= type protocol/ack-id)
                        (acker/ack-message daemon
                                           (:id msg)
                                           (:completion-id msg)
                                           (:ack-val msg))

                        (= type protocol/completion-id)
                        (>!! release-ch (:id msg))))
             (catch Exception e
               (taoensso.timbre/error e)
               (throw e)))
           (recur)))

(defrecord NettyTcpSockets [opts]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Netty TCP Sockets")

    (let [net-ch (chan (clojure.core.async/dropping-buffer 1000000))
          inbound-ch (:inbound-ch (:messenger-buffer component))
          release-ch (chan (clojure.core.async/dropping-buffer 1000000))
          daemon (:acking-daemon component)
          ip "127.0.0.1"
          port (+ 5000 (rand-int 45000))
          server-shutdown-fn (start-netty-server ip port)
          app-loop (app daemon net-ch inbound-ch release-ch)]
      (assoc component 
             :app-loop app-loop 
             :server-shutdown-fn server-shutdown-fn 
             :ip ip 
             :port port 
             :release-ch release-ch)))

  (stop [component]
    (taoensso.timbre/info "Stopping Netty TCP Sockets")
    ((:server-shutdown-fn component))
    (close! (:app-loop component))
    (close! (:release-ch component))
    (assoc component :release-ch nil)))

(defn netty-tcp-sockets [opts]
  (map->NettyTcpSockets {:opts opts}))

; Not much need for these in aleph
(defmethod extensions/send-peer-site NettyTcpSockets
  [messenger]
  [(:ip messenger) (:port messenger)])

(defmethod extensions/acker-peer-site NettyTcpSockets
  [messenger]
  [(:ip messenger) (:port messenger)])

(defmethod extensions/completion-peer-site NettyTcpSockets
  [messenger]
  [(:ip messenger) (:port messenger)])

;; FIXME: these are just for client for now
(defn wrap-duplex-stream
  [protocol s]
  (let [out (s/stream)]
    (s/connect
     (s/map #(io/encode protocol %) out)
     s)
    (s/splice
     out
     (io/decode-stream s protocol))))

; Reused as is from HttpKitWebSockets. Probably something to extract.
(defmethod extensions/receive-messages NettyTcpSockets
  [messenger {:keys [onyx.core/task-map] :as event}]
  (let [ms (or (:onyx/batch-timeout task-map) 1000)
        ch (:inbound-ch (:onyx.core/messenger-buffer event))
        timeout-ch (timeout ms)]
    (loop [segments [] i 0]
      (if (< i (:onyx/batch-size task-map))
        (if-let [v (first (alts!! [ch timeout-ch]))]
          (recur (conj segments v) (inc i))
          segments)
        segments))))

(defmethod extensions/send-messages NettyTcpSockets
  [messenger event peer-link messages]
  (s/put! peer-link (protocol/send-messages->frame messages)))

(defmethod extensions/internal-ack-message NettyTcpSockets
  [messenger event peer-link message-id completion-id ack-val]
  (s/put! peer-link (protocol/ack-msg->frame {:id message-id :completion-id completion-id :ack-val ack-val})))

(defmethod extensions/internal-complete-message NettyTcpSockets
  [messenger event id peer-link]
  (s/put! peer-link (protocol/completion-msg->frame {:id id})))
