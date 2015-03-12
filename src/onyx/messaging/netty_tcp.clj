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
             [io.netty.buffer ByteBuf Unpooled UnpooledByteBufAllocator ByteBufAllocator CompositeByteBuf]
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
             [io.netty.handler.codec.protobuf 
              ProtobufDecoder
              ProtobufEncoder]  
             [io.netty.bootstrap Bootstrap ServerBootstrap]
             [io.netty.channel.socket.nio NioServerSocketChannel]
             [org.jboss.netty.buffer ChannelBuffers]))

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
      (timbre/error cause "TCP handler caught")
      (.close (.channel ctx)))
    (isSharable [] true)))

(defn event-executor
  "Creates a new netty execution handler for processing events. Defaults to 1
  thread per core."
  []
  (DefaultEventExecutorGroup. (.. Runtime getRuntime availableProcessors)))

(defonce ^DefaultEventExecutorGroup shared-event-executor
  (event-executor))

(defn channel-initializer-done [handler]
  (proxy [ChannelInitializer] []
    (initChannel [ch]
      (let [pipeline (.pipeline ^Channel ch)]
        (doto pipeline 
          (.addLast shared-event-executor "handler" handler))))))

(defn add-component! [^CompositeByteBuf composite buf]
  (timbre/info "REFCOUNTS: " (.refCnt composite) (.refCnt buf))
  ;; FIXME THIS IS PROBABLY BAD!!!!
  ;(.retain buf)
  (.addComponent composite buf)
  (.writerIndex composite (+ ^int (.writerIndex buf)
                             ^int (.writerIndex composite))))

(defn handle [inbound-ch ^CompositeByteBuf composite-buf msg]
  (timbre/info "Handling message " msg " comp buf " composite-buf)
  (add-component! composite-buf msg)
  (timbre/info "Added composite buf " composite-buf)
  ; FIXME: should read as many messages as possible 
  ; here given the number of bytes
  (.markReaderIndex composite-buf)
  (if-let [decompressed (protocol/read-buf composite-buf)]
    (do (timbre/info "Read message " decompressed)
        (>!! inbound-ch decompressed)
        (.discardReadComponents composite-buf))
    (do (timbre/info "Can't perform a full read: " composite-buf)
        (.resetReaderIndex composite-buf)
        (timbre/info "Reset index: " composite-buf)))
  nil)

(defn new-composite-buffer [] 
  (timbre/info "creating new composite buffer")
  (.retain (.compositeBuffer (UnpooledByteBufAllocator. false))))

(defn create-server-handler
  "Given a core, a channel, and a message, applies the message to 
  core and writes a response back on this channel."
  [net-recvd-ch composite-buf] 
  (timbre/info "CREATED SERVER HANDLER")
  (fn [^ChannelHandlerContext ctx ^Object message]
    (timbre/info "TCP HANDLER MESSAGE " message)
    ; Actually handle request and reply if any
    (when-let [response (handle net-recvd-ch composite-buf message)]
      (.writeAndFlush ctx response))))

; "Provide native implementation of Netty for improved performance on
; Linux only. Provide pure-Java implementation of Netty on all other
; platforms. See http://netty.io/wiki/native-transports.html"
(def netty-implementation
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
  [host port net-recvd-ch]
  (let [bootstrap (ServerBootstrap.)
        event-loop-group-fn (:event-loop-group-fn netty-implementation)
        boss-group (event-loop-group-fn)
        worker-group (event-loop-group-fn)
        channel-group (DefaultChannelGroup. (str "tcp-server " host ":" port)
                                            (ImmediateEventExecutor/INSTANCE))
        composite-buf (.retain (new-composite-buffer))
        initializer (channel-initializer-done (gen-tcp-handler channel-group (create-server-handler net-recvd-ch composite-buf)))] 
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
      (.release composite-buf)
      (.. channel-group close awaitUninterruptibly)
      ; Shut down workers and boss concurrently.
      (let [w (shutdown-event-executor-group worker-group)
            b (shutdown-event-executor-group boss-group)]
        @w
        @b)
      (timbre/info "TCP server" host port "shut down"))))

(defn client-handler [msg]
  (timbre/info "TCP client handler"))

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
               (.addLast "handler" handler)))
           (catch Exception e
             (timbre/fatal e))))))

(defn create-client [host port]
  (let [channel (if (epoll?)
                  EpollSocketChannel
                  NioSocketChannel)
        ; replace with faster group - i.e. epoll case?
        worker-group (NioEventLoopGroup.)]
    (try
      (let [b (doto (Bootstrap.)
                (.option ChannelOption/SO_REUSEADDR true)
                (.option ChannelOption/MAX_MESSAGES_PER_READ Integer/MAX_VALUE)
                (.group worker-group)
                (.channel channel)
                (.handler (client-channel-initializer (new-client-handler))))]
        (.channel ^ChannelFuture (.connect b host port))))))

(defn app [daemon net-ch inbound-ch release-ch]
  (go-loop []
           (try (let [msg (<!! net-ch)
                      t ^byte (:type msg)]
                  (cond (= t protocol/messages-type-id) 
                        (doseq [message (:messages msg)]
                          (>!! inbound-ch message))

                        (= t protocol/ack-type-id)
                        (acker/ack-message daemon
                                           (:id msg)
                                           (:completion-id msg)
                                           (:ack-val msg))

                        (= t protocol/completion-type-id)
                        (>!! release-ch (:id msg))))
             (catch Exception e
               (taoensso.timbre/error e)
               (throw e)))
           (recur)))

(defrecord NettyTcpSockets [opts]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Netty TCP Sockets")

    (let [net-recvd-ch (chan (clojure.core.async/dropping-buffer 1000000))
          inbound-ch (:inbound-ch (:messenger-buffer component))
          release-ch (chan (clojure.core.async/dropping-buffer 1000000))
          daemon (:acking-daemon component)
          ip "127.0.0.1"
          port (+ 5000 (rand-int 45000))
          server-shutdown-fn (start-netty-server ip port net-recvd-ch)
          app-loop (app daemon net-recvd-ch inbound-ch release-ch)]
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

(defmethod extensions/connect-to-peer NettyTcpSockets
  [messenger event [host port]]
  (create-client host port))

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
  [messenger event peer-link]
  (.writeAndFlush peer-link 
                  (protocol/build-messages-msg-buf (:onyx.core/compressed event))))

(defmethod extensions/internal-ack-message NettyTcpSockets
  [messenger event peer-link message-id completion-id ack-val]
  (.writeAndFlush peer-link 
                  (protocol/build-ack-msg-buf message-id completion-id ack-val)))

(defmethod extensions/internal-complete-message NettyTcpSockets
  [messenger event id peer-link]
  (.writeAndFlush peer-link 
                  (protocol/build-completion-msg-buf id)))
