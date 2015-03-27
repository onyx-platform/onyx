(ns ^:no-doc onyx.messaging.netty-tcp
    (:require [clojure.core.async :refer [chan >!! >! <!! alts!! timeout close! go-loop]]
              [com.stuartsierra.component :as component]
              [taoensso.timbre :as timbre]
              [onyx.messaging.protocol :as protocol]
              [onyx.messaging.acking-daemon :as acker]
              [onyx.messaging.common :refer [bind-addr]]
              [onyx.compression.nippy :refer [compress decompress]]
              [onyx.extensions :as extensions])
    (:import [java.net InetSocketAddress]
             [java.util.concurrent TimeUnit Executors]
             [io.netty.channel.epoll 
              Epoll 
              EpollEventLoopGroup
              EpollServerSocketChannel
              EpollSocketChannel]
             [io.netty.buffer ByteBuf Unpooled UnpooledByteBufAllocator PooledByteBufAllocator ByteBufAllocator CompositeByteBuf]
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
             [io.netty.util ResourceLeakDetector ResourceLeakDetector$Level]
             [io.netty.bootstrap Bootstrap ServerBootstrap]
             [io.netty.channel.socket.nio NioServerSocketChannel]))

(defn leak-detector-level! [level]
  (ResourceLeakDetector/setLevel
    (case level
      :disabled ResourceLeakDetector$Level/DISABLED
      :simple ResourceLeakDetector$Level/SIMPLE
      :advanced ResourceLeakDetector$Level/ADVANCED
      :paranoid ResourceLeakDetector$Level/PARANOID)))

;(leak-detector-level! :paranoid)

(defn epoll? []
  (Epoll/isAvailable))

(defn int32-frame-decoder
  []
  ; Offset 0, 4 byte header, skip those 4 bytes.
  (LengthFieldBasedFrameDecoder. Integer/MAX_VALUE, 0, 4, 0, 4))

(defn int32-frame-encoder
  []
  (LengthFieldPrepender. 4))

(defn gen-tcp-handler
  [^ChannelGroup channel-group handler]
  (proxy [ChannelInboundHandlerAdapter] []
    (channelActive [ctx]
      (timbre/info "Channel active")
      (.add channel-group (.channel ctx)))
    (channelRead [^ChannelHandlerContext ctx ^Object message]
      (try
        (handler ctx message)
        (catch java.nio.channels.ClosedChannelException e
          (timbre/warn "channel closed"))))
    (exceptionCaught [^ChannelHandlerContext ctx ^Throwable cause]
      (timbre/error cause "TCP handler caught")
      (.close (.channel ctx)))
    (isSharable [] true)))

(defn event-executor
  "Creates a new netty execution handler for processing events. 
  Defaults to 1 thread per core."
  []
  (DefaultEventExecutorGroup. (.. Runtime getRuntime availableProcessors)))

(defonce ^DefaultEventExecutorGroup shared-event-executor
  (event-executor))

(defn channel-initializer-done [handler]
  (proxy [ChannelInitializer] []
    (initChannel [ch]
      (let [pipeline (.pipeline ^Channel ch)]
        (doto pipeline 
          (.addLast "int32-frame-decoder" (int32-frame-decoder))
          (.addLast "int32-frame-encoder" (int32-frame-encoder))
          (.addLast shared-event-executor "handler" handler))))))

(defn handle [buf-recvd-ch msg]
  (.retain ^ByteBuf msg)
  (>!! buf-recvd-ch msg))

(defn create-server-handler
  "Given a core, a channel, and a message, applies the message to 
  core and writes a response back on this channel."
  [buf-recvd-ch] 
  (fn [^ChannelHandlerContext ctx ^Object message]
    ;(timbre/info "TCP HANDLER MESSAGE " message)

    ; No need for any reply
    (handle buf-recvd-ch message)
    #_(when-let [response (handle buf-recvd-ch message)]
        (.writeAndFlush ctx response))))

(defn get-default-event-loop-threads
  "Determines the default number of threads to use for a Netty EventLoopGroup.
   This mimics the default used by Netty as of version 4.1."
  []
  (let [cpu-count (->> (Runtime/getRuntime) (.availableProcessors))]
    ; TODO: evaluate whether too many threads are used.
    ; reducing can improve performance
    (max 1 (SystemPropertyUtil/getInt "io.netty.eventLoopThreads" (* cpu-count 2)))))

(def ^String client-event-thread-pool-name "onyx-netty-client-event-pool")

(def client-group
  (let [thread-count (get-default-event-loop-threads)
        thread-factory (DefaultThreadFactory. client-event-thread-pool-name true)]
    (if (epoll?)
      (EpollEventLoopGroup. (long thread-count) thread-factory)
      (NioEventLoopGroup. (long thread-count) thread-factory))))

(def ^String server-event-thread-pool-name "onyx-netty-worker-event-pool")

(def worker-group
  (let [thread-count (get-default-event-loop-threads)
        thread-factory (DefaultThreadFactory. server-event-thread-pool-name true)]
    (if (epoll?)
      (EpollEventLoopGroup. (long thread-count) thread-factory)
      (NioEventLoopGroup. (long thread-count) thread-factory))))

(def ^String server-event-thread-pool-name "onyx-netty-boss-event-pool")

(def boss-group
  (let [thread-count (get-default-event-loop-threads)
        thread-factory (DefaultThreadFactory. server-event-thread-pool-name true)]
    (if (epoll?)
      (EpollEventLoopGroup. (long thread-count) thread-factory)
      (NioEventLoopGroup. (long thread-count) thread-factory))))


(defn start-netty-server
  [host port buf-recvd-ch]
  (let [bootstrap (ServerBootstrap.)
        channel (if (epoll?)
                  EpollServerSocketChannel
                  NioServerSocketChannel)

        channel-group (DefaultChannelGroup. (str "tcp-server " host ":" port)
                                            (ImmediateEventExecutor/INSTANCE))
        initializer (channel-initializer-done (gen-tcp-handler channel-group (create-server-handler buf-recvd-ch)))] 
    ; Configure bootstrap
    (doto bootstrap
      (.group boss-group worker-group)
      (.channel channel)
      (.option ChannelOption/SO_REUSEADDR true)
      (.option ChannelOption/TCP_NODELAY true)
      (.option ChannelOption/ALLOCATOR PooledByteBufAllocator/DEFAULT)
      (.childOption ChannelOption/SO_REUSEADDR true)
      (.childOption ChannelOption/TCP_NODELAY true)
      (.childOption ChannelOption/SO_KEEPALIVE true)
      (.childOption ChannelOption/ALLOCATOR PooledByteBufAllocator/DEFAULT)
      (.childHandler initializer))
    ; Start bootstrap
    (let [ch (->> (InetSocketAddress. host port)
                  (.bind bootstrap)
                  (.sync)
                  (.channel))
          _ (.add channel-group ch)
          assigned-port (.. ch localAddress getPort)]
      (timbre/info "TCP server" host assigned-port "online")
      {:port assigned-port
       :server-shutdown-fn (fn killer []
                             (.. channel-group close awaitUninterruptibly)
                             (timbre/info "TCP server" host port "shut down"))})))

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
               (.addLast "int32-frame-decoder" (int32-frame-decoder))
               (.addLast "int32-frame-encoder" (int32-frame-encoder))
               (.addLast "handler" handler)))
           (catch Exception e
             (timbre/fatal e))))))

(defn create-client [host port]
  (let [channel (if (epoll?)
                  EpollSocketChannel
                  NioSocketChannel)]
    (try
      (let [b (doto (Bootstrap.)
                (.option ChannelOption/SO_REUSEADDR true)
                (.option ChannelOption/MAX_MESSAGES_PER_READ Integer/MAX_VALUE)
                (.group client-group)
                (.channel channel)
                (.handler (client-channel-initializer (new-client-handler))))]
        (.channel ^ChannelFuture (.connect b host port))))))

(defn app [daemon parsed-ch inbound-ch release-ch]
    (go-loop []
             (try (let [msg (<!! parsed-ch)
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

; FIXME; don't want a whole go-loop here, can probably parse above in the handler
(defn buf-recv-loop [buf-recv-ch parsed-ch]
  (go-loop []
           (let [buf (<!! buf-recv-ch)]
             (try 
               (let [decompressed (protocol/read-buf buf)]
                 (>!! parsed-ch decompressed)
                 ; undo retain added in server handler
                 (.release ^ByteBuf buf))
               (catch Throwable t
                 (println t)
                 (timbre/fatal t (str "exception in go loop." buf " " (.getId (Thread/currentThread))))))
                 (recur))))

(defrecord NettyTcpSockets [opts]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Netty TCP Sockets")
    (let [buf-recv-ch (chan 10000)
          parsed-ch (chan (clojure.core.async/dropping-buffer 1000000))
          inbound-ch (:inbound-ch (:messenger-buffer component))
          release-ch (chan (clojure.core.async/dropping-buffer 1000000))
          daemon (:acking-daemon component)
          ip (bind-addr opts)
          {:keys [port server-shutdown-fn]} (start-netty-server ip 0 buf-recv-ch)
          buf-loop (buf-recv-loop buf-recv-ch parsed-ch)
          app-loop (app daemon parsed-ch inbound-ch release-ch)]
      (assoc component 
             :app-loop app-loop 
             :server-shutdown-fn server-shutdown-fn 
             :buf-loop buf-loop
             :ip ip 
             :port port 
             :release-ch release-ch)))

  (stop [component]
    (taoensso.timbre/info "Stopping Netty TCP Sockets")
    ((:server-shutdown-fn component))
    (close! (:app-loop component))
    (close! (:buf-loop component))
    (close! (:release-ch component))
    (assoc component :release-ch nil)))

(defn netty-tcp-sockets [opts]
  (map->NettyTcpSockets {:opts opts}))

; Not much need for these in aleph
; (defmethod extensions/send-peer-site NettyTcpSockets
;   [messenger]
;   [(:ip messenger) (:port messenger)])

; (defmethod extensions/acker-peer-site NettyTcpSockets
;   [messenger]
;   [(:ip messenger) (:port messenger)])

; (defmethod extensions/completion-peer-site NettyTcpSockets
;   [messenger]
;   [(:ip messenger) (:port messenger)])

(defmethod extensions/connect-to-peer NettyTcpSockets
  [messenger event [host port]]
  (timbre/info "Created client " host port)
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

(defn allocator [^Channel x]
  (.alloc x))

(defmethod extensions/send-messages NettyTcpSockets
  [messenger event ^Channel peer-link messages]
  (try
    (.writeAndFlush peer-link 
                    ^ByteBuf (protocol/build-messages-msg-buf (allocator peer-link) 
                                                              messages) 
                    (.voidPromise ^Channel peer-link))
    (catch Exception e 
      (timbre/error e))))

(defmethod extensions/internal-ack-message NettyTcpSockets
  [messenger event ^Channel peer-link message-id completion-id ack-val]
  (.writeAndFlush peer-link 
                  ^ByteBuf (protocol/build-ack-msg-buf (allocator peer-link) 
                                                       message-id 
                                                       completion-id 
                                                       ack-val)
                  (.voidPromise ^Channel peer-link)))

(defmethod extensions/internal-complete-message NettyTcpSockets
  [messenger event id ^Channel peer-link]
  (.writeAndFlush peer-link 
                  ^ByteBuf (protocol/build-completion-msg-buf (allocator peer-link) id)
                  (.voidPromise ^Channel peer-link)))

(defmethod extensions/close-peer-connection NettyTcpSockets
  [messenger event ^Channel peer-link]
  (.close peer-link))
