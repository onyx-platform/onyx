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
             [io.netty.util.concurrent GenericFutureListener Future DefaultThreadFactory]
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
             [io.netty.util ResourceLeakDetector ResourceLeakDetector$Level]
             [io.netty.bootstrap Bootstrap ServerBootstrap]
             [io.netty.channel.socket.nio NioServerSocketChannel]
             [org.jboss.netty.buffer ChannelBuffers]))

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

(defn gen-tcp-handler
  [^ChannelGroup channel-group handler]
  (proxy [ChannelInboundHandlerAdapter] []
    (channelActive [^ChannelHandlerContext ctx]
      (timbre/info "Channel active")
      (.add channel-group (.channel ctx)))
    (channelRead [^ChannelHandlerContext ctx ^Object message]
      (try
        ;(timbre/info "Channel read " message)
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

(defn add-component! [^CompositeByteBuf composite ^ByteBuf buf]
  ;(timbre/info "Added to composite buf: " composite buf)
  (.addComponent composite buf)
  (.writerIndex composite (+ ^int (.writerIndex buf)
                             ^int (.writerIndex composite))))

(defn handle [buf-recvd-ch msg]
  (println "got message " msg)
  (.retain ^ByteBuf msg)
  (>!! buf-recvd-ch msg))

(defn new-composite-buffer [] 
  ;(timbre/info "creating new composite buffer")
  (.compositeBuffer (UnpooledByteBufAllocator. false)))

(defn create-server-handler
  "Given a core, a channel, and a message, applies the message to 
  core and writes a response back on this channel."
  [buf-recvd-ch] 
  (timbre/info "CREATED SERVER HANDLER")
    (fn [^ChannelHandlerContext ctx ^Object message]
      ;(timbre/info "TCP HANDLER MESSAGE " message)
      ; Actually handle request and reply if any
      (when-let [response (handle buf-recvd-ch message)]
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

(defn get-default-event-loop-threads
  "Determines the default number of threads to use for a Netty EventLoopGroup.
  This mimics the default used by Netty as of version 4.1."
  []
  (let [cpu-count (->> (Runtime/getRuntime) (.availableProcessors))]
    (max 1 (SystemPropertyUtil/getInt "io.netty.eventLoopThreads" (* cpu-count 2)))))

(def ^String client-event-thread-pool-name "aleph-netty-client-event-pool")

(def client-group
  (let [thread-count (get-default-event-loop-threads)
        thread-factory (DefaultThreadFactory. client-event-thread-pool-name true)]
    (if (epoll?)
      (EpollEventLoopGroup. (long thread-count) thread-factory)
      (NioEventLoopGroup. (long thread-count) thread-factory))))

(def ^String server-event-thread-pool-name "aleph-netty-server-event-pool")

(def server-group
  (let [thread-count (get-default-event-loop-threads)
        thread-factory (DefaultThreadFactory. server-event-thread-pool-name true)]
    (if (epoll?)
      (EpollEventLoopGroup. (long thread-count) thread-factory)
      (NioEventLoopGroup. (long thread-count) thread-factory))))

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
                    (timbre/info "RECEIVED PARSED: " msg)
                    (println "RECEIVED PARSED: " msg)
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

(defn start-netty-server
  [host port buf-recvd-ch]
  (let [channel (if (epoll?)
                  EpollSocketChannel
                  NioSocketChannel)
        bootstrap (ServerBootstrap.)
        event-loop-group-fn (:event-loop-group-fn netty-implementation)
        boss-group (event-loop-group-fn)
        worker-group (event-loop-group-fn)
        channel-group (DefaultChannelGroup. (str "tcp-server " host ":" port)
                                            (ImmediateEventExecutor/INSTANCE))
        initializer (channel-initializer-done (gen-tcp-handler channel-group (create-server-handler buf-recvd-ch)))] 
    ; Configure bootstrap
    (doto bootstrap
      ;(.group server-group)
      (.group worker-group boss-group)
      (.channel channel)
      (.option ChannelOption/SO_REUSEADDR true)
      (.option ChannelOption/TCP_NODELAY true)
      (.childOption ChannelOption/SO_REUSEADDR true)
      (.childOption ChannelOption/TCP_NODELAY true)
      (.childOption ChannelOption/SO_KEEPALIVE true)
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
                             ; Shut down workers and boss concurrently.
                             (let [w (shutdown-event-executor-group worker-group)
                                   b (shutdown-event-executor-group boss-group)]
                               @w
                               @b)
                             (timbre/info "TCP server" host port "shut down"))})))

(defn buf-recv-loop [buf-recv-ch composite-buf parsed-ch]
  (go-loop []
           (let [buf (<!! buf-recv-ch)]
             (try 
               (add-component! composite-buf buf)
               (.release ^ByteBuf buf) ; undo retain added in server handler
               (.markReaderIndex ^CompositeByteBuf composite-buf)
               ;(timbre/info "Making a read on " id " buf " composite-buf)
               (loop [decompressed (protocol/read-buf composite-buf)]
                 (if decompressed
                   (do (timbre/info "Read message " decompressed)
                       ;(timbre/info "After buf: " composite-buf)
                       ;(timbre/info "Readable bytes after: " (.readableBytes composite-buf) composite-buf)
                       ;(timbre/info "After read on id " id composite-buf)
                       (.discardReadComponents ^CompositeByteBuf composite-buf)
                       ;(timbre/info "After clear on id " id composite-buf)
                       ;(timbre/info "Decompressed is " decompressed)
                       ;(timbre/info "Readable bytes after discard : " (.readableBytes composite-buf) composite-buf)
                       (>!! parsed-ch decompressed)
                       (when-not (zero? (.readableBytes ^CompositeByteBuf composite-buf))
                         (.markReaderIndex ^CompositeByteBuf composite-buf)
                         (recur (protocol/read-buf composite-buf))))
                   (.resetReaderIndex ^CompositeByteBuf composite-buf)))
               ;(do (timbre/info "Can't perform a full read: " composite-buf)))) 
               (catch Throwable t
                 (println t)
                 (timbre/fatal t (str "exception in go loop." composite-buf))))
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
          ip "127.0.0.1"
          {:keys [port server-shutdown-fn]} (start-netty-server ip 0 buf-recv-ch)
          composite-buf (.retain ^CompositeByteBuf (new-composite-buffer))
          buf-loop (buf-recv-loop buf-recv-ch composite-buf parsed-ch)
          app-loop (app daemon parsed-ch inbound-ch release-ch)]
      (assoc component 
             :app-loop app-loop 
             :server-shutdown-fn server-shutdown-fn 
             :buf-loop buf-loop
             :composite-buf composite-buf
             :ip ip 
             :port port 
             :release-ch release-ch)))

  (stop [component]
    (taoensso.timbre/info "Stopping Netty TCP Sockets")
    ((:server-shutdown-fn component))
    (close! (:app-loop component))
    (close! (:buf-loop component))
    (.release ^CompositeByteBuf (:composite-buf component))
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
  [messenger event ^Channel peer-link]
  (timbre/info "Sending messages: " (:onyx.core/compressed event))
  (.writeAndFlush peer-link ^ByteBuf (protocol/build-messages-msg-buf (:onyx.core/compressed event))))

(defmethod extensions/internal-ack-message NettyTcpSockets
  [messenger event ^Channel peer-link message-id completion-id ack-val]
  (timbre/info "Sending ack message: " {:id message-id :completion-id completion-id :ack-val ack-val})
  (.writeAndFlush peer-link ^ByteBuf (protocol/build-ack-msg-buf message-id completion-id ack-val)))

(defmethod extensions/internal-complete-message NettyTcpSockets
  [messenger event id ^Channel peer-link]
  (timbre/info "Sending completion message: " {:id id})
  (.writeAndFlush peer-link 
                  (protocol/build-completion-msg-buf id)))

(defmethod extensions/close-peer-connection NettyTcpSockets
  [messenger event ^Channel peer-link]
  (.close peer-link))
