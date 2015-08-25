(ns ^:no-doc onyx.messaging.netty-tcp
    (:require [clojure.core.async :refer [chan >!! >! <!! alts!! timeout close!
                                          thread go-loop sliding-buffer]]
              [com.stuartsierra.component :as component]
              [taoensso.timbre :as timbre]
              [onyx.messaging.protocol-netty :as protocol]
              [onyx.messaging.common :refer [bind-addr external-addr allowable-ports]]
              [onyx.compression.nippy :refer [compress decompress]]
              [onyx.extensions :as extensions]
              [onyx.static.default-vals :refer [defaults]])
    (:import [java.net InetSocketAddress]
             [java.util.concurrent TimeUnit Executors]
             [java.nio]
             [io.netty.buffer ByteBuf]
             [io.netty.util.internal SystemPropertyUtil]
             [io.netty.util.concurrent Future EventExecutorGroup DefaultThreadFactory
              DefaultEventExecutorGroup ImmediateEventExecutor GenericFutureListener]
             [io.netty.channel Channel ChannelOption ChannelFuture ChannelInitializer ChannelPipeline
              MultithreadEventLoopGroup ChannelHandler ChannelHandlerContext ChannelInboundHandlerAdapter]
             [io.netty.channel.epoll Epoll EpollEventLoopGroup EpollServerSocketChannel EpollSocketChannel]
             [io.netty.channel.socket SocketChannel]
             [io.netty.channel.socket.nio NioServerSocketChannel NioSocketChannel]
             [io.netty.channel.nio NioEventLoopGroup]
             [io.netty.channel.group ChannelGroup DefaultChannelGroup]
             [io.netty.handler.codec LengthFieldBasedFrameDecoder LengthFieldPrepender]
             [io.netty.util ResourceLeakDetector ResourceLeakDetector$Level]
             [io.netty.bootstrap Bootstrap ServerBootstrap]))

(def ^String client-event-thread-pool-name "onyx-netty-client-event-pool")
(def ^String worker-event-thread-pool-name "onyx-netty-worker-event-pool")
(def ^String boss-event-thread-pool-name "onyx-netty-boss-event-pool")

(defn leak-detector-level! [level]
  (ResourceLeakDetector/setLevel
   (case level
     :disabled ResourceLeakDetector$Level/DISABLED
     :simple ResourceLeakDetector$Level/SIMPLE
     :advanced ResourceLeakDetector$Level/ADVANCED
     :paranoid ResourceLeakDetector$Level/PARANOID)))

(defn event-executor
  "Creates a new netty execution handler for processing events.
  Defaults to 1 thread per core."
  []
  (DefaultEventExecutorGroup. (.. Runtime getRuntime availableProcessors)))

(defn epoll? []
  (Epoll/isAvailable))

(defonce ^DefaultEventExecutorGroup shared-event-executor (event-executor))

(defrecord NettyPeerGroup [opts]
  component/Lifecycle
  (start [component]
    (let [thread-count (or (:onyx.messaging.netty/thread-pool-sizes opts)
                           (:onyx.messaging.netty/thread-pool-sizes defaults))

          client-thread-factory (DefaultThreadFactory. client-event-thread-pool-name true)
          client-group (if (epoll?)
                         (EpollEventLoopGroup. thread-count client-thread-factory)
                         (NioEventLoopGroup. thread-count client-thread-factory))
          boss-thread-factory (DefaultThreadFactory. boss-event-thread-pool-name true)
          boss-group (if (epoll?)
                       (EpollEventLoopGroup. thread-count boss-thread-factory)
                       (NioEventLoopGroup. thread-count boss-thread-factory))
          worker-thread-factory (DefaultThreadFactory. worker-event-thread-pool-name true)
          worker-group (if (epoll?)
                         (EpollEventLoopGroup. thread-count worker-thread-factory)
                         (NioEventLoopGroup. thread-count worker-thread-factory))]
      (timbre/info "Starting Netty Peer Group")
      (assoc component
             :shared-event-executor shared-event-executor
             :client-group client-group
             :worker-group worker-group
             :boss-group boss-group)))

  (stop [{:keys [client-group worker-group boss-group] :as component}]
    (timbre/info "Stopping Netty Peer Group")
    (.shutdownGracefully ^MultithreadEventLoopGroup client-group)
    (.shutdownGracefully ^MultithreadEventLoopGroup boss-group)
    (.shutdownGracefully ^MultithreadEventLoopGroup worker-group)
    (assoc component
      :shared-event-executor nil :client-group nil
      :worker-group nil :boss-group nil)))

(defn netty-peer-group [opts]
  (map->NettyPeerGroup {:opts opts}))

(defmethod extensions/assign-site-resources :netty
  [_ peer-id peer-site peer-sites]
  (let [used-ports (->> (vals peer-sites)
                        (filter
                         (fn [s]
                           (= (:netty/external-addr peer-site)
                              (:netty/external-addr s))))
                        (map :netty/port)
                        set)
        port (first (remove used-ports
                            (:netty/ports peer-site)))]
    (when-not port
      (throw (ex-info "Couldn't assign port - ran out of available ports.
                      Available ports can be configured in the peer-config.
                      e.g. {:onyx.messaging/peer-ports [40000, 40002],
                      :onyx.messaging/peer-port-range [40200 40260]}"
                      peer-site)))
    {:netty/port port}))

(defmethod extensions/get-peer-site :netty
  [replica peer]
  (get-in replica [:peer-sites peer :netty/external-addr]))

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
      (.add channel-group (.channel ^ChannelHandlerContext ctx)))
    (channelRead [^ChannelHandlerContext ctx ^Object message]
      (try
        (handler ctx message)
        (catch java.nio.channels.ClosedChannelException e
          (timbre/warn "Channel closed"))))
    (exceptionCaught [^ChannelHandlerContext ctx ^Throwable cause]
      (timbre/error cause "TCP handler caught")
      (.close (.channel ctx)))
    (isSharable [] true)))

(defn channel-initializer-done [handler]
  (proxy [ChannelInitializer] []
    (initChannel [ch]
      (let [pipeline ^ChannelPipeline (.pipeline ^Channel ch)]
        (doto pipeline
          (.addLast "int32-frame-decoder" ^LengthFieldBasedFrameDecoder (int32-frame-decoder))
          (.addLast "int32-frame-encoder" ^LengthFieldPrepender (int32-frame-encoder))
          (.addLast shared-event-executor "handler" handler))))))

(defn create-server-handler
  "Given a core, a channel, and a message, applies the message to
  core and writes a response back on this channel."
  [messenger inbound-ch release-ch retry-ch]
  (let [acking-ch (:acking-ch (:acking-daemon messenger))
        decompress-f (:decompress-f messenger)]
    (fn [^ChannelHandlerContext ctx ^ByteBuf buf]
      (try
        (let [t ^byte (protocol/read-msg-type buf)]
          (cond (= t protocol/messages-type-id)
                (doseq [message (protocol/read-messages-buf decompress-f buf)]
                  (>!! inbound-ch message))

                (= t protocol/ack-type-id)
                (doseq [ack (protocol/read-acks-buf buf)]
                  (>!! acking-ch ack))

                (= t protocol/completion-type-id)
                (>!! release-ch (protocol/read-completion-buf buf))

                (= t protocol/retry-type-id)
                (>!! retry-ch (protocol/read-retry-buf buf))))
        (catch Throwable e
          (taoensso.timbre/error e)
          (throw e))))))

(defn start-netty-server
  [boss-group worker-group host port messenger inbound-ch release-ch retry-ch]
  (let [bootstrap (ServerBootstrap.)
        channel (if (epoll?) EpollServerSocketChannel NioServerSocketChannel)
        channel-group (DefaultChannelGroup. (str "tcp-server " host ":" port)
                                            (ImmediateEventExecutor/INSTANCE))
        initializer (->> (create-server-handler messenger inbound-ch release-ch retry-ch)
                         (gen-tcp-handler channel-group)
                         (channel-initializer-done))]
    (doto bootstrap
      (.group boss-group worker-group)
      (.channel channel)
      (.option ChannelOption/SO_REUSEADDR true)
      (.option ChannelOption/TCP_NODELAY true)
      (.childOption ChannelOption/SO_REUSEADDR true)
      (.childOption ChannelOption/TCP_NODELAY true)
      (.childOption ChannelOption/SO_KEEPALIVE true)
      (.childHandler initializer))
    (let [ch (->> (InetSocketAddress. ^String host ^Integer port)
                  (.bind bootstrap)
                  (.sync)
                  (.channel))
          _ (.add channel-group ch)]
      (timbre/info "Netty server" host port "online")
      (fn killer []
        (.close channel-group)
        (timbre/info "TCP server" host port "shut down")))))

(defn new-client-handler []
  (proxy [ChannelHandler] []
    (handlerAdded [ctx])
    (handlerRemoved [ctx])
    (exceptionCaught [^ChannelHandlerContext context cause]
      (timbre/error cause "TCP client exception.")
      (.close context))))

(defn client-channel-initializer [handler]
  (proxy [ChannelInitializer] []
    (initChannel [ch]
      (timbre/info "Initializing client channel")
      (try (let [pipeline ^ChannelPipeline (.pipeline ^Channel ch)]
             (doto ^ChannelPipeline pipeline
               (.addLast "int32-frame-decoder" ^LengthFieldBasedFrameDecoder (int32-frame-decoder))
               (.addLast "int32-frame-encoder" ^LengthFieldPrepender (int32-frame-encoder))
               (.addLast "handler" ^ChannelHandler handler)))
           (catch Throwable e
             (timbre/fatal e))))))

(defn established? [^Channel channel]
  (and channel (.isActive channel)))

(defn create-client [client-group host port]
  (let [channel (if (epoll?)
                  EpollSocketChannel
                  NioSocketChannel)
        b (doto (Bootstrap.)
            (.option ChannelOption/SO_REUSEADDR true)
            (.option ChannelOption/MAX_MESSAGES_PER_READ Integer/MAX_VALUE)
            (.group client-group)
            (.channel channel)
            (.handler (client-channel-initializer (new-client-handler))))
        ch-fut ^ChannelFuture (.connect ^Bootstrap b ^String host ^Integer port)]
    (if (.awaitUninterruptibly ch-fut (:onyx.messaging.netty/connect-timeout-millis defaults))
      (let [ch (.channel ch-fut)]
        (if (and (.isSuccess ch-fut)
                 (established? ch))
          ch
          (do (.close ch)
              nil))))))

(defrecord NettyTcpSockets [peer-group]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Netty TCP Sockets")
    (let [{:keys [client-group worker-group boss-group]} (:messaging-group peer-group)
          config (:config peer-group)
          release-ch (chan (sliding-buffer (:onyx.messaging/release-ch-buffer-size defaults)))
          retry-ch (chan (sliding-buffer (:onyx.messaging/retry-ch-buffer-size defaults)))
          bind-addr (bind-addr config)
          external-addr (external-addr config)
          ports (allowable-ports config)
          pending-buffer-size (or (:onyx.messaging.netty/pending-buffer-size config)
                                  (:onyx.messaging.netty/pending-buffer-size defaults))]
      (assoc component
             :bind-addr bind-addr
             :external-addr external-addr
             :boss-group boss-group
             :client-group client-group
             :worker-group worker-group
             :pending-buffer-size pending-buffer-size
             :ports ports
             :resources (atom nil)
             :release-ch release-ch
             :retry-ch retry-ch
             :decompress-f (or (:onyx.messaging/decompress-fn (:config peer-group)) decompress)
             :compress-f (or (:onyx.messaging/compress-fn (:config peer-group)) compress))))

  (stop [{:keys [resources release-ch] :as component}]
    (taoensso.timbre/info "Stopping Netty TCP Sockets")
    (try
      (when-let [rs @resources]
        (let [{:keys [shutdown-fn]} rs]
          (shutdown-fn))
        (reset! resources nil))
      (catch Throwable e (timbre/fatal e)))
    (assoc component :bind-addr nil :external-addr nil
           :worker-group nil :client-group nil :boss-group nil
           :resources nil :release-ch nil :retry-ch nil)))

(defn netty-tcp-sockets [peer-group]
  (map->NettyTcpSockets {:peer-group peer-group}))

(defmethod extensions/peer-site NettyTcpSockets
  [messenger]
  {:netty/ports (:ports messenger)
   :netty/external-addr (:external-addr messenger)})

(defmethod extensions/open-peer-site NettyTcpSockets
  [messenger assigned]
  (let [inbound-ch (:inbound-ch (:messenger-buffer messenger))
        release-ch (:release-ch messenger)
        retry-ch (:retry-ch messenger)
        shutdown-fn (start-netty-server (:boss-group messenger)
                                        (:worker-group messenger)
                                        (:bind-addr messenger)
                                        (:netty/port assigned)
                                        messenger
                                        inbound-ch
                                        release-ch
                                        retry-ch)]
    (reset! (:resources messenger)
            {:shutdown-fn shutdown-fn})))

(defn flush-pending
  "Flush all pending bufs. Run after when the channel is established."
  [^Channel channel pending-ch]
  (close! pending-ch)
  (loop []
    (when-let [buf ^ByteBuf (<!! pending-ch)]
      (.write channel buf (.voidPromise channel))
      (recur)))
  (.flush channel))

(defprotocol IConnectionManager
  (connect [_])
  (write [connection buf])
  (reset-connection [_])
  (enqueue-pending [_ buf])
  (close [_]))

(defprotocol IConnectionState
  (initializing [this])
  (reset [this])
  (connecting [this])
  (failed [this])
  (connected [this]))

(defn add-failed-check
  "Check if the message failed to send"
  [^ChannelFuture f connection buf]
  (.addListener f (reify GenericFutureListener
                    (operationComplete [_ _]
                      (when-not (.isSuccess f)
                        (timbre/trace (ex-info "Message failed to send" {:cause (.cause f)}))
                        (reset-connection connection))))))

(defn make-pending-chan [messenger]
  (chan (sliding-buffer (:pending-buffer-size messenger))))

(defn state->connecting [state]
  (compare-and-set! state :initializing :connecting))

(defn state->reset [state]
    (compare-and-set! state :connected :reset))

(defn state->connecting [state]
  (or (compare-and-set! state :reset :connecting)
      (compare-and-set! state :initializing :connecting)
      (compare-and-set! state :failed :connecting)))

(defn state->failed [state]
  (compare-and-set! state :connecting :failed))

(defn state->connected [state]
  (compare-and-set! state :connecting :connected))

(defrecord ConnectionManager [messenger site state pending-ch channel]
  IConnectionManager
  (reset-connection [connection]
    (when (state->reset state)
      (reset! pending-ch (make-pending-chan messenger))
      (reset! channel nil)
      (connect connection)))

  (enqueue-pending [connection buf]
    (case @state
      :initializing (>!! @pending-ch buf)
      :connecting (>!! @pending-ch buf)
      :reset (connect connection)
      :failed (connect connection)
      ;; may have finished connecting in the time between
      ;; so we should check if it's established now before trying to reconnect
      :connected (let [channel-val ^Channel @channel]
                   (if (established? channel-val)
                     (.writeAndFlush channel-val buf (.voidPromise channel-val))
                     (reset-connection connection)))))

  (write [connection buf]
    (let [channel-val ^Channel @channel]
      (if (and channel-val (.isActive channel-val))
        (let [fut (.writeAndFlush channel-val ^ByteBuf buf)]
          (add-failed-check fut connection ^ByteBuf buf))
        (enqueue-pending connection buf))))

  (close [_]
    (let [cval @channel] (if cval (.close ^Channel cval)))
    (some-> @pending-ch close!))

  (connect [_]
    ; The state machine decides who gets to connect to ensure only one thread
    ; will connect, and the remaining will write out to the pending channel
    (when (state->connecting state)
      (future
        (if-let [opened-channel (create-client (:client-group messenger)
                                               (:netty/external-addr site)
                                               (:netty/port site))]
          (let [_ (reset! channel opened-channel)
                connected? (state->connected state)]
            (assert connected?)
            (flush-pending opened-channel @pending-ch))
          (do
            (state->failed state)
            (reset! channel nil)
            (reset! pending-ch (make-pending-chan messenger))))))))

(defmethod extensions/connect-to-peer NettyTcpSockets
  [messenger peer-id event site]
  (doto
    (->ConnectionManager messenger
                         site
                         (atom :initializing)
                         (atom (make-pending-chan messenger))
                         (atom nil))
    connect))

(defmethod extensions/receive-messages NettyTcpSockets
  [messenger {:keys [onyx.core/task-map] :as event}]
  (let [batch-size (:onyx/batch-size task-map)
        ms (or (:onyx/batch-timeout task-map) (:onyx/batch-timeout defaults))
        ch (:inbound-ch (:onyx.core/messenger-buffer event))
        timeout-ch (timeout ms)]
    (loop [segments [] i 0]
      (if (< i batch-size)
        (if-let [v (first (alts!! [ch timeout-ch]))]
          (recur (conj segments v) (inc i))
          segments)
        segments))))

(defmethod extensions/send-messages NettyTcpSockets
  [messenger event peer-link messages]
  (write peer-link (protocol/build-messages-msg-buf (:compress-f messenger) messages)))

(defmethod extensions/internal-ack-segments NettyTcpSockets
  [messenger event peer-link acks]
  (write peer-link (protocol/build-acks-msg-buf acks)))

(defmethod extensions/internal-complete-message NettyTcpSockets
  [messenger event id peer-link]
  (write peer-link (protocol/build-completion-msg-buf id)))

(defmethod extensions/internal-retry-segment NettyTcpSockets
  [messenger event id peer-link]
  (write peer-link (protocol/build-retry-msg-buf id)))

(defmethod extensions/close-peer-connection NettyTcpSockets
  [messenger event peer-link]
  (close peer-link))
