(ns ^:no-doc onyx.messaging.netty-tcp
    (:require [clojure.core.async :refer [chan >!! >! <!! alts!! timeout close! thread go-loop dropping-buffer]]
              [com.stuartsierra.component :as component]
              [taoensso.timbre :as timbre]
              [onyx.messaging.protocol-netty :as protocol]
              [onyx.messaging.acking-daemon :as acker]
              [onyx.messaging.common :refer [bind-addr external-addr allowable-ports]]
              [onyx.compression.nippy :refer [compress decompress]]
              [onyx.extensions :as extensions])
    (:import [java.net InetSocketAddress]
             [java.util.concurrent TimeUnit Executors]
             [java.nio]
             [io.netty.buffer ByteBuf]
             [io.netty.util.internal SystemPropertyUtil]
             [io.netty.util.concurrent Future EventExecutorGroup DefaultThreadFactory 
              DefaultEventExecutorGroup ImmediateEventExecutor]
             [io.netty.channel Channel ChannelOption ChannelFuture ChannelInitializer 
              ChannelHandler ChannelHandlerContext ChannelInboundHandlerAdapter]
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

(defn get-default-event-loop-threads
  "Determines the default number of threads to use for a Netty EventLoopGroup.
   This mimics the default used by Netty as of version 4.1."
  []
  (let [cpu-count (->> (Runtime/getRuntime) (.availableProcessors))]
    (max 1 (SystemPropertyUtil/getInt "io.netty.eventLoopThreads" 
                                      (* cpu-count 2)))))

(defonce ^DefaultEventExecutorGroup shared-event-executor (event-executor))

(defrecord NettyPeerGroup [opts]
  component/Lifecycle
  (start [component]
    (let [thread-count (long (get-default-event-loop-threads))
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

      (timbre/info "Starting Netty peer group")
      (assoc component
        :shared-event-executor shared-event-executor
        :client-group client-group
        :worker-group worker-group
        :boss-group boss-group)))

  (stop [{:keys [client-group worker-group boss-group] :as component}]
    (.shutdownGracefully client-group)
    (.shutdownGracefully boss-group)
    (.shutdownGracefully worker-group)
    (assoc component 
      :shared-event-executor nil :client-group nil
      :worker-group nil :boss-group nil)))

(defn netty-peer-group [opts]
  (map->NettyPeerGroup {:opts opts}))

(defmethod extensions/assign-site-resources :netty
  [config peer-site peer-sites]
  (let [used-ports (->> (vals peer-sites) 
                        (filter 
                         (fn [s]
                           (= (:netty/external-addr peer-site) 
                              (:netty/external-addr s))))
                        (map :netty/port)
                        set)
        port (first (remove used-ports
                            (:netty/ports peer-site)))]
    (assert port "Couldn't assign port - ran out of available ports.")
    {:netty/port port}))

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
      (.add channel-group (.channel ctx)))
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
    (handle buf-recvd-ch message)))

(defn start-netty-server
  [boss-group worker-group host port buf-recvd-ch]
  (let [bootstrap (ServerBootstrap.)
        channel (if (epoll?)
                  EpollServerSocketChannel
                  NioServerSocketChannel)
        channel-group (DefaultChannelGroup. (str "tcp-server " host ":" port)
                        (ImmediateEventExecutor/INSTANCE))
        initializer (channel-initializer-done 
                     (gen-tcp-handler channel-group 
                                      (create-server-handler buf-recvd-ch)))] 
                                        ; Configure bootstrap
    (doto bootstrap
      (.group boss-group worker-group)
      (.channel channel)
      (.option ChannelOption/SO_REUSEADDR true)
      (.option ChannelOption/TCP_NODELAY true)
      (.childOption ChannelOption/SO_REUSEADDR true)
      (.childOption ChannelOption/TCP_NODELAY true)
      (.childOption ChannelOption/SO_KEEPALIVE true)
      (.childHandler initializer))
    (let [ch (->> (InetSocketAddress. host port)
                  (.bind bootstrap)
                  (.sync)
                  (.channel))
          _ (.add channel-group ch)
          assigned-port (.. ch localAddress getPort)]
      (timbre/info "Netty server" host assigned-port "online")
      (fn killer []
        (.. channel-group close awaitUninterruptibly)
        (timbre/info "TCP server" host port "shut down")))))

(defn new-client-handler []
  (proxy [ChannelHandler] []
    (handlerAdded [ctx])
    (handlerRemoved [ctx])
    (exceptionCaught [context cause]
      (timbre/error cause "TCP client exception.")
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

(defn create-client [client-group host port]
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

(defn app [daemon messenger buf-recv-ch inbound-ch release-ch retry-ch shutdown-ch]
  (thread
    (loop []
          (let [[buf ch] (alts!! [buf-recv-ch shutdown-ch])]
            (when buf 
              (try 
                (let [msg (protocol/read-buf (:decompress-f messenger) buf)]
                  ;(taoensso.timbre/info "Message buf: " buf msg)
                  ;; undo retain added in server handler
                  (.release ^ByteBuf buf)
                  (let [t ^byte (:type msg)]
                    (cond (= t protocol/messages-type-id) 
                          (doseq [message (:messages msg)]
                            (>!! inbound-ch message))

                          (= t protocol/ack-type-id)
                          (acker/ack-message daemon
                                             (:id msg)
                                             (:completion-id msg)
                                             (:ack-val msg))

                          (= t protocol/completion-type-id)
                          (>!! release-ch (:id msg))

                          (= t protocol/retry-type-id)
                          (>!! retry-ch (:id msg))

                          :else
                          (throw (ex-info "Unexpected message received from Netty" {:message msg})))))
                (catch Exception e
                  (taoensso.timbre/error e)
                  (throw e)))
              (recur))))))

(defrecord NettyTcpSockets [peer-group]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Netty TCP Sockets")
    (let [{:keys [client-group worker-group boss-group]} (:messaging-group peer-group)
          config (:config peer-group)
          release-ch (chan (dropping-buffer 10000))
          retry-ch (chan (dropping-buffer 10000))
          bind-addr (bind-addr config)
          external-addr (external-addr config)
          ports (allowable-ports config)]
      (assoc component
             :bind-addr bind-addr 
             :external-addr external-addr
             :boss-group boss-group
             :client-group client-group
             :worker-group worker-group
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
        (let [{:keys [shutdown-fn shutdown-ch app-loop]} rs] 
          (shutdown-fn)
          (close! shutdown-ch)
          (<!! app-loop))
        (reset! resources nil)) 
      (catch Exception e (timbre/fatal e)))
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
  (let [buf-recv-ch (chan 10000)
        inbound-ch (:inbound-ch (:messenger-buffer messenger))
        release-ch (:release-ch messenger)
        retry-ch (:retry-ch messenger)
        daemon (:acking-daemon messenger)
        shutdown-fn (start-netty-server (:boss-group messenger)
                                        (:worker-group messenger)
                                        (:bind-addr messenger) 
                                        (:netty/port assigned) 
                                        buf-recv-ch)
        shutdown-ch (chan 1)
        app-loop (app daemon messenger buf-recv-ch inbound-ch release-ch retry-ch shutdown-ch)]
    (reset! (:resources messenger)
            {:shutdown-fn shutdown-fn 
             :shutdown-ch shutdown-ch
             :buf-recv-ch buf-recv-ch
             :app-loop app-loop})))

(defmethod extensions/connect-to-peer NettyTcpSockets
  [messenger event {:keys [netty/external-addr netty/port]}]
  (create-client (:client-group messenger) external-addr port))

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
  [messenger event ^Channel peer-link messages]
  ;(taoensso.timbre/info "SENDING messages " messages)
  (.writeAndFlush peer-link 
                  ^ByteBuf (protocol/build-messages-msg-buf (:compress-f messenger) messages) 
                  (.voidPromise ^Channel peer-link)))

(defmethod extensions/internal-ack-message NettyTcpSockets
  [messenger event ^Channel peer-link message-id completion-id ack-val]
  ;(taoensso.timbre/info "SENDING ACK: " message-id ack-val)
  (.writeAndFlush peer-link 
                  ^ByteBuf (protocol/build-ack-msg-buf message-id completion-id ack-val)
                  (.voidPromise ^Channel peer-link)))

(defmethod extensions/internal-complete-message NettyTcpSockets
  [messenger event id ^Channel peer-link]
  (.writeAndFlush peer-link 
                  ^ByteBuf (protocol/build-completion-msg-buf id)
                  (.voidPromise ^Channel peer-link)))

(defmethod extensions/internal-retry-message NettyTcpSockets
  [messenger event id ^Channel peer-link]
  (.writeAndFlush peer-link 
                  ^ByteBuf (protocol/build-retry-msg-buf id)
                  (.voidPromise ^Channel peer-link)))

(defmethod extensions/close-peer-connection NettyTcpSockets
  [messenger event ^Channel peer-link]
  (.close peer-link))
