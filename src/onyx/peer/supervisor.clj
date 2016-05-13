(ns ^:no-doc onyx.peer.supervisor
  (:require [clojure.core.async :refer [promise-chan thread alts!! >!! <!! close! chan]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info warn fatal error]]))

(defn restart-component [component-f restart-ch shutdown-ch parent-val]
  (let [result
        (try
          (let [[_ ch] (alts!! [shutdown-ch] :default true)]
            (when-not (= ch shutdown-ch)
              (component-f restart-ch parent-val)))
          (catch Throwable t
            (warn t "Error restarting Component. Retrying...")
            :retry))]
    (if (= :retry result)
      (recur component-f restart-ch shutdown-ch parent-val)
      result)))

(defn supervisor-daemon
  [component-f component-state parent-restart-ch restart-ch
   shutdown-ch shutdown-ack-ch children sleep-ms]
  (try
    (loop [live @component-state
           r-ch restart-ch]
      (let [[v ch] (alts!! [shutdown-ch r-ch parent-restart-ch] :priority true)]
        (cond (= ch shutdown-ch)
              (do (component/stop live)
                  (reset! component-state nil)
                  (>!! shutdown-ack-ch true))
              (some #{ch} #{r-ch parent-restart-ch})
              (let [r-ch* (promise-chan)]
                (component/stop live)
                (Thread/sleep sleep-ms)
                (if-let [live (restart-component component-f r-ch* shutdown-ch v)]
                  (do (reset! component-state live)
                      (doseq [c @children]
                        (>!! c live))
                      (recur live r-ch*))
                  (>!! shutdown-ack-ch true))))))
    (catch Throwable e
      (fatal e "Supervisor daemon threw an exception."))))

(defn supervise [component-f sleep-ms]
  (let [parent-restart-ch (chan)
        restart-ch (promise-chan)
        shutdown-ch (promise-chan)
        shutdown-ack-ch (promise-chan)
        started-component (component-f restart-ch)
        children (atom #{})
        component-state (atom started-component)]
    {:daemon-ch (thread
                  (supervisor-daemon
                   component-f component-state
                   parent-restart-ch restart-ch
                   shutdown-ch shutdown-ack-ch children
                   sleep-ms))
     :component-state component-state
     :parents (atom #{})
     :children children
     :parent-restart-ch parent-restart-ch
     :shutdown-ch shutdown-ch
     :shutdown-ack-ch shutdown-ack-ch}))

(defn restartable-by [parent-sv child-sv]
  (swap! (:children parent-sv) conj (:parent-restart-ch child-sv))
  (swap! (:parents child-sv conj) conj parent-sv))

(defn shutdown-supervisor [sv]
  (swap! (:parents sv) disj (:parent-restart-ch sv))
  (close! (:shutdown-ch sv))
  (<!! (:shutdown-ack-ch sv)))
