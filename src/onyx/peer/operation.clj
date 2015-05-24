(ns onyx.peer.operation
  (:require [onyx.extensions :as extensions]
            [taoensso.timbre :refer [info]]))

(defn apply-function [f params segment]
  ((reduce #(partial %1 %2) f params) segment))

(defn kw->fn [kw]
  (try
    (let [user-ns (symbol (name (namespace kw)))
          user-fn (symbol (name kw))]
      (or (ns-resolve user-ns user-fn) (throw (Exception.))))
    (catch Throwable e
      (throw (ex-info "Could not resolve symbol on the classpath, did you require the file that contains this symbol?" {:symbol kw})))))

(defn resolve-fn [task-map]
  (kw->fn (:onyx/fn task-map)))

(defn exception? [e]
  (instance? java.lang.Throwable e))

(defn start-lifecycle?
  [{:keys [onyx.core/queue onyx.core/ingress-queues onyx.core/task-map]}]
  true)

(defn peer-link
  [{:keys [onyx.core/state] :as event} peer-id]
  (if-let [link (:link (get (:links @state) peer-id))]
    link
    (let [site (-> @(:onyx.core/replica event)
                   :peer-sites
                   (get peer-id))]
      (-> state 
          (swap! update-in 
                 [:links peer-id] 
                 (fn [link]
                   (or link 
                       {:link (extensions/connect-to-peer (:onyx.core/messenger event) event site)
                        :timestamp (System/currentTimeMillis)})))
          :links
          (get peer-id)
          :link))))
