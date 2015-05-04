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
  (let [classes (supers (class e))]
    (boolean (some #{java.lang.Throwable} classes))))

(defn start-lifecycle?
  [{:keys [onyx.core/queue onyx.core/ingress-queues onyx.core/task-map]}]
  true)

(defn peer-link
  [{:keys [onyx.core/messenger onyx.core/state onyx.core/replica] :as event} peer-id]
  (if-let [link (get-in @state [:links peer-id])]
    link
    (let [site (get-in @replica [:peer-sites peer-id])
          link (-> state 
                   (swap! update-in 
                          [:links peer-id] 
                          (fn [link]
                            (or link 
                                (extensions/connect-to-peer messenger event site))))
                   (get-in [:links peer-id]))]
      link)))
