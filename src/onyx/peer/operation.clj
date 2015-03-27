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
    (catch Exception e
      (throw (ex-info "Could not resolve function" {:fn kw})))))

(defn resolve-fn [task-map]
  (kw->fn (:onyx/fn task-map)))

(defn exception? [e]
  (let [classes (supers (class e))]
    (boolean (some #{java.lang.Exception} classes))))

(defn start-lifecycle?
  [{:keys [onyx.core/queue onyx.core/ingress-queues onyx.core/task-map]}]
  true)

(defn peer-link
  [{:keys [onyx.core/messenger onyx.core/state onyx.core/replica] :as event} peer-id link-type]
  (if-let [link (get-in @state [:links link-type peer-id])]
    link
    (let [site (get-in @replica [link-type peer-id])
          link (extensions/connect-to-peer messenger event site)]
      (swap! state assoc-in [:links link-type peer-id] link)
      link)))

