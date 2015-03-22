(ns onyx.peer.operation
  (:require [onyx.extensions :as extensions]
            [taoensso.timbre :refer [info]]))

(defn apply-fn [f params segment]
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
  [{:keys [onyx.core/messenger onyx.core/state onyx.core/replica] :as event} peer-id]
  (if-let [link (get-in @state [:links peer-id])]
    link
    (let [site (get-in @replica [:peer-sites peer-id])
          site-resources (get-in @replica [:peer-site-resources peer-id])
          link (extensions/connect-to-peer messenger event site site-resources)]
      (swap! state assoc-in [:links peer-id] link)
      link)))
