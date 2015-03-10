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

(defn sentinel-node-name [task-id input-name]
  (format "%s-%s" task-id input-name))

(defn vote-for-sentinel-leader [log task-id input-name uuid]
  (let [node (sentinel-node-name task-id input-name)]
    (extensions/write-chunk log :sentinel {:leader uuid} node)))

(defn filter-sentinels [decompressed]
  (remove (partial = :done) decompressed))

(defn learned-all-sentinels? [event state]
  (= (into #{} (keys (:learned-sentinel state)))
     (into #{} (keys (:onyx.core/ingress-queues event)))))

(defn drained-all-inputs? [event state]
  (let [drained-inputs (keys (into {} (filter second (:drained-inputs state))))]
    (= (into #{} drained-inputs)
       (into #{} (keys (:onyx.core/ingress-queues event))))))

(defn on-last-batch
  [event f]
  {})

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

