(ns onyx.peer.operation
  (:require [onyx.extensions :as extensions]
            [onyx.types :refer [->Link]]
            [taoensso.timbre :refer [info]]))

(defn get-method-java [class-name method-name] 
  (let [ms (filter #(= (.getName %) method-name) 
                   (.getMethods (Class/forName class-name)))]
    (if (= 1 (count ms)) 
      (first ms)   
      (throw (Exception. (format "Multiple methods found for %s/%s. Only one method may be defined." class-name method-name))))))

(defn build-fn-java 
  "Builds a clojure fn from a static java method. 
  Note, may be slower than it should be because of varargs, and the many
  (unnecessary) calls to partial in apply-function above."
  [method]
  (fn [& args] 
    (.invoke ^java.lang.reflect.Method method nil #^"[Ljava.lang.Object;" (into-array Object args))))

(defn kw->fn [kw]
  (try
    (let [user-ns (name (namespace kw))
          user-fn (name kw)]
      (or (try (ns-resolve (symbol user-ns) (symbol user-fn))
               (catch Throwable _)) 
          (build-fn-java (get-method-java user-ns user-fn))
          (throw (Exception.))))
    (catch Throwable e
      (throw (ex-info "Could not resolve symbol on the classpath, did you require the file that contains this symbol?" 
                      {:symbol kw :exception e})))))

(defn resolve-fn [task-map]
  (kw->fn (:onyx/fn task-map)))

(defn exception? [e]
  (instance? java.lang.Throwable e))

(defn start-lifecycle?
  [{:keys [onyx.core/queue onyx.core/ingress-queues onyx.core/task-map]}]
  true)

;; TODO: can be precalculated for peer in replica-view
(defn select-n-peers 
  "Stably select n peers using our id and the downstream task ids.
  If a peer is added or removed, the set can only change by one value at max"
  [id all-peers n]
  (if (<= (count all-peers) n)
    all-peers
    (take n 
          (sort-by (fn [peer-id] (hash-combine (.hashCode id) (.hashCode peer-id)))
                   all-peers))))

(defn peer-link
  [replica-val state event peer-id]
  (if-let [link (get (:links @state) peer-id)]
    (do 
      (reset! (:timestamp link) (System/currentTimeMillis))
      (:link link))
    (let [site (-> replica-val
                   :peer-sites
                   (get peer-id))]
      (-> state 
          (swap! update-in 
                 [:links peer-id] 
                 (fn [link]
                   (or link 
                       (->Link (extensions/connect-to-peer (:onyx.core/messenger event) peer-id event site)
                               (atom (System/currentTimeMillis))))))
          :links
          (get peer-id)
          :link))))
