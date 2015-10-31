(ns onyx.peer.operation
  (:require [onyx.extensions :as extensions]
            [onyx.types :refer [->Link]]
            [taoensso.timbre :refer [info warn]]))

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
  [kw]
  (when (namespace kw)
    (throw (ex-info "Namespaced keywords cannot be used for java static method fns. Use in form :java.lang.Math.sqrt" {:kw kw})))
  (let [path (clojure.string/split (name kw) #"[.]")
        class-name (clojure.string/join "." (butlast path))
        method-name (last path)
        method (get-method-java class-name method-name)]
    (fn [& args]
    (.invoke ^java.lang.reflect.Method method nil #^"[Ljava.lang.Object;" (into-array Object args)))))

(defn kw->fn [kw]
  (try
    (let [user-ns (symbol (namespace kw))
          user-fn (symbol (name kw))]
      (or (ns-resolve user-ns user-fn)
          (throw (Exception.))))
    (catch Throwable e
      (throw (ex-info (str "Could not resolve symbol on the classpath, did you require the file that contains the symbol " kw "?") {:kw kw})))))

(defn resolve-fn [task-map]
  (kw->fn (:onyx/fn task-map)))

(defn exception? [e]
  (instance? java.lang.Throwable e))

(defn start-lifecycle?
  [{:keys [onyx.core/queue onyx.core/ingress-queues onyx.core/task-map]}]
  true)

(defn resolve-task-fn [entry]
  (let [f (if (or (:onyx/fn entry)
                  (= (:onyx/type entry) :function))
            (case (:onyx/language entry)
              :java (build-fn-java (:onyx/fn entry))
              (kw->fn (:onyx/fn entry))))]
    (or f identity)))

(defn resolve-restart-pred-fn [entry]
  (if-let [kw (:onyx/restart-pred-fn entry)]
    (kw->fn kw)
    (constantly false)))

(defn instantiate-plugin-instance [class-name pipeline-data]
  (.newInstance (.getDeclaredConstructor ^Class (Class/forName class-name)
                                         (into-array Class [clojure.lang.IPersistentMap]))
                (into-array [pipeline-data])))

(defn peer-link
  [replica-val state event peer-id]
  (if-let [link (get (:links @state) peer-id)]
    (do
      (reset! (:timestamp link) (System/currentTimeMillis))
      (:link link))
    (if-let [site (-> replica-val
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
          :link)
      (do (warn "Could not obtain peer-site from replica" peer-id)
          nil))))

(defn grouped-task? [task-map]
  (or (:onyx/group-by-key task-map)
      (:onyx/group-by-fn task-map)))
