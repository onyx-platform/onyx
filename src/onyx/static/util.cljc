(ns onyx.static.util)

(defn index-of [coll element]
  (ffirst 
   (filter (fn [[index elem]] (= elem element)) 
           (map list (range) coll))))

#?(:cljs
   (defn munge-fn-name [kw]
     (str (munge-str (str (namespace kw)))
          "."
          (munge-str (str (name kw))))))

#?(:cljs
   (defn resolve-dynamic [kw]
           (js/eval (munge-fn-name kw))))

(defn kw->fn [kw]
  #?(:clj
     (try
       (let [user-ns (symbol (namespace kw))
             user-fn (symbol (name kw))]
         (or (ns-resolve user-ns user-fn)
             (throw (Exception.))))
       (catch Throwable e
         (throw (ex-info (str "Could not resolve symbol on the classpath, did you require the file that contains the symbol " kw "?") {:kw kw})))))
  #?(:cljs (resolve-dynamic kw)))

(defn exception? [e]
  #?(:clj (instance? java.lang.Throwable e))
  #?(:cljs (instance? js/Error e)))

(defn now []
  #?(:clj (System/currentTimeMillis))
  #?(:cljs (.now js/Date)))

(defn ms->ns [ms]
  (* 1000000 ms))
