(ns onyx.peer.operation
  (:require [onyx.types :refer [->Link]]
            [onyx.static.util :refer [kw->fn]]))

#?(:clj 
   (defn get-method-java [class-name method-name]
     (let [ms (filter #(= (.getName ^Class %) method-name)
                      (.getMethods (Class/forName class-name)))]
       (if (= 1 (count ms))
         (first ms)
         (throw (Exception. (format "Multiple methods found for %s/%s. Only one method may be defined." class-name method-name)))))))

#?(:clj 
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
         (.invoke ^java.lang.reflect.Method method nil #^"[Ljava.lang.Object;" (into-array Object args))))))

(defn resolve-fn [task-map]
  (kw->fn (:onyx/fn task-map)))

(defn start-lifecycle? [event]
  true)

#?(:clj 
   (defn resolve-task-fn [entry]
     (let [f (if (or (:onyx/fn entry)
                     (= (:onyx/type entry) :function))
               (case (:onyx/language entry)
                 :java (build-fn-java (:onyx/fn entry))
                 (kw->fn (:onyx/fn entry))))]
       (or f identity))))

#?(:clj 
   (defn instantiate-plugin-instance [class-name pipeline-data]
     (.newInstance (.getDeclaredConstructor ^Class (Class/forName class-name)
                                            (into-array Class [clojure.lang.IPersistentMap]))
                   (into-array [pipeline-data]))))
