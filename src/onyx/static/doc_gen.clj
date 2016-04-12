(ns ^{:no-doc true} onyx.static.doc-gen
  "Document generator using the information model. Only useful for Onyx development purposes."
  (:require [onyx.information-model]
            [table.core :as t]
            [table.width]))

(defn format-md [v]
  (cond (keyword? v) 
        (str "`" v "`")
        :else v))

(defn gen-markdown [model rows table-width]
  (binding [table.width/*width* (delay table-width)] 
    (let [header (into ["Parameter"] (map second rows))]  
      (t/table (into [header]
                     (map (fn [[k v]]
                            (map format-md 
                                 (into [k] 
                                       (map (fn [[col-key _]] 
                                              (col-key (model k))) 
                                            rows))))
                          model))
             :style :github-markdown))))

(comment
  (spit "st.md" (with-out-str
                  (gen-markdown (:model (:state-aggregation model))
                                [[:optional? "Optional?"]
                                 [:doc "Doc"]]
                                160)))

  (spit "rocksdb.md" (with-out-str
                       (gen-markdown (into {}
                                           (filter #(re-matches #".*rocksdb.*" (namespace (key %)))
                                                   (:model (:peer-config model))))
                                     [;[:optional? "Optional?"]
                                      [:doc "Description"]
                                      [:default "Default"]]
                                     800)))

  (spit "bookkeeper.md" (with-out-str
                          (gen-markdown (into {}
                                              (filter #(re-matches #".*bookkeeper.*" (namespace (key %)))
                                                      (:model (:peer-config model))))
                                        [;[:optional? "Optional?"]
                                         [:doc "Description"]
                                         [:default "Default"]]
                                        800)))


  (spit "bookkeeper.md" (with-out-str
                          (gen-markdown (into {}
                                              (filter #(re-matches #".*bookkeeper.*" (namespace (key %)))
                                                      (:model (:env-config model))))
                                        [;[:optional? "Optional?"]
                                         [:doc "Description"]
                                         [:default "Default"]]
                                        800)))) 
