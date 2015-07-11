(ns onyx.types)

(defrecord Message [id payload])

(defrecord Leaf [message id acker-id completion-id ack-val])

(defn leaf 
  ([message] 
     (->Leaf message nil nil nil nil)))

(defrecord Route [flow exclusions post-transformation action])

(defrecord Ack [id completion-id ack-val timestamp])

(defrecord Result [root leaves])

(defrecord Link [link timestamp])
