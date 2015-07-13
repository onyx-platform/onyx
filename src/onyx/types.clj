(ns onyx.types)

<<<<<<< HEAD
(defrecord Message [id payload])

(defrecord Leaf [message id acker-id completion-id ack-val hash-group route])
=======
(defrecord Leaf [message id acker-id completion-id ack-val ack-vals route routes hash-group])

(defn leaf 
  ([message] 
     (->Leaf message nil nil nil nil nil nil nil nil)))
>>>>>>> ba78b0e... Read vpeer-id separately to rest of buffer so that

(defrecord Route [flow exclusions post-transformation action])

(defrecord Ack [id completion-id ack-val timestamp])

(defrecord Results [tree acks segments])

(defrecord Result [root leaves])

(defrecord Link [link timestamp])
