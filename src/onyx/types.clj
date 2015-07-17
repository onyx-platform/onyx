(ns onyx.types)

(defrecord Leaf [message id acker-id completion-id ack-val hash-group route])

(defn input [id message]
  (->Leaf message id nil nil nil nil nil))

(defrecord Route [flow exclusions post-transformation action])

(defrecord Ack [id completion-id ack-val timestamp])

(defrecord Results [tree acks segments])

(defrecord Result [root leaves])

(defrecord Link [link timestamp])
