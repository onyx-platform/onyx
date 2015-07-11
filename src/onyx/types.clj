(ns onyx.types)

(defrecord Message [id payload])

(defrecord Leaf [message id acker-id completion-id ack-val hash-group route])

(defrecord Route [flow exclusions post-transformation action])

(defrecord Ack [id completion-id ack-val timestamp])

(defrecord Results [tree acks segments])

(defrecord Result [root leaves])

(defrecord Link [link timestamp])
