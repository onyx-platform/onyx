(ns onyx.peer.abs-engine)

(defn create-barrier [replica job-id barrier-id]
  {:barrier-id barrier-id
   :allocations (get-in @replica [:allocations job-id])})

(defn filter-channels
  "Returns channels from which to read. Filters out
   channels that we've already seen a barrier for."
  [task->ch barrier-state barrier-id]
  (reduce-kv
   (fn [result task-id ch]
     (if-not (some #{task-id} (get barrier-state barrier-id))
       (assoc result task-id ch)
       result))
   {}
   task->ch))

(defn emit-barrier?
  "Returns true if this peer has encountered the barrier from
   all of its immediate upstream peers."
  [barrier-state barrier-id upstream-peers]
  (= (get barrier-state barrier-id) upstream-peers))


(require '[clojure.test :refer [is]])

(is (= {:t2 :ch2 :t3 :ch3}
       (filter-channels {:t1 :ch1 :t2 :ch2 :t3 :ch3} {0 #{:t1}} 0)))

(is (= {}
       (filter-channels {:t1 :ch1 :t2 :ch2 :t3 :ch3} {0 #{:t1 :t2 :t3}} 0)))

(is (= {:t1 :ch1 :t2 :ch2 :t3 :ch3}
       (filter-channels {:t1 :ch1 :t2 :ch2 :t3 :ch3} {0 #{}} 0)))

(is (not (emit-barrier? {0 #{}} 0 #{:p1})))
(is (not (emit-barrier? {0 #{:p3}} 0 #{:p1 :p3})))

(is (emit-barrier? {0 #{:p1}} 0 #{:p1}))
(is (emit-barrier? {0 #{}} 0 #{}))
(is (emit-barrier? {0 #{:p2 :p3}} 0 #{:p3 :p2}))
