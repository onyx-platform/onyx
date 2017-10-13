(ns onyx.windowing.window-id
  (:require #?(:clj [primitive-math :as pm])
            #?(:cljs [clojure.core :as pm])))

;; An implementation of the Window-ID specification, as discussed
;; in http://web.cecs.pdx.edu/~tufte/papers/WindowAgg.pdf.
;;
;; Window-ID (WID) reduces memory space and execution time for
;; sliding, fixed, and landmark windows. All mistakes in this
;; implementation are our own.

;; WID uses two main algorithms - `extents` and `wids`.
;; `extents` takes a window ID and returns the windowing
;; attribute upper and lower bounds for which it accepts values.
;; WID is a powerful technique because it works over any totally
;; ordered domain - not just timestamps. We can exploit this to
;; create windows based on features of the incoming data.

;; `wids` is the inverse. `wids` takes a segment with a windowing
;; attribute and returns the window IDs to which it belongs.

;; `wids` is useful for bucketing segments. `extents` is useful for
;; computing the user-readable upper and lower bounds that a window
;; ID represents.

;; There are multiple variations of this algorithm depending on the
;; style of windowing being performed. We're going to start by focusing
;; on the case where the window is defined on the same attribute
;; for both the range and slide values. The window ID values for this
;; case are the natural numbers (0, 1, 2 ...). We'll note which variation
;; we're focusing on during each implementation.

;; Let's draw a picture to show how WID buckets segments, regardless
;; of what the windowing attribute is. Below is a table. On the left
;; hand side running vertically 0 - 14 are the natural numbers - these
;; are window IDs. Running horizontally across the top are multiples of
;; 5. Our window will slide by units of 5. The bars |---| represent
;; windows across the respective value range. The span of the window is denoted
;; on the right side of the table, running vertically. Notice that the WID
;; algorithms make "partial" windows for the minimum possible value. The first
;; 3 window IDs are of length 5, 10, and 15 respectively. You can imagine that
;; the rest of the window is on the left-hand-side (not seen) of the X axis.
;; The ranges (right hand side) are inclusive on both sides. That is, the bucket
;; [0 - 4] captures all values between 0.0 and 4.999999...

;;   1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75
;;0  |--|                                                         [0  - 4]
;;1  |-----|                                                      [0  - 9]
;;2  |---------|                                                  [0  - 14]
;;3  |-------------|                                              [0  - 19]
;;4     |--------------|                                          [5  - 24]
;;5        |---------------|                                      [10 - 29]
;;6            |---------------|                                  [15 - 34]
;;7                |---------------|                              [20 - 39]
;;8                    |---------------|                          [25 - 44]
;;9                        |---------------|                      [30 - 49]
;;10                           |---------------|                  [35 - 54]
;;11                               |---------------|              [40 - 59]
;;12                                   |---------------|          [45 - 64]
;;13                                       |---------------|      [50 - 69]
;;14                                           |---------------|  [55 - 74]

;; We're first going to implement `extents` for queries whose range and
;; slide values are the same. This is the first algorithm detailed in
;; section 3.3.

(defn extent-lower [^long min-windowing-attr ^long w-range ^long w-slide ^long w]
  (pm/max min-windowing-attr (pm/- (pm/+ min-windowing-attr (pm/* w-slide (pm/inc w))) w-range)))

(defn extent-upper ^long [^long min-windowing-attr ^long w-slide ^long w]
  (pm/dec (pm/+ min-windowing-attr (pm/* w-slide (pm/inc w)))))

(defn extents [min-windowing-attr w-range w-slide w]
  (range (extent-lower min-windowing-attr w-range w-slide w)
         (pm/inc (extent-upper min-windowing-attr w-slide w))))

;; WID requires that a strict lower-bound of the windowing attribute
;; be defined. In our example, this will be 0. We will use a window
;; range of 20, and a slide value of 5. `extents` tells of which values
;; of the windowing key fall into their respect windows via:
;;
;; (doseq [n (range 20)]
;;  (println n ": " (extents 0 20 5 n)))

;; We now see the first 20 window IDs, and which windowing values
;; they capture. This matches up with the table above
;;
;; 0 => (0 1 2 3 4)
;; 1 => (0 1 2 3 4 5 6 7 8 9)
;; 2 => (0 1 2 3 4 5 6 7 8 9 10 11 12 13 14)
;; 3 => (0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19)
;; 4 => (5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24)
;; 5 => (10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29)
;; 6 => (15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34)
;; 7 => (20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39)
;; 8 => (25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44)
;; 9 => (30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49)
;; 10 => (35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54)
;; 11 => (40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59)
;; 12 => (45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64)
;; 13 => (50 51 52 53 54 55 56 57 58 59 60 61 62 63 64 65 66 67 68 69)
;; 14 => (55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74)
;; 15 => (60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79)
;; 16 => (65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 80 81 82 83 84)
;; 17 => (70 71 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89)
;; 18 => (75 76 77 78 79 80 81 82 83 84 85 86 87 88 89 90 91 92 93 94)
;; 19 => (80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 96 97 98 99)

(defn floor ^long [x]
  #?(:clj x)
  #?(:cljs (pm/long (.floor js/Math x))))

;; Now we implement the inverse, `wids`. `wids` lets us take
;; segment and directly find the window IDs that it corresponds to.
;; `wids` is defined in section 3.4 of the paper. This is the variant
;; of the algorithm that also covers the case where range and slide
;; are defined on the same value.
(defn wids-lower ^long [^long min-windowing-attr ^long w-slide ^long w-val]
  (pm/dec (floor (pm// (- w-val 
                          min-windowing-attr)
                       w-slide))))

(defn wids-upper ^long [^long min-windowing-attr ^long w-range ^long w-slide ^long w-val]
  (pm/dec (floor (pm// (pm/- (pm/+ w-val 
                                   w-range)
                           min-windowing-attr)
                      w-slide))))

(defn wids [min-windowing-attr w-range w-slide w-val]
  (let [lower (wids-lower min-windowing-attr w-slide w-val)
        upper (wids-upper min-windowing-attr w-range w-slide w-val)]
    (range (pm/inc lower) (pm/inc upper))))

;; The follow code runs through 30 segments with
;; windowing attributes 0-30 in sequence, producing
;; which windows the segments belongs to. This matches
;; up with both tables we looked at above.
;;
;; (doseq [n (range 30)]
;;   (println n "=>" (wids 0 20 5 :k {:k n})))

;; 0 => (0 1 2 3)
;; 1 => (0 1 2 3)
;; 2 => (0 1 2 3)
;; 3 => (0 1 2 3)
;; 4 => (0 1 2 3)
;; 5 => (1 2 3 4)
;; 6 => (1 2 3 4)
;; 7 => (1 2 3 4)
;; 8 => (1 2 3 4)
;; 9 => (1 2 3 4)
;; 10 => (2 3 4 5)
;; 11 => (2 3 4 5)
;; 12 => (2 3 4 5)
;; 13 => (2 3 4 5)
;; 14 => (2 3 4 5)
;; 15 => (3 4 5 6)
;; 16 => (3 4 5 6)
;; 17 => (3 4 5 6)
;; 18 => (3 4 5 6)
;; 19 => (3 4 5 6)
;; 20 => (4 5 6 7)
;; 21 => (4 5 6 7)
;; 22 => (4 5 6 7)
;; 23 => (4 5 6 7)
;; 24 => (4 5 6 7)
;; 25 => (5 6 7 8)
;; 26 => (5 6 7 8)
;; 27 => (5 6 7 8)
;; 28 => (5 6 7 8)
;; 29 => (5 6 7 8)

;; See the generative tests in onyx/windowing/wid_generative_test.clj for more.
