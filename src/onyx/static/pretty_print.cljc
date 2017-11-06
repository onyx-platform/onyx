(ns onyx.static.pretty-print
  #?(:clj (:require [io.aviso.ansi :as a])))

(def bold
  #?(:clj a/bold
     :cljs identity))

(def magenta
  #?(:clj a/magenta
     :cljs identity))

(def blue
  #?(:clj a/blue
     :cljs identity))

(def bold-red
  #?(:clj a/bold-red
     :cljs identity))

(def bold-green
  #?(:clj a/bold-green
     :cljs identity))
