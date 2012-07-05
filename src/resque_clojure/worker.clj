(ns resque-clojure.worker
  (:refer-clojure :exclude [name])
  (:require [clojure.string :as s]))

(defn hyphenate [title]
  "Convert a titlecase'd class or method name (like those used on the ruby resque side) to hyphen-separated lowercase"
  (s/lower-case (s/replace title #"([a-z])([A-Z])" "$1-$2")))

(defn split-namespace [fn-name]
  "Split a qualified clojure namespace or convert a fully qualified ruby class name into clojure namespace"
  (let [[namespace fun] (s/split fn-name #"/")]
    (if (nil? fun)
      (let [parts (map hyphenate (s/split namespace #"::"))]
        [(s/join "." (butlast parts)) (last parts)])
      [namespace fun])))

(defn lookup-fn [namespaced-fn]
  (let [[namespace fun] (split-namespace namespaced-fn)]
    (ns-resolve (symbol namespace) (symbol fun))))

(defn work-on [state {:keys [func args queue] :as job}]
  (try
    (apply (lookup-fn func) args)
    {:result :pass :job job :queue queue}
    (catch Exception e
      {:result :error :exception e :job job :queue queue})))

(defn name [queues]
  (let [pid-host (.getName (java.lang.management.ManagementFactory/getRuntimeMXBean))
        [pid hostname] (s/split pid-host #"@")
        qs (apply str (interpose "," queues))]
    (str hostname ".clj:" pid ":" qs)))
