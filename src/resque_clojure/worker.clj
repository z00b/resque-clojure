(ns resque-clojure.worker
  (:refer-clojure :exclude [name])
  (:use [clojure.string :only [split]]))

(def config (atom {}))

(defn configure [c]
  (swap! config merge c))

(defn lookup-fn [namespaced-fn]
  (let [handler (:lookup-fn @config)]
    (if (nil? handler)
      (let [[namespace fun] (split namespaced-fn #"/")]
        (ns-resolve (symbol namespace) (symbol fun)))
      (handler namespaced-fn))))

(defn work-on [state {:keys [func args queue] :as job}]
  (try
    (apply (lookup-fn func) args)
    {:result :pass :job job :queue queue}
    (catch Exception e
      {:result :error :exception e :job job :queue queue})))

(defn name [queues]
  (let [pid-host (.getName (java.lang.management.ManagementFactory/getRuntimeMXBean))
        [pid hostname] (split pid-host #"@")
        qs (apply str (interpose "," queues))]
    (str hostname ".clj:" pid ":" qs)))
