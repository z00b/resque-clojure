(ns resque-clojure.worker
  (:refer-clojure :exclude [name])
  (:use [clojure.string :only [split]]))

(def config (atom {}))

(defn configure [c]
  "use the config map entry :lookup-fn to override the default lookup-fn for jobs found in resque
  ie, (worker/configure {:lookup-fn my-custom-func})
  where my-custom-func take a single argument of the :class string found in the resque job and returns
  a function capable of processing the jobs arguments"
  (swap! config merge c))

(defn lookup-fn [namespaced-fn]
  (let [[namespace fun] (split namespaced-fn #"/")]
    (ns-resolve (symbol namespace) (symbol fun))))

(defn work-on [state {:keys [func args queue] :as job}]
  (let [resolver (or (:lookup-fn @config) lookup-fn)]
    (try
      (apply (resolver func) args)
      {:result :pass :job job :queue queue}
      (catch Exception e
        {:result :error :exception e :job job :queue queue}))))

(defn name [queues]
  (let [pid-host (.getName (java.lang.management.ManagementFactory/getRuntimeMXBean))
        [pid hostname] (split pid-host #"@")
        qs (apply str (interpose "," queues))]
    (str hostname ".clj:" pid ":" qs)))
