(defproject raft "0.1.0-SNAPSHOT"
  :description "Raft implementation and specification"
  :url "https://github.com/angelini/raft"
  :license {:name "MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/math.numeric-tower "0.0.4"]
                 [com.stuartsierra/component "0.2.1"]
                 [clj-http "0.9.1"]
                 [clj-json "0.5.3"]
                 [ring/ring-core "1.2.2"]
                 [ring/ring-json "0.3.0"]
                 [ring/ring-jetty-adapter "1.2.2"]
                 [org.clojure/core.async "0.1.278.0-76b25b-alpha"]]
  :profiles {:dev {:source-paths ["dev"]
                   :dependencies [[org.clojure/tools.namespace "0.2.4"]]}})
