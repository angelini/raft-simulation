(ns user
  (:require [com.stuartsierra.component :as component]
            [clojure.tools.namespace.repl :refer (refresh)]
            [clojure.core.async :as async]
            [clj-http.client :as http]
            [raft.core :refer :all]
            [raft.server :refer :all]
            [raft.client :refer :all]
            [raft.log :refer :all]))

(def system nil)

(defn init []
  (alter-var-root #'system
                  (constantly (raft-system 1 [2 3]))))

(defn start []
  (alter-var-root #'system component/start))

(defn stop []
  (alter-var-root #'system component/stop))

(defn go []
  (init)
  (start))

(defn reset []
  (stop)
  (refresh :after 'user/go))
