(ns raft.core
  (:require [raft.server :refer :all]
            [raft.client :refer :all]
            [com.stuartsierra.component :as component]
            [clojure.core.async :as async])
  (:gen-class))

(def raft-system-components [:node :server])

(def port-base 8080)

(defn port [id] (+ port-base id))

(defn raft-system [id cluster]
  (component/system-map
    :client (create-client "http://127.0.0.1" port-base)
    :server (create-server (port id))))

(defn init-node [id]
  {:id id
   :state :follower
   :current-term 1
   :voted-for nil})

(defn follower->candidate [node]
  (assoc node :state :candidate))

(defn candidate->leader [node]
  (assoc node :state :leader))

(defn leader->follower [node]
  (assoc node :state :follower))

(defn request-vote-handler [server message node]
  (if (or (< (:term message) (:current-term node))
          (not (nil? (:voted-for node))))
    (do (respond server {:term (:current-term node) :vote-granted false})
        node)
    (do (respond server {:term (:current-term node) :vote-granted true})
        (assoc node :voted-for (:candidate-id message)))))

(defn wait [system node]
  (let [{:keys [client server]} system
        req (incoming-rpc server)
        [message ch] (async/alts!! [req])]
    (case (:type message)
      :request-vote (request-vote-handler server message node))))

(defn -main [& args]
  (let [nodes (map #(Integer/parseInt %) args)
        system (raft-system (first nodes) (rest nodes))]
    (loop [node (init-node (first nodes))]
      (recur (wait system node)))))
