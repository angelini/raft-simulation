(ns raft.core
  (:require [raft.server :refer :all]
            [raft.client :refer :all]
            [raft.log :refer :all]
            [com.stuartsierra.component :as component]
            [clojure.core.async :as async]
            [clojure.math.numeric-tower :as math]))

(def raft-system-components [:client :server :log])

(def port-base 8080)

(defn port [id] (+ port-base id))

(defn file [id] (str "node_" id ".log"))

(defn majority? [cluster votes]
  (>= (math/ceil (/ (count cluster) 2)) (count votes)))

(defn raft-system [id cluster]
  (component/system-map
    :id id
    :cluster cluster
    :client (create-client "http://127.0.0.1" port-base)
    :server (create-server (port id))
    :log (create-log (file id))))

(defn init-node [id]
  {:id id
   :state :follower
   :current-term 1
   :voted-for nil
   :leader nil
   :votes #{}})

(defn request-vote-rpc [client log cluster node]
  (let [[last-index last-term] (last-entry log)]
    (doseq [id cluster]
      (rpc client id "request-vote" {:term (:current-term node)
                                     :candidate-id (:id node)
                                     :last-log-index last-index
                                     :last-log-term last-term}))))

(defn append-entries-rpc [client log cluster node]
  (doseq [id cluster]
    (rpc client id "append-entries" {:term (:current-term node)
                                     :leader-id (:id node)
                                     :prev-log-index 0
                                     :prev-log-term nil
                                     :entries []})))

(defn follower->candidate [node]
  (assoc node :state :candidate
              :voted-for (:id node)
              :votes [(:id node)]
              :current-term (inc (:current-term node))))

(defn candidate->follower [node]
  (assoc node :state :follwer
              :voted-for nil
              :votes #{}))

(defn candidate->leader [node]
  (assoc node :state :leader
              :voted-for nil
              :votes #{}))

(defn leader->follower [node]
  (assoc node :state :follower))

(defn request-vote-handler [server message node]
  (let [{:keys [term candidate-id]} message
        {:keys [current-term voted-for id]} node
        response {:term current-term :id id :type :vote-response}]
    (if (or (< term current-term)
            (not (nil? voted-for)))
      (do (respond server (assoc response :vote-granted false))
          node)
      (do (respond server (assoc response :vote-granted true))
          (assoc node :voted-for message)))))

(defn append-entry-handler [server log message node]
  (let [{:keys [term leader entries leader-commit
                prev-log-index prev-log-term]} message
        {:keys [current-term id]} node
        response {:term current-term :id id :type :append-response}]
    (if (or (< term current-term)
            (not (compare-prev? log prev-log-index prev-log-term)))
      (do (respond server (assoc response :success false))
          node)
      (do (respond server (assoc response :success true))
          (append-entries log entries leader-commit)
          (assoc node :leader leader)))))

(defn vote-response-handler [client log cluster message node]
  (let [{:keys [term vote-granted id]} message
        {:keys [current-term]} node]
    (cond
      (> term current-term) (candidate->follower (assoc node :current-term term))
      (not vote-granted) node
      (vote-granted) (if (not (majority? cluster (conj (:votes node) id)))
                       (assoc node :votes (conj (:votes node) id))
                       (do (append-entries-rpc client log cluster node)
                           (candidate->leader node))))))

(defn append-response-handler [message node]
  (let [{:keys [term success id]} message
        {:keys [current-term]} node]
    (cond
      (> term current-term) (leader->follower (assoc node :current-term term))
      (not success) node
      (success) node)))

(defn heartbeat-handler [client log cluster node]
  (do (append-entries-rpc client log cluster node)
      node))

(defn timeout-handler [client log cluster node]
  (let [new-node (follower->candidate node)]
    (request-vote-rpc client log cluster new-node)
    new-node))

(defn generate-timeout [node]
  (if (= (:state node) :leader)
    (async/timeout 500)
    (async/timeout (+ (rand-int 1000) 2000))))

(defn wait [system node]
  (let [{:keys [client server log cluster]} system
        req (incoming-rpc server)
        res (response-rpc client)
        timeout (generate-timeout node)
        [message ch] (async/alts!! [req res timeout])]
    (prn message)
    (case (:type message)
      :request-vote (request-vote-handler server message node)
      :append-entry (append-entry-handler server log message node)
      :vote-response (vote-response-handler client log cluster message node)
      :append-response (append-response-handler message node)
      nil (if (= (:state node) :leader)
            (heartbeat-handler client log cluster node)
            (timeout-handler client log cluster node)))))

(defn -main [& args]
  (let [nodes (map #(Integer/parseInt %) args)
        [id & cluster] nodes
        system (component/start (raft-system id cluster))]
    (loop [node (init-node id)]
      (recur (wait system node)))))
