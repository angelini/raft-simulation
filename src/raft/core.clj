(ns raft.core
  (:require [raft.server :refer :all]
            [raft.client :refer :all]
            [raft.log :refer :all]
            [com.stuartsierra.component :as component]
            [clojure.core.async :as async]
            [clojure.math.numeric-tower :as math]))

(def raft-system-components [:client :server :log])

(def host "http://127.0.0.1")

(defn port [id] (+ 8080 id))

(defn url [id] (str host ":" (port id)))

(defn file [id] (str "node_" id ".log"))

(defn majority? [cluster votes]
  (let [cluster-size (+ (count cluster) 1)]
    (>= (count votes) (math/ceil (/ cluster-size 2)))))

(defn raft-system [id cluster]
  (component/system-map
    :id id
    :cluster cluster
    :client (create-client)
    :server (create-server (port id))
    :log (create-log (file id))))

(defn init-node [id]
  {:id id
   :state :follower
   :current-term 1
   :voted-for nil
   :leader-id nil
   :leader-state nil
   :votes #{}})

(defn leader-state [cluster last-log-index]
  {:next-index (zipmap (map :id cluster) (repeat (+ last-log-index 1)))
   :match-index (zipmap (map :id cluster) (repeat 0))})

(defn cluster-node-info [id]
  {:id id
   :url (url id)})

(defn request-vote-rpc [client log cluster node]
  (let [[last-index last-term] (last-entry log)]
    (doseq [cluster-node cluster]
      (rpc client cluster-node "request-vote" {:term (:current-term node)
                                               :candidate-id (:id node)
                                               :last-log-index last-index
                                               :last-log-term last-term}))))

(defn append-entries-rpc [client log cluster node]
  (lets [[last-index last-term] (last-entry log)
         new-entires (uncommitted-entries log)])
  (doseq [cluster-node cluster]
    (rpc client cluster-node "append-entries" {:term (:current-term node)
                                               :leader-id (:id node)
                                               :leader-commit last-index
                                               :prev-log-index 0
                                               :prev-log-term nil
                                               :entries new-entries})))

(defn follower->candidate [node]
  (assoc node :state :candidate
              :voted-for (:id node)
              :votes #{(:id node)}
              :current-term (inc (:current-term node))))

(defn candidate->follower [node]
  (assoc node :state :follwer
              :voted-for nil
              :votes #{}))

(defn candidate->leader [node]
  (assoc node :state :leader
              :voted-for nil
              :votes #{}
              :leader-id (:id node)))

(defn leader->follower [node]
  (assoc node :state :follower
              :leader-id nil
              :leader-state nil))

(defn request-vote-handler [message node]
  (let [{:keys [term candidate-id]} message
        {:keys [current-term voted-for id]} node
        response {:term current-term :id id :type :vote-response}]
    (if (or (< term current-term)
            (not (nil? voted-for)))
      (do (respond message (assoc response :vote-granted false))
          node)
      (do (respond message (assoc response :vote-granted true))
          (assoc node :voted-for candidate-id)))))

(defn append-entries-handler [log message node]
  (let [{:keys [term leader-id entries leader-commit
                prev-log-index prev-log-term]} message
        {:keys [current-term id]} node
        response {:term current-term :id id :type :append-response}
        consistent? (compare-prev? log prev-log-index prev-log-term)]
    (println (str ";; term -> " term))
    (println (str ";; current-term -> " current-term))
    (cond
      (< term current-term) (do (respond message (assoc response :success false))
                                node)
      (not consistent?) (do (respond message (assoc response :success false))
                            (remove-from! log prev-log-index)
                            node)
      :else (do (respond message (assoc response :success true))
                (append-entries! log entries)
                (assoc node :leader-id leader-id
                            :current-term term
                            :voted-for nil)))))

(defn vote-response-handler [client log cluster message node]
  (let [{:keys [term vote-granted id]} message
        {:keys [current-term]} node
        last-log-index (nth (last-entry log) 0)]
    (cond
      (> term current-term) (candidate->follower (assoc node :current-term term))
      (not vote-granted) node
      vote-granted (if (not (majority? cluster (conj (:votes node) id)))
                     (assoc node :votes (conj (:votes node) id))
                     (do (append-entries-rpc client log cluster node)
                         (-> node
                             (candidate->leader)
                             (assoc :leader-state (leader-state cluster last-log-index))))))))

(defn append-response-handler [message node]
  (let [{:keys [term success id]} message
        {:keys [current-term]} node]
    (cond
      (> term current-term) (leader->follower (assoc node :current-term term))
      (not success) node
      success node)))

(defn client-set-handler [log cluster message node]
  (if (not= (:state node) :leader)
    (if (nil? (:leader-id node))
      (redirect-client message (url (:id (rand-nth cluster))))
      (redirect-client message (url (:leader-id node))))
    (let [command (:command message)
          current-term (:current-term node)
          index (append-string-entries! log current-term [command])]
        (watch-commit-index log index (fn [] (respond message {:status :ok})))
        node)))

(defn heartbeat-handler [client log cluster node]
  (do (append-entries-rpc client log cluster node)
      node))

(defn timeout-handler [client log cluster node]
  (let [new-node (follower->candidate node)]
    (request-vote-rpc client log cluster new-node)
    new-node))

(defn generate-timeout [node]
  (if (= (:state node) :leader)
    (async/timeout 3000)
    (async/timeout (+ (rand-int 5000) 5000))))

(defn wait [system node]
  (let [{:keys [client server log cluster]} system
        req (incoming-rpc server)
        res (response-rpc client)
        timeout (generate-timeout node)
        [message ch] (async/alts!! [req res timeout])]
    (println "; Node")
    (prn node)
    (println "; Message")
    (prn message)
    (println "")
    (case (:type message)
      :request-vote (request-vote-handler message node)
      :append-entries (append-entries-handler log message node)
      :vote-response (vote-response-handler client log cluster message node)
      :append-response (append-response-handler message node)
      :client-set (client-set-handler log cluster message node)
      nil (if (= (:state node) :leader)
            (heartbeat-handler client log cluster node)
            (timeout-handler client log cluster node)))))

(defn -main [& args]
  (let [nodes (map #(Integer/parseInt %) args)
        id (first nodes)
        cluster (map cluster-node-info (rest nodes))
        system (component/start (raft-system id cluster))]
    (loop [node (init-node id)]
      (recur (wait system node)))))
