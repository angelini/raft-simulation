(ns raft.client
  (:require [com.stuartsierra.component :as component]
            [clojure.core.async :as async]
            [clj-http.client :as http]))

(defn url [host port-base action id]
  (str host ":" (+ port-base id) "/" action))

(defrecord Client [host port-base resp-chan]
  component/Lifecycle

  (start [this]
    (println "; Starting client component")
    (if resp-chan
      this
      (assoc this :resp-chan (async/chan 5))))

  (stop [this]
    (println "; Stopping client component")
    (if (not resp-chan)
      this
      (do
        (async/close! resp-chan)
        (assoc this :resp-chan nil)))))

(defn create-client [host port-base]
  (map->Client {:host host :port-base port-base}))

(defn response-rpc [client]
  (:resp-chan client))

(defn rpc [client id action body]
  (let [{:keys [host port-base resp-chan]} client]
    (async/go
      (async/>! resp-chan (http/get (url host port-base action id)
                                    {:query-params body})))))