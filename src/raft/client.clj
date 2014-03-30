(ns raft.client
  (:require [com.stuartsierra.component :as component]
            [clojure.core.async :as async]
            [clj-http.client :as http]
            [clj-json.core :as json]))

(defn format-response [response]
  (let [body (json/parse-string (:body response) true)]
    (assoc body :type (keyword (:type body)))))

(defrecord Client [resp-chan]
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

(defn create-client []
  (map->Client {}))

(defn response-rpc [client]
  (:resp-chan client))

(defn rpc [client node action body]
  (async/go
    (let [params {:body (json/generate-string body)
                  :content-type :json}
          resp (try (http/post (:url node) params) (catch Exception e nil))]
      (if resp
        (async/>! (:resp-chan client) (format-response resp))))))
