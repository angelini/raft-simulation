(ns raft.server
  (:require [com.stuartsierra.component :as component]
            [clojure.core.async :as async]
            [ring.adapter.jetty :as jetty]
            [ring.middleware.json :refer :all]
            [ring.util.response :refer :all]))

(defn message-type [uri]
  (case uri
    "/request-vote" :request-vote
    "/append-entries" :append-entries
    "/client-set" :client-set))

(defn format-request [request]
  (let [{:keys [body uri]} request]
    (assoc body :type (message-type uri))))

(defn create-handler [req-chan]
  (fn [request]
    (let [resp-chan (async/chan)
          formatted (format-request request)]
      (async/>!! req-chan (assoc formatted :resp-chan resp-chan))
      (let [resp (async/<!! resp-chan)] (async/close! resp-chan) resp))))

(defn wrap-handler [handler]
  (-> handler
      (wrap-json-body {:keywords? true})
      wrap-json-response))

(defrecord Server [port server req-chan]
  component/Lifecycle

  (start [this]
    (println "; Starting server component")
    (if server
      this
      (let [req-chan (async/chan 5)
            handler (create-handler req-chan)
            server (jetty/run-jetty (wrap-handler handler) {:port port :join? false})]
       (assoc this :server server
                   :req-chan req-chan))))

  (stop [this]
    (println "; Stopping server component")
    (if (not server)
      this
      (do
        (async/close! req-chan)
        (.stop server)
        (.join server)
        (assoc this :server nil :req-chan nil)))))

(defn create-server [port]
  (map->Server {:port port}))

(defn incoming-rpc [server]
  (:req-chan server))

(defn respond [request body]
  (async/>!! (:resp-chan request) (response body)))

(defn redirect-client [request leader-url]
  (async/>!! (:resp-chan request) (redirect leader-url)))
