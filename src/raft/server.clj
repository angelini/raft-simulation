(ns raft.server
  (:require [com.stuartsierra.component :as component]
            [clojure.core.async :as async]
            [ring.adapter.jetty :as jetty]
            [ring.middleware.json :refer :all]
            [ring.util.response :refer :all]))

(defn message-type [uri]
  (case uri
    "/request-vote" :request-vote
    "/append-entries" :append-entries))

(defn format-request [request]
  (let [{:keys [body uri]} request]
    (assoc body :type (message-type uri))))

(defn create-handler [req-chan resp-chan]
  (fn [request]
    (do
      (async/>!! req-chan (format-request request))
      (async/<!! resp-chan))))

(defn wrap-handler [handler]
  (-> handler
      (wrap-json-body {:keywords? true})
      wrap-json-response))

(defrecord Server [port server req-chan resp-chan]
  component/Lifecycle

  (start [this]
    (println "; Starting server component")
    (if server
      this
      (let [req-chan (async/chan 5)
            resp-chan (async/chan 5)
            handler (create-handler req-chan resp-chan)
            server (jetty/run-jetty (wrap-handler handler) {:port port :join? false})]
       (assoc this :server server
                   :req-chan req-chan
                   :resp-chan resp-chan))))

  (stop [this]
    (println "; Stopping server component")
    (if (not server)
      this
      (do
        (async/close! req-chan)
        (async/close! resp-chan)
        (.stop server)
        (.join server)
        (assoc this :server nil :req-chan nil :resp-chan nil)))))

(defn create-server [port]
  (map->Server {:port port}))

(defn incoming-rpc [server]
  (:req-chan server))

(defn respond [server body]
  (async/>!! (:resp-chan server) (response body)))
