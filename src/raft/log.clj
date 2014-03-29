(ns raft.log
  (:require [com.stuartsierra.component :as component]))

(defrecord Log [file state]
  component/Lifecycle

  (start [this]
    (println "; Starting log component")
    (if state
      this
      (assoc this :state (atom {:entries []
                                :commit-index 0
                                :last-applied 0}))))

  (stop [this]
    (println "; Starting log component")
    (if (not state)
      this
      (assoc this :state nil))))

(defn create-log [file]
  (map->Log {:file file}))

(defn last-entry [log]
  (let [{:keys [entries]} @(:state log)]
    [(count entries) (last entries)]))

(defn compare-prev? [log prev-log-index prev-log-term]
  (let [{:keys [entries]} @(:state log)]
    (if (= prev-log-index 0)
      true
      (= (nth entries (- prev-log-index 1)) prev-log-term))))

(defn append-entries [log entries leader-commit]
  (swap! (:state log)
         (fn [current]
           (let [new-commit (min (count (:entries current)) leader-commit)]
             (assoc current :entries (conj (:entries current) entries)
                            :commit-index new-commit)))))
