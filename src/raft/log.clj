(ns raft.log
  (:require [clojure.java.io :as io]
            [com.stuartsierra.component :as component]))

(defn swap-and-return! [old-atom f & args]
  (loop []
    (let [old @old-atom
          new-val (apply f old args)]
      (if (compare-and-set! old-atom old new-val)
        [old new-val]
        (recur)))))

(defn inc-commit-index [state]
  (assoc state :commit-index (count (:entries state))))

(defn write [writer entries]
  (doseq [entry entries]
    (.write writer (str (:val entry) "\n"))))

(defn val-at [entries index]
  (if (= index 0)
    nil
    (nth entries (- index 1))))

(defrecord Log [file writer state]
  component/Lifecycle

  (start [this]
    (println "; Starting log component")
    (if state
      this
      (assoc this :writer (io/writer file :append true)
                  :state (atom {:entries []
                                :commit-index 0}))))

  (stop [this]
    (println "; Starting log component")
    (if (not state)
      this
      (do
        (.close writer)
        (assoc this :writer nil :state nil)))))

(defn create-log [file]
  (map->Log {:file file}))

(defn last-entry [log]
  (let [{:keys [entries commit-index]} @(:state log)]
    [commit-index (val-at entries commit-index)]))

(defn entries-from [log index]
  (let [{:keys [entries]} @(:state log)]
    (subvec entries (min index (count entries)))))

(defn compare-prev? [log prev-index prev-term]
  (let [{:keys [entries]} @(:state log)]
    (if (= prev-index 0)
      true
      (= (val-at entries prev-index) prev-term))))

(defn append-entries! [log entries]
  (swap! (:state log)
         (fn [current]
           (assoc current :entries (vec (concat (:entries current) entries))))))

(defn append-string-entries! [log term entries]
  (append-entries! log (map (fn [val] {:term term :val val}) entries)))

(defn apply-entries! [log index]
  (let [[prev state] (swap-and-return! (:state log) inc-commit-index)
        {:keys [entries commit-index]} state
        amount (- commit-index (:commit-index prev))]
    (->> entries
         (take-last amount)
         (write (:writer log))
         count)))

(defn remove-from! [log index]
  (swap! (:state log)
         (fn [current]
           (assoc current :entries (drop-last index (:entries current))))))

(defn watch-commit-index [log index cb]
  (add-watch (:state log)
             :commit-index
             (fn [_ _ _ new-state]
               (if (= new-state index) (cb)))))
