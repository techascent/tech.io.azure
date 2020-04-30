(ns tech.io.azure.perf-test
  (:require [tech.io :as io]
            [tech.io.azure.blob]
            [tech.io.azure.file]
            [tech.config.core :as config])
  (:import [java.util UUID]))

(defn- manual-perf-test!
  []
  (let [data {:a (for [_ (range 1000)] (rand))}]
    ;; Writing blobs on GFS
    (let [start (System/nanoTime)]
      (dotimes [_ 100]
        (config/with-config [:azure-blob-account-name (config/get-config :gfs-name)
                             :azure-blob-account-key (config/get-config :gfs-key)]
          (io/put-nippy! (format "azb://perf-test/%s.nippy" (UUID/randomUUID)) data)))
      (println (format"azb done: %.3fs" (* 1e-9 (- (System/nanoTime) start)))))
    ;; Writing files on LFS
    (let [start (System/nanoTime)]
      (dotimes [_ 100]
        (config/with-config [:azure-blob-account-name (config/get-config :lfs-name)
                             :azure-blob-account-key (config/get-config :lfs-key)]
          (io/put-nippy! (format "azf://perf-test/%s.nippy" (UUID/randomUUID)) data)))
      (println (format"azf done: %.3fs" (* 1e-9 (- (System/nanoTime) start)))))))
