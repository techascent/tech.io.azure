(defproject tech.io.azure "0.1-SNAPSHOT"
  :description "Bindings to at least blob storage for tech.io system."
  :url "http://github.com/techascent/tech.io.azure"
  :license {:name "EPL-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 ;; [com.microsoft.rest.v2/client-runtime "2.1.1"]
                 ;; [com.microsoft.azure/azure-storage-blob "11.0.1"]
                 [com.microsoft.azure/azure-storage "8.3.0"]
                 [techascent/tech.io "2.13"]]
  :profiles {:dev {:dependencies [[amperity/vault-clj "0.7.0"]
                                  [ch.qos.logback/logback-classic "1.1.3"]]}})
