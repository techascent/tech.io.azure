(ns tech.io.azure.blob-test
  (:require [tech.io :as io]
            [tech.io.auth :as io-auth]
            [tech.io.azure.auth :as azure-auth]
            [tech.io.azure.blob :as az-blob]
            [clojure.test :refer :all]))


(deftest base-test
  (with-bindings {#'az-blob/*default-azure-provider*
                  (io-auth/authenticated-provider
                   (az-blob/blob-provider {})
                   (azure-auth/azure-blob-auth-provider))}
    (let [blob-url "azb://test-container/projects/blob.clj"]
      (try
        (io/delete! blob-url)
        (catch Throwable e nil))
      (az-blob/ensure-container! "test-container")
      (io/copy "project.clj" "azb://test-container/projects/blob.clj")
      (is (= #{:byte-length :modify-date :create-date :public-url}
             (-> (io/metadata "azb://test-container/projects/blob.clj")
                 keys
                 set)))
      (let [non-rec-ls (io/ls "azb://test-container/")
            rec-ls (io/ls "azb://test-container/" :recursive? true)]
        (is (= (count non-rec-ls)
               (count rec-ls)))
        (is (= #{:url :byte-length :public-url}
               (->> (keys (first rec-ls))
                    set)))
        (io/delete! blob-url)
        (is (= (- (count non-rec-ls) 1)
               (count (io/ls "azb://test-container" :recursive? true))))))))
