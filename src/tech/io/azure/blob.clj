(ns tech.io.azure.blob
  (:import [com.microsoft.azure.storage.blob CloudBlobClient CloudBlobContainer]
           [com.microsoft.azure.storage
            StorageUri StorageCredentials StorageCredentialsAccountAndKey
            CloudStorageAccount]
           [java.net URI]))

(defn blob-client
  ^CloudBlobClient [^String account-name ^String account-key]
  (-> (StorageCredentialsAccountAndKey. account-name account-key)
      ;;true means to use https
      (CloudStorageAccount. true)
      (.createCloudBlobClient)))


(comment
  (require '[tech.io.azure.auth :as azure-auth])
  (require '[tech.config.core :as config])
  (def creds (azure-auth/vault-azure-blob-creds
              (config/get-config :tech-azure-blob-vault-path) {}))
  (def account-key (:tech.azure.blob/account-key creds))
  (def account-name (:tech.azure.blob/account-name creds))
  (def client (blob-client account-name account-key))
  (def containers (vec (.listContainers client)))
  )
