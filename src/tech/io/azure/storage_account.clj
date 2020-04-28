(ns tech.io.azure.storage-account
  (:require [tech.config.core :as config])
  (:import [com.microsoft.azure.storage
            StorageUri StorageCredentials StorageCredentialsAccountAndKey
            CloudStorageAccount]))


(set! *warn-on-reflection* true)


(defn storage-account
  (^CloudStorageAccount [options]
   (let [account-name (or (:tech.azure.blob/account-name options)
                          (config/unchecked-get-config :azure-blob-account-name))
         account-key (or (:tech.azure.blob/account-key options)
                         (config/unchecked-get-config :azure-blob-account-key))
         _ (when (or (= 0 (count account-name))
                     (= 0 (count account-key)))
             (throw (ex-info
                     (format "Could not find account name (%s) or account key %s
Consider setting environment variables AZURE_BLOB_ACCOUNT_NAME and AZURE_BLOB_ACCOUNT_KEY"
                             account-name account-key)
                     {})))]
     (-> (StorageCredentialsAccountAndKey. ^String account-name
                                           ^String account-key)
         ;;true means to use https
         (CloudStorageAccount. true))))
  (^CloudStorageAccount [] (storage-account {})))
