(ns tech.io.azure.blob
  (:require [tech.io.protocols :as io-prot]
            [tech.io.url :as url]
            [clojure.string :as s])
  (:import [com.microsoft.azure.storage.blob CloudBlobClient CloudBlobContainer
            CloudBlob]
           [com.microsoft.azure.storage
            StorageUri StorageCredentials StorageCredentialsAccountAndKey
            CloudStorageAccount]
           [java.io InputStream OutputStream File]))

(set! *warn-on-reflection* true)

(defn blob-client
  ^CloudBlobClient [^String account-name ^String account-key]
  (-> (StorageCredentialsAccountAndKey. account-name account-key)
      ;;true means to use https
      (CloudStorageAccount. true)
      (.createCloudBlobClient)))


(defn url-parts->container
  [{:keys [path]}]
  (first path))


(defn url-parts->path
  [{:keys [path]}]
  (s/join "/" (rest path)))


(defrecord BlobProvider [default-options]
  io-prot/IOProvider
  (input-stream [provider url-parts options]
    (let [options (merge default-options options)
          client (blob-client (:tech.azure.blob/account-name options)
                              (:tech.azure.blob/account-key options))
          container-name (url-parts->container url-parts)
          container (.getContainerReference client container-name)
          _ (when-not (.exists container)
              (throw (ex-info (format "Container does not exist: %s" container-name)
                              {})))
          blob (.getBlockBlobReference container (url-parts->path url-parts))
          _ (when-not (.exists blob)
              (throw (ex-info (format "Blob does not exist: %s" (url/parts->url
                                                                 url-parts))
                              {})))]
      (.openInputStream blob)))
  (output-stream! [provider url-parts options]
    (let [options (merge default-options options)
          client (blob-client (:tech.azure.blob/account-name options)
                              (:tech.azure.blob/account-key options))
          container-name (url-parts->container url-parts)
          container (.getContainerReference client container-name)
          _ (when-not (.exists container)
              (throw (ex-info (format "Container does not exist: %s" container-name)
                              {})))
          blob (.getBlockBlobReference container (url-parts->path url-parts))]
      (.openOutputStream blob)))
  (exists? [provider url-parts options]
    (let [options (merge default-options options)
          client (blob-client (:tech.azure.blob/account-name options)
                              (:tech.azure.blob/account-key options))
          container-name (url-parts->container url-parts)
          container (.getContainerReference client container-name)
          _ (when-not (.exists container)
              (throw (ex-info (format "Container does not exist: %s" container-name)
                              {})))
          blob (.getBlockBlobReference container (url-parts->path url-parts))]
      (.exists blob)))
  (delete! [provider url-parts options]
    (let [options (merge default-options options)
          client (blob-client (:tech.azure.blob/account-name options)
                              (:tech.azure.blob/account-key options))
          container-name (url-parts->container url-parts)
          container (.getContainerReference client container-name)
          _ (when-not (.exists container)
              (throw (ex-info (format "Container does not exist: %s" container-name)
                              {})))
          blob (.getBlockBlobReference container (url-parts->path url-parts))]
      (when (.exists blob)
        (.delete blob))))
  (ls [provider url-parts options]
    (let [options (merge default-options options)
          client (blob-client (:tech.azure.blob/account-name options)
                              (:tech.azure.blob/account-key options))

          containers (if-let [container-name (url-parts->container url-parts)]
                       [(.getContainerReference client container-name)]
                       (.listContainers client))]
      (->> containers
           (mapcat
            (fn [^CloudBlobContainer container]
              (let [blobs (.listBlobs container)]
                (->> blobs
                     (map
                      (fn [^CloudBlob blob]
                        {:url (str "azb://" (.getName container)
                                   "/"
                                   (.getName blob))
                         :length (-> (.getProperties blob)
                                     (.getLength))
                         :public-url (-> (.getUri blob)
                                         (.toString))}))))))))))


(defn blob-provider
  [default-options]
  (->BlobProvider default-options))


(defmethod io-prot/url-parts->provider :azb
  [& args]
  (blob-provider))

(comment
  (require '[tech.io.azure.auth :as azure-auth])
  (require '[tech.config.core :as config])
  (def creds (azure-auth/vault-azure-blob-creds
              (config/get-config :tech-azure-blob-vault-path) {}))
  (def account-key (:tech.azure.blob/account-key creds))
  (def account-name (:tech.azure.blob/account-name creds))
  (def client (blob-client account-name account-key))
  (def containers (vec (.listContainers client)))
  (def container (first containers))
  (def blobs (.listBlobs container))
  (def blob (first blobs))
  )
