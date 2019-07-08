(ns tech.io.azure.blob
  (:require [tech.io.protocols :as io-prot]
            [tech.io.url :as url]
            [clojure.string :as s]
            [tech.io :as io]
            [tech.io.auth :as io-auth]
            [tech.io.azure.auth :as azure-auth]
            [tech.config.core :as config])
  (:import [com.microsoft.azure.storage.blob CloudBlobClient CloudBlobContainer
            CloudBlob CloudBlockBlob CloudBlobDirectory]
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

(defn- opts->client
  [default-options options]
  (let [options (merge default-options options)
        ;;Use environment variable fallback if not provided by options.
        account-name (or (:tech.azure.blob/account-name options)
                         (config/unchecked-get-config :azure-blob-account-name))
        account-key (or (:tech.azure.blob/account-key options)
                        (config/unchecked-get-config :azure-blob-account-key))
        client (blob-client account-name account-key)]
    [options client]))


(defn- url-parts->blob
  ^CloudBlockBlob [^CloudBlobClient client
                   url-parts & {:keys [blob-must-exist?]}]
  (let [container-name (url-parts->container url-parts)
        container (.getContainerReference client container-name)
        _ (when (not (.exists container))
            (throw (ex-info (format "Container does not exist: %s" container-name)
                            {})))
        blob (.getBlockBlobReference container (url-parts->path url-parts))]
    (when (and blob-must-exist?
               (not (.exists blob)))
      (throw (ex-info (format "Blob does not exist: %s" (url/parts->url
                                                         url-parts))
                      {})))
    blob))

(defn- blob->metadata-seq
  [recursive? container-name blob]
  (cond
    (instance? CloudBlob blob)
    (let [^CloudBlob blob blob]
      [{:url (str "azb://" container-name
                  "/"
                  (.getName blob))
        :byte-length (-> (.getProperties blob)
                         (.getLength))
        :public-url (-> (.getUri blob)
                        (.toString))}])
    (and recursive?
         (instance? CloudBlobDirectory blob))
    (->> (.listBlobs ^CloudBlobDirectory blob)
         (mapcat (partial blob->metadata-seq recursive? container-name)))))


(defrecord BlobProvider [default-options]
  io-prot/IOProvider
  (input-stream [provider url-parts options]
    (-> (opts->client default-options options)
        second
        (url-parts->blob url-parts :blob-must-exist? true)
        (.openInputStream)))
  (output-stream! [provider url-parts options]
    (-> (opts->client default-options options)
        second
        (url-parts->blob url-parts :blob-must-exist? false)
        (.openOutputStream)))
  (exists? [provider url-parts options]
    (-> (opts->client default-options options)
        second
        (url-parts->blob url-parts :blob-must-exist? false)
        (.exists)))
  (delete! [provider url-parts options]
    (let [blob (-> (opts->client default-options options)
                   second
                   (url-parts->blob url-parts :blob-must-exist? false))]
      (when (.exists blob)
        (.delete blob))
      :ok))
  (ls [provider url-parts options]
    (let [[options client] (opts->client default-options options)
          ^CloudBlobClient client client
          containers (if-let [container-name (url-parts->container url-parts)]
                       [(.getContainerReference client container-name)]
                       (.listContainers client))]
      (->> containers
           (mapcat
            (fn [^CloudBlobContainer container]
              (->> (.listBlobs container)
                   (mapcat (partial blob->metadata-seq
                                    (:recursive? options)
                                    (.getName container))))))
           (remove nil?))))
  (metadata [provider url-parts options]
    (let [blob (-> (opts->client default-options options)
                         second
                         (url-parts->blob url-parts :blob-must-exist? true))
          properties (.getProperties blob)]
      {:byte-length (.getLength properties)
       :modify-date (.getLastModified properties)
       :create-date (.getCreatedTime properties)
       :public-url (-> (.getUri blob)
                       (.toString))}))

  io-prot/ICopyObject
  (get-object [provider url-parts options]
    (io-prot/input-stream provider url-parts options))
  (put-object! [provider url-parts value options]
    (let [blob (-> (opts->client default-options options)
                   second
                   (url-parts->blob url-parts :blob-must-exist? false))]
      (cond
        (instance? (Class/forName "[B") value)
        (.uploadFromByteArray blob ^bytes value 0
                              (alength ^bytes value))
        :else
        (let [file (io/file value)]
          (.uploadFromFile blob (.getCanonicalPath file)))))))


(defn blob-provider
  [default-options]
  (->BlobProvider default-options))


(defn create-default-azure-provider
  []
  (let [provider (blob-provider {})]
    (if (config/get-config :tech-io-vault-auth)
      (io-auth/authenticated-provider
       provider
       (azure-auth/azure-blob-auth-provider))
      provider)))


(def ^:dynamic *default-azure-provider*
  (create-default-azure-provider))


(defmethod io-prot/url-parts->provider :azb
  [& args]
  *default-azure-provider*)

(comment
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
