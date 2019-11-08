(ns tech.io.azure.blob
  (:require [tech.io.protocols :as io-prot]
            [tech.io.url :as url]
            [clojure.string :as s]
            [tech.io :as io]
            [tech.io.auth :as io-auth]
            [tech.io.azure.auth :as azure-auth]
            [tech.config.core :as config]
            [clojure.tools.logging :as log])
  (:import [com.microsoft.azure.storage.blob CloudBlobClient CloudBlobContainer
            CloudBlob CloudBlockBlob CloudBlobDirectory]
           [com.microsoft.azure.storage
            StorageUri StorageCredentials StorageCredentialsAccountAndKey
            CloudStorageAccount]
           [java.io InputStream OutputStream File]))


(set! *warn-on-reflection* true)

(defn blob-client
  ^CloudBlobClient [& [options]]
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
        (CloudStorageAccount. true)
        (.createCloudBlobClient))))


(defn url-parts->container
  [{:keys [path]}]
  (first path))


(defn url-parts->path
  [{:keys [path]}]
  (when (seq (rest path))
    (s/join "/" (rest path))))

(defn- opts->client
  [default-options options]
  (let [options (merge default-options options)
        ;;Use environment variable fallback if not provided by options.
        client (blob-client options)]
    [options client]))


(defn ensure-container!
  ^CloudBlobContainer [^CloudBlobClient client container-name]
  (let [container (.getContainerReference client container-name)]
    (.createIfNotExists container)
    container))


(defn- url-parts->blob
  ^CloudBlockBlob [^CloudBlobClient client
                   url-parts & {:keys [blob-must-exist?
                                       create-container?]}]
  (let [container-name (url-parts->container url-parts)
        container (.getContainerReference client container-name)
        _ (when (not (.exists container))
            (if (not create-container?)
              (throw (ex-info (format "Container does not exist: %s" container-name)
                              {}))
              (.createIfNotExists container)))
        blob (.getBlockBlobReference container (url-parts->path url-parts))]
    (when (and blob-must-exist?
               (not (.exists blob)))
      (throw (ex-info (format "Blob does not exist: %s" (url/parts->url
                                                         url-parts))
                      {})))
    blob))


(defn- is-directory?
  [blob]
    (instance? CloudBlobDirectory blob))

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
    (is-directory? blob)
    (if recursive?
      (->> (.listBlobs ^CloudBlobDirectory blob)
           (mapcat (partial blob->metadata-seq recursive? container-name)))
      [{:url (str "azb://" container-name "/" (.getPrefix ^CloudBlobDirectory blob))
        :directory? true}])))


(defrecord BlobProvider [default-options]
  io-prot/IOProvider
  (input-stream [provider url-parts options]
    (let [^InputStream istream
          (-> (opts->client default-options options)
              second
              (url-parts->blob url-parts :blob-must-exist? true)
              (.openInputStream))
          closer (delay (.close istream))]
      ;;These @#$@#$ streams throw exception upon double close
      ;;which doesn't follow java conventions in other places.
      (proxy [InputStream] []
        (available [] (.available istream))
        (close [] @closer)
 	(mark [readlimit] (.mark istream readlimit))
        (markSupported [] (.markSupported istream))
        (read
          ([] (.read istream))
          ([b] (.read istream b))
          ([b off len] (.read istream b off len)))
       	(reset [] (.reset istream))
 	(skip [n] (.skip istream n)))))
  (output-stream! [provider url-parts options]
    (let [[options client] (opts->client default-options options)
          ^OutputStream ostream
          (-> client
              (url-parts->blob url-parts
                               :blob-must-exist? false
                               :create-container? (:create-container? options))
              (.openOutputStream))
          closer (delay (.close ostream))]
      (proxy [OutputStream] []
        (close [] @closer)
        (flush [] (.flush ostream))
        (write
          ([b off len] (.write ostream b off len))
          ([b] (if (number? b)
                 (.write ostream (int b))
                 (.write ostream ^bytes b)))))))
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
          container-name (url-parts->container url-parts)
          path-data (url-parts->path url-parts)]
      (if-not path-data
        (let [containers (if-let [container-name (url-parts->container url-parts)]
                           [(.getContainerReference client container-name)]
                           (.listContainers client))]
          (->> containers
               (mapcat
                (fn [^CloudBlobContainer container]
                  (->> (.listBlobs container)
                       (mapcat (partial blob->metadata-seq
                                        (:recursive? options)
                                        (.getName container))))))
               (remove nil?)))
        (let [container (.getContainerReference client container-name)
              target-blob (url-parts->blob client url-parts :blob-must-exist? false)
              dir-blob (.getDirectoryReference container (url-parts->path url-parts))]
          (cond
            (.exists target-blob)
            (blob->metadata-seq (:recursive? options)
                                (.getName container)
                                target-blob)
            :else
            (->> (.listBlobs ^CloudBlobDirectory dir-blob)
                 (mapcat (partial blob->metadata-seq
                                  (:recursive? options)
                                  (.getName container)))))))))
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
                   (url-parts->blob url-parts
                                    :blob-must-exist? false
                                    :create-container? (:create-container? options)))]
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
