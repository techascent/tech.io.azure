(ns tech.io.azure.file
  (:require [tech.v3.io :as io]
            [tech.v3.io.protocols :as io-prot]
            [tech.v3.io.url :as url]
            [tech.v3.io.auth :as io-auth]
            [tech.v3.io.azure.auth :as azure-auth]
            [tech.v3.io.azure.storage-account :as azure-storage-account]
            [tech.config.core :as config]
            [clojure.tools.logging :as log]
            [clojure.string :as s])
  (:import [com.microsoft.azure.storage.file
            CloudFileClient
            CloudFileShare
            CloudFileDirectory
            CloudFile
            ListFileItem]
           [java.io ByteArrayOutputStream
            InputStream OutputStream]))


(set! *warn-on-reflection* true)


(defn file-client
  (^CloudFileClient [options]
   (-> (azure-storage-account/storage-account options)
       (.createCloudFileClient)))
  (^CloudFileClient [] (file-client {})))


(defn- opts->client
  [default-options options]
  [options (file-client (merge default-options options))])


(defn ensure-share!
  (^CloudFileShare [^CloudFileClient client container-name]
   (let [retval (.getShareReference client container-name)]
     (.createIfNotExists retval)
     retval))
  (^CloudFileShare [container-name]
   (ensure-share! (file-client) container-name)))

(defn url-parts->container
  [{:keys [path]}]
  (first path))

(defn url-parts->path
  [{:keys [path]}]
  (when (seq (rest path))
    (s/join "/" (rest path))))

(defn- url-parts->file
  ^CloudFile [^CloudFileClient client
              url-parts & {:keys [file-must-exist?
                                  create-container?]}]
  (let [container-name (url-parts->container url-parts)
        container (.getShareReference client container-name)
        _ (when (not (.exists container))
            (if (not create-container?)
              (throw (ex-info (format "Container does not exist: %s" container-name)
                              {}))
              (.createIfNotExists container)))
        root-dir (.getRootDirectoryReference container)
        file (.getFileReference root-dir (url-parts->path url-parts))]
    (when (and file-must-exist?
               (not (.exists file)))
      (throw (ex-info (format "File does not exist: %s" (url/parts->url
                                                         url-parts))
                      {})))
    file))

(defn- is-directory?
  [file]
  (instance? CloudFileDirectory file))


(defn- parent-seq
  [^ListFileItem item]
  (when item
    (cons item (lazy-seq (parent-seq (.getParent item))))))


(defn- last-name
  [^ListFileItem item]
  (cond
    (instance? CloudFile item)
    (last (s/split (.getName ^CloudFile item) #"/"))
    (instance? CloudFileDirectory item)
    (last (s/split (.getName ^CloudFileDirectory item) #"/"))
    :else
    (throw (Exception. "Type failure in last-name"))))


(defn- get-full-name
  [file]
  (cond
    (instance? CloudFile file)
    (str (get-full-name
          (.getParent ^CloudFile file))
         (last-name file))
    (instance? CloudFileDirectory file)
    (when-let [name-seq (->> (parent-seq file)
                             (reverse)
                             (map #(last-name %))
                             (remove empty?)
                             (seq))]
      (str (s/join "/" name-seq) "/"))))


(defn- file->metadata-seq
  [recursive? container-name file]
  (cond
    (instance? CloudFile file)
    (let [^CloudFile file file]
      [{:url (str "azf://" container-name
                  "/"
                  (get-full-name file))
        :byte-length (-> (.getProperties file)
                         (.getLength))
        :public-url (-> (.getUri file)
                        (.toString))}])
    (is-directory? file)
    (if recursive?
      (->> (.listFilesAndDirectories ^CloudFileDirectory file)
           (mapcat (partial file->metadata-seq recursive? container-name)))
      [{:url (str "azf://" container-name "/" (get-full-name file))
        :directory? true}])
    :else
    (throw (Exception. (format "Failed to recognized %s"
                               (type file))))))


(deftype FileProvider [default-options]
  io-prot/IOProvider
  (input-stream [provider url-parts options]
    (let [^InputStream istream
          (-> (opts->client default-options options)
              second
              (url-parts->file url-parts :file-must-exist? true)
              (.openRead))
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
          ;;We have to have the length to open the stream which means
          ;;this has to be delayed.  So writing terabyte files ain't
          ;;gonna work :-).
          file-ref
          (-> client
              (url-parts->file url-parts
                               :file-must-exist? false
                               :create-container?
                               (:create-container? options)))
          ostream (ByteArrayOutputStream.)
          closer
          (delay
           (let [byte-data (.toByteArray ostream)
                 n-bytes (alength byte-data)]
             (.uploadFromByteArray file-ref byte-data 0 n-bytes)))]
      ;;The default we always wanted
      (doseq [parent (reverse (parent-seq (.getParent file-ref)))]
        (.createIfNotExists ^CloudFileDirectory parent))
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
        (url-parts->file url-parts :file-must-exist? false)
        (.exists)))
  (delete! [provider url-parts options]
    (let [file (-> (opts->client default-options options)
                   second
                   (url-parts->file url-parts :file-must-exist? false))]
      (when (.exists file)
        (.delete file))
      :ok))
  (ls [provider url-parts options]
    (let [[options client] (opts->client default-options options)
          ^CloudFileClient client client
          container-name (url-parts->container url-parts)
          path-data (url-parts->path url-parts)
          recursive? (:recursive? options)
          metadata-seq-fn (partial file->metadata-seq recursive? container-name)]
      (if-not path-data
        (if container-name
          (->> (.getShareReference client container-name)
               (.getRootDirectoryReference)
               (.listFilesAndDirectories)
               (mapcat metadata-seq-fn))
          (->> (.listShares client)
               (mapcat
                (fn [^CloudFileShare container]
                  (concat
                   [{:url (str "azf://" (.getName container))
                     :directory? true}]
                   (when recursive?
                     (metadata-seq-fn (.getRootDirectoryReference container))))))))
        (let [container (-> (.getShareReference client container-name)
                            (.getRootDirectoryReference))
              target-file (url-parts->file client url-parts :file-must-exist? false)
              dir-file (.getDirectoryReference container (url-parts->path url-parts))]
          (cond
            (.exists target-file)
            (metadata-seq-fn target-file)
            :else
            (->> (.listFilesAndDirectories ^CloudFileDirectory dir-file)
                 (mapcat metadata-seq-fn)))))))
  (metadata [provider url-parts options]
    (let [file (-> (opts->client default-options options)
                   second
                   (url-parts->file url-parts :file-must-exist? true))
          properties (.getProperties file)]
      {:byte-length (.getLength properties)
       :modify-date (.getLastModified properties)
       :public-url (-> (.getUri file)
                       (.toString))}))

  io-prot/ICopyObject
  (get-object [provider url-parts options]
    (io-prot/input-stream provider url-parts options))
  (put-object! [provider url-parts value options]
    (let [file (-> (opts->client default-options options)
                   second
                   (url-parts->file url-parts
                                    :file-must-exist? false
                                    :create-container? (:create-container? options)))]
      (cond
        (instance? (Class/forName "[B") value)
        (.uploadFromByteArray file ^bytes value 0
                              (alength ^bytes value))
        :else
        (let [filedata (io/file value)]
          (.uploadFromFile file (.getCanonicalPath filedata)))))))


(defn create-default-azure-provider
  []
  (let [provider (FileProvider. {})]
    (if (config/get-config :tech-io-vault-auth)
      (io-auth/authenticated-provider
       provider
       (azure-auth/azure-blob-auth-provider))
      provider)))


(def ^:dynamic *default-azure-provider*
  (create-default-azure-provider))


(defmethod io-prot/url-parts->provider :azf
  [& args]
  *default-azure-provider*)
