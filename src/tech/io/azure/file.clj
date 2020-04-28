(ns tech.io.azure.file
  (:require [tech.io :as io]
            [tech.io.protocols :as io-prot]
            [tech.io.url :as url]
            [tech.io.auth :as io-auth]
            [tech.io.azure.auth :as azure-auth]
            [tech.io.azure.storage-account :as azure-storage-account]
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
  ^CloudFileClient
  [default-options options]
  (file-client (merge default-options options)))


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

(defn- url-parts->blob
  ^CloudFile [^CloudFileClient client
              url-parts & {:keys [blob-must-exist?
                                  create-container?]}]
  (let [container-name (url-parts->container url-parts)
        container (.getShareReference client container-name)
        _ (when (not (.exists container))
            (if (not create-container?)
              (throw (ex-info (format "Container does not exist: %s" container-name)
                              {}))
              (.createIfNotExists container)))
        root-dir (.getRootDirectoryReference container)
        blob (.getFileReference root-dir (url-parts->path url-parts))]
    (when (and blob-must-exist?
               (not (.exists blob)))
      (throw (ex-info (format "Blob does not exist: %s" (url/parts->url
                                                         url-parts))
                      {})))
    blob))

(defn- is-directory?
  [blob]
  (instance? CloudFileDirectory blob))


(defn- parent-seq
  [^ListFileItem item]
  (when item
    (cons item (lazy-seq (parent-seq (.getParent item))))))


(defn- get-full-name
  [blob]
  (cond
    (instance? CloudFile blob)
    (.getName ^CloudFile blob)
    (instance? CloudFileDirectory blob)
    (str
     (->> (parent-seq blob)
          (reverse)
          (map #(.getName ^CloudFileDirectory %))
          (s/join "/"))
     "/")))


(defn- blob->metadata-seq
  [recursive? container-name blob]
  (cond
    (instance? CloudFile blob)
    (let [^CloudFile blob blob]
      [{:url (str "azf://" container-name
                  "/"
                  (.getName blob))
        :byte-length (-> (.getProperties blob)
                         (.getLength))
        :public-url (-> (.getUri blob)
                        (.toString))}])
    (is-directory? blob)
    (if recursive?
      (->> (.listFilesAndDirectories ^CloudFileDirectory blob)
           (mapcat (partial blob->metadata-seq recursive? container-name)))
      [{:url (str "azf://" container-name (get-full-name blob))
        :directory? true}])
    :else
    (throw (Exception. (format "Failed to recognized %s"
                               (type blob))))))

(deftype BlobProvider [default-options]
  io-prot/IOProvider
  (input-stream [provider url-parts options]
    (let [^InputStream istream
          (-> (opts->client default-options options)
              second
              (url-parts->blob url-parts :blob-must-exist? true)
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
          ostream (ByteArrayOutputStream.)
          closer
          (delay
           (let [byte-data (.toByteArray ostream)
                 n-bytes (alength byte-data)
                 fstream
                 (-> client
                     (url-parts->blob url-parts
                                      :blob-must-exist? false
                                      :create-container?
                                      (:create-container? options)))]
             (.uploadFromByteArray fstream byte-data 0 n-bytes)))]
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
          ^CloudFileClient client client
          container-name (url-parts->container url-parts)
          path-data (url-parts->path url-parts)]
      (if-not path-data
        (let [containers (if-let [container-name (url-parts->container url-parts)]
                           [(.getShareReference client container-name)]
                           (.listShares client))]
          (->> containers
               (mapcat
                (fn [^CloudFileShare container]
                  (->> (.getRootDirectoryReference container)
                       (.listFilesAndDirectories)
                       (mapcat (partial blob->metadata-seq
                                        (:recursive? options)
                                        (.getName container))))))
               (remove nil?)))
        (let [container (-> (.getShareReference client container-name)
                            (.getRootDirectoryReference))
              target-blob (url-parts->blob client url-parts :blob-must-exist? false)
              dir-blob (.getDirectoryReference container (url-parts->path url-parts))]
          (cond
            (.exists target-blob)
            (blob->metadata-seq (:recursive? options)
                                (.getName container)
                                target-blob)
            :else
            (->> (.listFilesAndDirectories ^CloudFileDirectory dir-blob)
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
