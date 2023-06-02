(ns genegraph.framework.storage.gcs
  (:require [clojure.java.io :as io]
            [io.pedestal.log :as log]
            [genegraph.framework.storage :as storage])
  (:import [java.time ZonedDateTime ZoneOffset]
           java.time.format.DateTimeFormatter
           [java.nio ByteBuffer]
           [java.nio.file Path Paths]
           [java.io InputStream OutputStream FileInputStream File]
           [com.google.common.io ByteStreams]
           [com.google.cloud.storage Bucket BucketInfo Storage StorageOptions
            BlobId BlobInfo Blob]
           [com.google.cloud.storage Storage$BlobWriteOption
            Storage$BlobTargetOption
            Storage$BlobSourceOption
            Storage$BlobListOption
            Blob$BlobSourceOption]
           (com.google.cloud WriteChannel)
           (java.nio.channels WritableByteChannel Channels)
           (java.nio.charset StandardCharsets)))

(defn- storage []
  (.getService (StorageOptions/getDefaultInstance)))

(defn put-file-in-bucket!
  ([bucket source-file blob-name]
   (put-file-in-bucket! bucket
                        source-file
                        blob-name
                        {:content-type "application/gzip"}))
  ([bucket source-file blob-name options]
   (let [blob-id (BlobId/of bucket blob-name)
         blob-info (-> blob-id
                       BlobInfo/newBuilder
                       (.setContentType (:content-type options))
                       .build)
         from (.getChannel (FileInputStream. source-file))]
     (with-open [to (.writer
                     (storage)
                     blob-info
                     (make-array Storage$BlobWriteOption 0))]
       (ByteStreams/copy from to)))))

(defn get-file-from-bucket!
  [bucket source-blob target-file]
  (let [target-path (Paths/get target-file
                               (make-array java.lang.String 0))
        blob (.get (storage)
                   (BlobId/of bucket source-blob))]
    (.downloadTo blob target-path)))

(comment
  (put-file-in-bucket! "genegraph-framework-dev"
                       "/Users/tristan/Desktop/test.txt"
                       "test.txt")
  (get-file-from-bucket! "genegraph-framework-dev"
                         "test.txt"
                         "/Users/tristan/Desktop/test-back.txt")
  )

(defn ^WriteChannel open-write-channel
  "Returns a function which when called returns an open WriteChannel to blob-name"
  [^String bucket-name ^String blob-name]
  (let [gc-storage (.getService (StorageOptions/getDefaultInstance))
        blob-id (BlobId/of bucket-name blob-name)
        blob-info (-> blob-id BlobInfo/newBuilder (.setContentType (:content-type "application/gzip")) .build)]
    (.writer gc-storage blob-info (make-array Storage$BlobWriteOption 0))))

(comment
  (with-open [wc (get-write-channel "genegraph-framework-dev" "test.txt")
              os (Channels/newOutputStream wc)
              is (io/input-stream "/Users/tristan/desktop/test.txt")]
    (.transferTo is os))
  )

;; Functions below waiting for a use case in genegraph-framework
;; may want to handle channel functions at some point

#_(defn channel-write-string!
  "Write a string in UTF-8 to a WriteableByteChannel.
  Returns the channel for use in threading."
  [^WritableByteChannel channel ^String input-string]
  (.write channel (ByteBuffer/wrap (.getBytes input-string StandardCharsets/UTF_8)))
  channel)

#_(defn list-items-in-bucket
  ([] (list-items-in-bucket nil))
  ([prefix]
   (let [options (if prefix
                   (into-array [(Storage$BlobListOption/prefix prefix)])
                   (make-array Storage$BlobListOption 0))]
     (-> (.list (storage)
                env/genegraph-bucket
                options)
         .iterateAll
         .iterator
         iterator-seq))))

#_(defn get-files-with-prefix!
  "Store all files in bucket matching PREFIX to TARGET-DIR"
  [prefix target-dir]
  (if (fs/ensure-target-directory-exists! target-dir)
    (doseq [blob (list-items-in-bucket prefix)]
      (let [target-path (Paths/get (str target-dir
                                        (re-find #"/.*$" (.getName blob)))
                                   (make-array java.lang.String 0))]
        (.downloadTo blob target-path)))
    (log/error :fn ::get-files-with-prefix!
               :msg "Could not create directory"
               :path target-dir)))

#_(defn push-directory-to-bucket!
  "Copy all files from SOURCE-DIR to TARGET-DIR in bucket."
  [source-dir target-dir]
  (let [dir (io/file source-dir)]
    (doseq [file (.listFiles dir)]
      (put-file-in-bucket! file (str target-dir "/" (.getName file))))))
