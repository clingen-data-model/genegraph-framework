(ns genegraph.framework.storage.gcs
  (:require [clojure.java.io :as io]
            [io.pedestal.log :as log]
            [genegraph.framework.storage :as storage]
            [genegraph.framework.protocol :as p])
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




(defn write-input-stream-to-bucket [bucket k is]
  (with-open [wc (open-write-channel bucket k)
              os (Channels/newOutputStream wc)]
    (.transferTo is os)))


(defn open-input-stream-on-bucket-object [bucket k]
  (Channels/newInputStream
   (.reader (storage)
            bucket
            k
            (make-array Storage$BlobSourceOption 0))))

(comment
  (with-open [ic (open-input-stream-on-bucket-object
                  "genegraph-framework-dev"
                  "test-file.txt")]
    (slurp ic))
  )


(defn list-items-in-bucket
  ([bucket] (list-items-in-bucket bucket nil))
  ([bucket prefix]
   (let [options (if prefix
                   (into-array [(Storage$BlobListOption/prefix prefix)])
                   (make-array Storage$BlobListOption 0))]
     (-> (.list (storage)
                bucket
                options)
         .iterateAll
         .iterator
         iterator-seq))))
(comment
  (list-items-in-bucket "genegraph-framework-dev" "test")

  )

(defrecord GCSBucket [bucket]

  ;; key is expected to be reasonable blob ID,
  ;; value is expected to be an InputStream.
  ;; functionality may be added later to allow queued,
  ;; asynchronous writes.
  storage/IndexedWrite
  (write [this k v]
    (write-input-stream-to-bucket bucket k v))
  (write [this k v commit-promise]
    (write-input-stream-to-bucket bucket k v)
    (deliver commit-promise true))

  ;; 
  storage/IndexedRead
  (read [this k]
    (open-input-stream-on-bucket-object bucket k))

  storage/RangeRead
  (scan [this prefix]
    (list-items-in-bucket bucket prefix))
  ;; Not supporting the 'end' part of this at the moment
  ;; this is currently only needed for a special use case anyawy.
  (scan [this begin end]
    (storage/scan this begin))
  
  
  )

(comment
  (with-open [is (io/input-stream "/Users/tristan/Desktop/test.txt")]
    (storage/write (map->GCSBucket {:bucket "genegraph-framework-dev"})
                   "test-file.txt"
                   is))
  (slurp
   (storage/read (map->GCSBucket {:bucket "genegraph-framework-dev"})
                 "test-file.txt"))

  )

;; Communication with Google Cloud does not require the
;; same sort of initialization as some of the other storage
;; systems do (or to the extent it does, it is handled by
;; upstream libraries. Most of this is included for compatibility
;; with other parts of the storage infrastructure.

;; That being said, once could imagine attaching other 
(defrecord GCS [name
                type
                bucket
                state
                instance]

  p/Lifecycle
  (start [this]
    (reset! state :started)
    this)
  (stop [this]
    (reset! state :stopped)
    this))

(defmethod p/init :gcs-bucket [bucket-def]
  (map->GCS
   (assoc bucket-def
          :state (atom :stopped)
          :instance (atom (map->GCSBucket bucket-def)))))

(extend-type Blob
  storage/HandleExists
  (storage/exists? [this]
    (< 0 (.getSize this)))

  storage/HandleOps
  (storage/delete-handle [this]
    (.delete this (make-array Blob$BlobSourceOption 0)))
  
  io/IOFactory
  (io/make-input-stream [this opts]
    (-> this
        (.reader (make-array Blob$BlobSourceOption 0))
        Channels/newInputStream))
  (io/make-output-stream [this opts]
    (-> this
        (.writer (make-array Storage$BlobWriteOption 0))
        Channels/newOutputStream))
  (io/make-reader [this opts]
    (-> this
        (.reader (make-array Blob$BlobSourceOption 0))
        (Channels/newReader StandardCharsets/UTF_8)))
  (io/make-writer [this opts]
        (-> this
        (.writer (make-array Storage$BlobWriteOption 0))
        (Channels/newWriter StandardCharsets/UTF_8))))


(defmethod storage/as-handle :gcs [def]
  (let [blob-id (BlobId/of (:bucket def) (str (:base def) (:path def)))]
    (if-let [blob (.get (storage) blob-id)]
      blob
      (.create (storage)
               (-> (BlobInfo/newBuilder blob-id)
                   (.setContentType "text/plain")
                   .build)
               (make-array Storage$BlobTargetOption 0)))))


(comment
  (slurp
   (storage/as-handle {:type :gcs
                       :bucket "genegraph-framework-dev"
                       :path "test.txt"}))

  (storage/as-handle {:type :gcs
                      :bucket "genegraph-framework-dev"
                      :path "test12346.txt"} )
  (def s (slurp
          (storage/as-handle {:type :gcs
                              :bucket "genegraph-framework-dev"
                              :base "current-base/"
                              :path "affiliations.csv"})))

  (def b (p/init {:type :gcs-bucket
                  :name :dev-bucket
                  :bucket "genegraph-framework-dev"}))
  (p/start b)
  (p/stop b)
  (storage/read @(:instance b) )
  (->> (storage/scan @(:instance b) "test")
       first
       io/reader
       slurp)

  (-> (storage/scan @(:instance b) "test")
      first
      io/writer
      (spit "this is another modified test file"))


  (-> {:type :gcs
       :bucket "genegraph-framework-dev"
       :path "no-snapshot1"}
      storage/as-handle
      storage/exists?
      #_(.exists (make-array Blob$BlobSourceOption 0)))

  
  )

;; Functions below waiting for a use case in genegraph-framework
;; may want to handle channel functions at some point

#_(defn channel-write-string!
  "Write a string in UTF-8 to a WriteableByteChannel.
  Returns the channel for use in threading."
  [^WritableByteChannel channel ^String input-string]
  (.write channel (ByteBuffer/wrap (.getBytes input-string StandardCharsets/UTF_8)))
  channel)

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
