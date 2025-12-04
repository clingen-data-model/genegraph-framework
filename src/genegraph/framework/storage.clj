(ns genegraph.framework.storage
  (:require [clojure.java.io :as io])
  (:import [java.io
            ByteArrayInputStream File BufferedOutputStream BufferedInputStream]
           [java.nio.file
            Path Files StandardCopyOption LinkOption]
           [java.nio.file.attribute FileAttribute]
           [org.apache.commons.compress.archivers.tar
            TarArchiveEntry TarArchiveOutputStream TarArchiveInputStream]
           [org.apache.commons.compress.archivers ArchiveEntry]
           [org.apache.commons.compress.compressors.lz4
            BlockLZ4CompressorOutputStream BlockLZ4CompressorInputStream FramedLZ4CompressorInputStream FramedLZ4CompressorOutputStream]
           [org.apache.commons.compress.compressors.gzip
            GzipCompressorOutputStream GzipCompressorInputStream]
           [java.time Instant])
  (:refer-clojure :exclude [read]))

(defprotocol HasInstance
  (instance [this]))

(defprotocol IndexedWrite
  (write [this k v] [this k v commit-promise]))

(defprotocol IndexedRead
  (read [this k]))

(defprotocol IndexedDelete
  (delete [this k] [this k commit-promise]))

(defprotocol Transactional
  (begin [this])
  (end [this]))

(defprotocol RangeRead
  (scan [this prefix] [this begin end]))

(defprotocol RangeDelete
  (range-delete [this prefix] [this begin end]))

(defprotocol TopicBackingStore
  (store-offset [this topic offset] [this topic offset commit-promise])
  (retrieve-offset [this topic]))

(defprotocol Snapshot
  (store-snapshot [this])
  (restore-snapshot [this]))

(defprotocol HandleExists
  (exists? [this]))

(defprotocol HandleOps
  (delete-handle [this]))

(defmulti as-handle :type)

(defmethod as-handle :file [def]
  (io/file (:base def) (:path def)))

(extend-type File
  HandleExists
  (exists? [this] (.exists this))

  HandleOps
  (delete-handle [this] (.delete this)))

(defn ->input-stream [source]
  (cond (map? source) (io/input-stream (as-handle source))
        (string? source) (ByteArrayInputStream. (.getBytes source))
        :else (io/input-stream source)))

;; Loosely moddeled from https://mkyong.com/java/how-to-create-tar-gz-in-java/
;; using lz4 instead of gzx

(defn store-archive [local-path storage-handle]
  (let [local-path-obj (Path/of local-path (make-array String 0))]
    (with-open [os (-> storage-handle
                       as-handle
                       io/output-stream
                       BufferedOutputStream.
                       FramedLZ4CompressorOutputStream.
                       TarArchiveOutputStream.)]
      (run! (fn [f]
              (.putArchiveEntry
               os
               (TarArchiveEntry.
                f
                (str
                 (.relativize local-path-obj
                              (Path/of (.getPath f)
                                       (make-array String 0))))))
              (io/copy f os)
              (.closeArchiveEntry os))
            (filter #(.isFile %) (file-seq (io/file local-path)))))))

(comment
  (store-archive "/Users/tristan/data/genegraph-neo/test-rocks-snapshot"
                 {:type :gcs
                  :bucket "genegraph-framework-dev"
                  :path "test-tarball.tar.gz"})
  )

(defn restore-archive [target-path storage-handle]
  (let [base-path (Path/of target-path (make-array String 0))]
    (with-open [is (-> storage-handle
                       as-handle
                       io/input-stream
                       BufferedInputStream.
                       FramedLZ4CompressorInputStream.
                       TarArchiveInputStream.)]
      (loop [entry (.getNextEntry is)]
        (when entry
          (let [file-path (.resolve base-path (.getName entry))
                parent-path (.getParent file-path)]
            (when (Files/notExists parent-path (make-array LinkOption 0))
              (Files/createDirectories parent-path (make-array FileAttribute 0)))
            (io/copy is (.toFile file-path)))
          (recur (.getNextEntry is)))))))


(comment
  (restore-archive
   "/Users/tristan/data/genegraph-neo/test-rocks-restore"
   {:type :file
    :base "/users/tristan/data/genegraph-neo"
    :path "test-tarball.tar.lz4"})
  )

#_(defn store-snapshot [this]
  (store-archive (:path this) (:snapshot-handle this)))

#_(defn restore-snapshot [this]
  (when-not (-> this :path io/as-file .exists)
    (restore-archive (:path this) (:snapshot-handle this))))

(comment
  (as-handle {:type :file
              :base "/users/tristan/data"
              :path "edntest.edn"}))
