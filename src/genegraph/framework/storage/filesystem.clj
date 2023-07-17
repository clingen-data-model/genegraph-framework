(ns genegraph.framework.storage.filesystem
  "Methods for incorporating a filesystem into the Genegraph storage framework.
  Currently supported primarily for the purpose of "
  (:require [genegraph.framework.storage :as storage]
            [clojure.java.io :as io])
  (:import [java.io OutputStream InputStream File]))

(defrecord Filesystem [path]
  storage/IndexedWrite
  (write [this k v]
    (with-open [os (io/output-stream (str path k))]
      (.transferTo (io/input-stream v) os)))
  (write [this k v commit-promise]
    (storage/write this k v)
    (deliver commit-promise true))

  storage/IndexedRead
  (read [this k]
    (io/input-stream (str path k)))

  ;; corresponds to 'list files in directory
  storage/RangeRead
  (scan [this prefix]
    (->> (str path prefix)
         io/file
         file-seq
         (filter #(.isFile %))))
  ;; Not supporting the 'end' part of this at the moment
  ;; this is currently only needed for a special use case anyawy.
  (scan [this begin end]
    (storage/scan this begin)))

(comment
  (-> (->Filesystem "/users/tristan/desktop/test-file-store/")
      (storage/write "test-file.txt" "this is a local test file"))

  (clojure.java.io/input-stream (.getBytes "this"))
  
  (-> (->Filesystem "/users/tristan/desktop/test-file-store/")
      (storage/write "test-file.txt" (clojure.java.io/input-stream
                                      (.getBytes "this is a local test file too"))))

  (-> (->Filesystem "/Users/tristan/data/genegraph/2023-04-13T1609/")
      (storage/scan "base"))
  
  )


(defrecord FilesystemDefinition [name
                                 type
                                 path
                                 state
                                 instance])



