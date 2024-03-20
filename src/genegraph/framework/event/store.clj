(ns genegraph.framework.event.store
  "Function for handling a store of events."
  (:require [clojure.edn :as edn]
            [clojure.pprint :refer [pprint]]
            [clojure.java.io :as io]
            [io.pedestal.log :as log])
  (:import [java.io File PushbackReader FileOutputStream BufferedWriter FileInputStream BufferedReader]
           [java.nio ByteBuffer]
           [java.util.zip GZIPInputStream GZIPOutputStream]))

(defn read-next-val [pbr]
  (try
    (edn/read pbr)
    (catch Exception e
      :EOF)))

(defn event-seq [pbr]
  (let [v (read-next-val pbr)]
    (if (= :EOF v)
      ()
      (cons v (lazy-seq (event-seq pbr))))))

(defmacro with-event-reader [[binding handle] & body]
  `(with-open [is# (io/input-stream ~handle)
               gzis# (GZIPInputStream. is#)
               r# (io/reader gzis#)
               ~binding (PushbackReader. r#)]
     (do ~@body)))

(defmacro with-event-writer [[binding handle] & body]
  `(with-open [os# (io/output-stream ~handle)
               gzos# (GZIPOutputStream. os#)
               ~binding (io/writer gzos#)]
     (binding [*out* ~binding]
       (do ~@body))))

(defn store-events [handle event-seq]
  (with-event-writer [w handle]
    (run! prn event-seq)))

(comment
 (def testfile (io/file "/users/tristan/data/edntest.edn"))

 (with-event-writer [w testfile]
   (prn {:a :test})
   (prn {:en :otro}))

 (with-event-reader [r testfile]
   (into [] (event-seq r)))

 (store-events testfile [{:hablo :espanol} {:no :ingles}])
 )
