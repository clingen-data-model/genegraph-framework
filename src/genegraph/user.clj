(ns genegraph.user
  (:require [clojure.edn :as edn]
            [clojure.pprint :refer [pprint]]
            [clojure.java.io :as io]
            [clojure.set :as s]
            [genegraph.framework.event :as event]
            [genegraph.framework.event.store :as event-store]
            [genegraph.framework.storage.rocksdb :as rocksdb]
            [genegraph.framework.storage :as storage])
  (:import [java.io File PushbackReader FileOutputStream BufferedWriter FileInputStream BufferedReader]
           [java.nio ByteBuffer]
           [java.util.zip GZIPInputStream GZIPOutputStream]))



(defn event-seq-from-directory [directory]
  (let [files (->> directory
                  io/file
                  file-seq
                  (filter #(re-find #".edn" (.getName %))))]
    (map #(edn/read-string (slurp %)) files)))


(def gv-prior-events
  (concat
   (event-seq-from-directory "/users/tristan/data/genegraph/2023-04-13T1609/events/:gci-raw-snapshot")
   (event-seq-from-directory "/users/tristan/data/genegraph/2023-04-13T1609/events/:gci-raw-missing-data")))

(def gv-initial-event-store "/users/tristan/data/genegraph-neo/gene_validity_inital_events.edn.gz")

(event-store/store-events
 gv-initial-event-store
 (map #(-> (s/rename-keys % {:genegraph.sink.event/key ::event/key
                             :genegraph.sink.event/value ::event/value})
           (select-keys [::event/key ::event/value])) gv-prior-events))


(event-store/with-event-reader [r gv-initial-event-store]
  (count (event-seq r)))


(-> gv-prior-events first keys)
(count gv-prior-events)

(comment
  "Experimentation with writing sequences of events to edn.
   This may be preferrable to writing a bunch of files"
  (def testfile (io/file "/users/tristan/data/edntest.edn"))

  (def bigtest (io/file "/users/tristan/data/bigtest.edn"))

  (with-open [w (-> testfile io/output-stream GZIPOutputStream. io/writer)]
    (println (type w))
    (binding [*out* w]
      (prn {:test :data})
      (prn {:more :data})
      (prn {:even-more :data})))

  (with-open [r (-> testfile io/input-stream GZIPInputStream. io/reader)
              pbr (PushbackReader. r)]
    [(edn/read pbr)
     (edn/read pbr)
     (edn/read pbr)])

  (defn read-next-val [pbr]
    (try
      (edn/read pbr)
      (catch Exception e
        #_(println e)
        :EOF)))

  (defn event-seq [pbr]
    (let [v (read-next-val pbr)]
      (if (= :EOF v)
        ()
        (cons v (lazy-seq (event-seq pbr))))))

  (with-open [r (io/reader testfile)
              pbr (PushbackReader. r)]
    [(read-next-val pbr)
     (read-next-val pbr)
     (read-next-val pbr)
     (read-next-val pbr)])

  
  
  (with-open [r (io/reader testfile)
              pbr (PushbackReader. r)]
    (into [] (event-seq pbr)))

  (time
   (with-open [w (io/writer bigtest) #_(-> bigtest io/output-stream GZIPOutputStream. io/writer)]
     (binding [*out* w]
       (run! #(prn {:val %}) (take 1000000 (iterate inc 0))))))
  
  (time
   (with-open [r (io/reader bigtest) #_(-> bigtest io/input-stream GZIPInputStream. io/reader)
               pbr (PushbackReader. r)]
     (count (event-seq pbr))))

  
  )

(comment
  (def used-curies
   (->> (File. "/Users/tristan/code/genegraph-framework/src/genegraph/gene_validity/")
        file-seq
        (filter #(re-find #"sparql$" (str %)))
        (mapcat (fn [sparql-file]
                  (map #(keyword (second %))
                       (re-seq #"(?:\s:)([a-z]+/\S+)"
                               (slurp sparql-file)))))
        set))



 (keyword "hi/there")
 (->>
  (select-keys 
   (into {}
         (concat (edn/read-string (slurp "/Users/tristan/code/genegraph/resources/class-names.edn"))
                 (edn/read-string (slurp "/Users/tristan/code/genegraph/resources/property-names.edn")))
         )
   used-curies)
  seq
  (sort-by first)
  pprint))


(-> (ByteBuffer/allocate 8) (.putLong 1234) )

(->> (File. "/Users/tristan/data/genegraph/2023-04-13T1609/events/:gci-raw-snapshot")
     file-seq
     (filter #(re-find #"edn$" (str %)))
     count)
