(ns genegraph.user
  (:require [clojure.edn :as edn]
            [clojure.pprint :refer [pprint]]
            [clojure.java.io :as io]
            [clojure.set :as s]
            [clojure.data.json :as json]
            [genegraph.framework.app :as gg-app]
            [genegraph.framework.kafka :as kafka]
            [genegraph.framework.protocol :as p]
            [genegraph.framework.event :as event]
            [genegraph.framework.event.store :as event-store]
            [genegraph.framework.storage.rdf :as rdf]
            [genegraph.framework.storage.rocksdb :as rocksdb]
            [genegraph.framework.storage :as storage]
            [genegraph.framework.processor :as processor]
            [genegraph.gene-validity :as gv])
  (:import [java.io File PushbackReader FileOutputStream BufferedWriter FileInputStream BufferedReader]
           [java.nio ByteBuffer]
           [java.time Instant OffsetDateTime]
           [java.util.zip GZIPInputStream GZIPOutputStream]))



(defn event-seq-from-directory [directory]
  (let [files (->> directory
                  io/file
                  file-seq
                  (filter #(re-find #".edn" (.getName %))))]
    (map #(edn/read-string (slurp %)) files)))

(comment
 (def gv-prior-events
   (concat
    (event-seq-from-directory "/users/tristan/data/genegraph/2023-08-08T1423/events/:gci-raw-snapshot")
    (event-seq-from-directory "/users/tristan/data/genegraph/2023-08-08T1423/events/:gci-raw-missing-data")))

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

 )

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

(comment
 (-> (ByteBuffer/allocate 8) (.putLong 1234) )

 (->> (File. "/Users/tristan/data/genegraph/2023-04-13T1609/events/:gci-raw-snapshot")
      file-seq
      (filter #(re-find #"edn$" (str %)))
      count))

(comment
 (def znfid "1bb8bc84")

 (def znf-events 
   (event-store/with-event-reader [r "/Users/tristan/data/genegraph-neo/gv_events.edn.gz"]
     (->> (event-store/event-seq r)
          (filter #(re-find #"1bb8bc84" (::event/value %)))
          (into []))))

 (def znf-models
   (mapv #(processor/process-event gv/gene-validity-transform %) znf-events))

 (-> znf-models first keys)

 (def published-znf
   (->> znf-models
        (filter #(= :publish (::event/action %)))))

 (rdf/pp-model
  (rdf/difference
   (::event/model (nth mras-events 1))
   (::event/model (nth mras-events 0))))


 (def mras-events
   (event-store/with-event-reader [r "/Users/tristan/data/genegraph-neo/gv_events.edn.gz"]
     (->> (event-store/event-seq r)
          (filter #(re-find #"e4ea022c-a24e-42dd-b7e6-62eccb391a4f" (::event/value %)))
          (map #(processor/process-event gv/gene-validity-transform %))
          (filter #(= :publish (::event/action %)))
          (filter #(= "http://dataexchange.clinicalgenome.org/gci/e4ea022c-a24e-42dd-b7e6-62eccb391a4f"
                      (::event/iri %)))
          (into []))))

 (def pex19-events
   (event-store/with-event-reader [r "/Users/tristan/data/genegraph-neo/gv_events.edn.gz"]
     (->> (event-store/event-seq r)
          (filter #(re-find #"fa073c77" (::event/value %)))
          #_(map #(processor/process-event gv/gene-validity-transform %))
          #_(filter #(= :publish (::event/action %)))
          #_(filter #(= "http://dataexchange.clinicalgenome.org/gci/e4ea022c-a24e-42dd-b7e6-62eccb391a4f"
                      (::event/iri %)))
          (into []))))

 (count pex19-events)

 (def pex19-curation
   (::event/model (processor/process-event gv/gene-validity-transform (last pex19-events))))

 ((rdf/create-query "select ?x where { ?x a :dc/BibliographicResource }") pex19-curation)

 (rdf/pp-model
  ((rdf/create-query "construct { ?s ?p ?o } where { ?s a :dc/BibliographicResource . ?s ?p ?o }") pex19-curation))

 (rdf/pp-model
  ((rdf/create-query "construct { ?s ?p ?o } where { ?s a :dc/BibliographicResource . ?s ?p ?o }") pex19-curation))

 (rdf/pp-model
  ((rdf/create-query "construct { ?s ?p ?o ; :dc/source ?source . } where { ?s :dc/source ?source . ?s ?p ?o . ?source a :dc/BibliographicResource .}")
   pex19-curation))

 ;;allele
 (rdf/pp-model
  ((rdf/create-query "construct { ?s ?p ?o } where { ?s ?p ?o .}")
   pex19-curation
   {:s (rdf/resource "http://dataexchange.clinicalgenome.org/gci/d664b4b5-4e2b-4d91-893c-4bcdeb804da4")}))


 (spit "/users/tristan/desktop/pex19.txt"
       (with-out-str (-> (processor/process-event gv/gene-validity-transform (last pex19-events))
                         ::event/model
                         rdf/pp-model)))

 (println "o")

 (count mras-events)

 ((rdf/create-query "select ?x where { ?x <http://dataexchange.clinicalgenome.org/gci/publishClassification> ?o }")
  (-> mras-events first :gene-validity/gci-model))

 (rdf/pp-model (-> mras-events first :gene-validity/gci-model))
 (rdf/pp-model (-> mras-events first ::event/model))

 (defn processed-model [event]
   (::event/model (processor/process-event gv/gene-validity-transform event)))

 (->  (rdf/difference (processed-model (nth mras-events 4))
                      (processed-model (nth mras-events 3)))
     rdf/pp-model)

 (rdf/pp-model (processed-model (nth mras-events 3)))
 
 (with-open [w (io/writer "/users/tristan/desktop/mras.edn")]
   (pprint (::event/data (first mras-events)) w))

 (rdf/pp-model (->> (nth znf-events 4)
                    (processor/process-event gv/gene-validity-transform)
                    ::event/model))
 (spit
  "/users/tristan/desktop/intermediate.json"
  (->> (nth znf-events 4)
       (processor/process-event gv/gene-validity-transform)
       ::event/data
       genegraph.gene-validity.gci-model/preprocess-json
       genegraph.gene-validity.gci-model/fix-gdm-identifiers))

  (-> (nth znf-events 4)
      ::event/timestamp
      Instant/ofEpochMilli
      str)
 
 (type
  (->> (nth znf-events 4)
       (processor/process-event gv/gene-validity-transform)
       ::event/data
       genegraph.gene-validity.gci-model/preprocess-json
       genegraph.gene-validity.gci-model/fix-gdm-identifiers))

 (map ::event/action znf-models)

 (->> mras-events
      (map gv/add-iri)
      (map ::event/iri)
      frequencies)
 )

(comment
  (p/start gv/gene-validity-transform)
  (keys gv/gene-validity-transform)
  (:state gv/gene-validity-transform)
  (p/stop gv/gene-validity-transform)
  )


(def dx-ccloud
  {:type :kafka-cluster
   :common-config {"ssl.endpoint.identification.algorithm" "https"
                   "sasl.mechanism" "PLAIN"
                   "request.timeout.ms" "20000"
                   "bootstrap.servers" "pkc-4yyd6.us-east1.gcp.confluent.cloud:9092"
                   "retry.backoff.ms" "500"
                   "security.protocol" "SASL_SSL"
                   "sasl.jaas.config" (System/getenv "DX_JAAS_CONFIG")}
   :consumer-config {"key.deserializer"
                     "org.apache.kafka.common.serialization.StringDeserializer"
                     "value.deserializer"
                     "org.apache.kafka.common.serialization.StringDeserializer"}
   :producer-config {"key.serializer"
                     "org.apache.kafka.common.serialization.StringSerializer"
                     "value.serializer"
                     "org.apache.kafka.common.serialization.StringSerializer"}})

(def local-kafka
  {:common-config {"bootstrap.servers" "localhost:9092"}
   :producer-config {"key.serializer"
                     "org.apache.kafka.common.serialization.StringSerializer",
                     "value.serializer"
                     "org.apache.kafka.common.serialization.StringSerializer"}
   :consumer-config {"key.deserializer"
                     "org.apache.kafka.common.serialization.StringDeserializer"
                     "value.deserializer"
                     "org.apache.kafka.common.serialization.StringDeserializer"}})

(defn publisher-fn [event]
  (println "publishing " (get-in event [:payload :key]))
  (event/publish event (assoc (:payload event)
                              ::event/topic :gv-complete)))

(defn transformer-fn [event]
  (println "reading offset: "
           (::event/offset event)
           " size: "
           (count (::event/value event)))
  event)


(def sept-1-2020
  (-> (OffsetDateTime/parse "2020-09-01T00:00Z")
      .toInstant
      .toEpochMilli))

(defn prior-event->publish-fn [e]
  {:payload
   (-> e
       (s/rename-keys {:genegraph.sink.event/key ::event/key
                       :genegraph.sink.event/value ::event/value})
       (select-keys [::event/key ::event/value])
       (assoc ::event/timestamp sept-1-2020))})

(defn current-event->publish-fn [e]
  {:payload (select-keys  e
                          [::event/key
                           ::event/value
                           ::event/timestamp])})

(comment
 (def gv
   (p/init
    {:type :genegraph-app
     :kafka-clusters {:local local-kafka}
     :topics {:gv-complete
              {:name :gv-complete
               :type :kafka-consumer-group-topic
               :kafka-consumer-group "testcg0"
               :kafka-cluster :local
               :kafka-topic "gene_validity_complete"}
              :publish-gv
              {:name :publish-gv
               :type :simple-queue-topic}}
     :processors {:gv-publisher
                  {:name :gv-publisher
                   :type :processor
                   :kafka-cluster :local
                   :subscribe :publish-gv
                   :interceptors `[publisher-fn]}
                  :gv-transformer
                  {:name :gv-transformer
                   :type :processor
                   :kafka-cluster :local
                   :subscribe :gv-complete
                   :interceptors `[transformer-fn]}}}))

 (p/start gv)
 (p/stop gv)

 ;; todo start here, bootstrap gv events on local topic

 (-> gv
     :topics
     :gv-complete
     :state
     deref
     :kafka-consumer
     kafka/end-offset)

 (def gv-prior-events
   (concat
    (event-seq-from-directory "/users/tristan/data/genegraph/2023-08-08T1423/events/:gci-raw-snapshot")
    (event-seq-from-directory "/users/tristan/data/genegraph/2023-08-08T1423/events/:gci-raw-missing-data")))

 (run! #(p/publish (get-in gv [:topics :publish-gv])
                   (prior-event->publish-fn %))
       gv-prior-events)

 (-> gv-prior-events
     first
     prior-event->publish-fn
     :payload
     ::event/timestamp
     Instant/ofEpochMilli)

 (kafka/topic->event-file
  {:name :gv-raw
   :type :kafka-reader-topic
   :kafka-cluster dx-ccloud
   :kafka-topic "gene_validity_raw"}
  "/users/tristan/desktop/gv_events.edn.gz")

 (event-store/with-event-reader [r "/users/tristan/desktop/gv_events.edn.gz"]
   (-> (event-store/event-seq r)
       first
       ::event/timestamp
       Instant/ofEpochMilli))



 (event-store/with-event-reader [r "/users/tristan/desktop/gv_events.edn.gz"]
   (run! #(p/publish (get-in gv [:topics :publish-gv])
                     (current-event->publish-fn %))
         (event-store/event-seq r)))



 )
