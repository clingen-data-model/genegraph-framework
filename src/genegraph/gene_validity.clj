(ns genegraph.gene-validity
  (:require [genegraph.framework.app :as app]
            [genegraph.framework.protocol :as p]
            [genegraph.framework.event.store :as event-store]
            [genegraph.framework.processor :as processor]
            [genegraph.framework.event :as event]
            [genegraph.framework.kafka :as kafka]
            [genegraph.framework.storage :as storage]
            [genegraph.framework.storage.rdf :as rdf]
            [genegraph.gene-validity.gci-model :as gci-model]
            [genegraph.gene-validity.sepio-model :as sepio-model]
            [genegraph.gene-validity.base :as base]
            [genegraph.gene-validity.graphql.schema :as gql-schema]
            [com.walmartlabs.lacinia.pedestal2 :as lacinia-pedestal]
            [io.pedestal.http :as http]
            [io.pedestal.interceptor :as interceptor]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.edn :as edn])
  (:import [org.apache.jena.rdf.model Model]
           [org.apache.jena.query ReadWrite]
           [java.util.concurrent LinkedBlockingQueue ThreadPoolExecutor TimeUnit]))


(def prop-query
  (rdf/create-query "select ?x where {?x a ?type}"))

;; Deprecated (but maybe test first)
(defn add-action [event]
  (assoc event
         ::event/action
         (if (re-find #"\"publishClassification\": ?true"
                      (::event/value event))
           :publish
           :unpublish)))

(defn add-iri [event]
  (assoc event
         ::event/iri
         (-> (prop-query
              (::event/model event)
              {:type :sepio/GeneValidityProposition})
             first
             str)))

(defn add-publish-actions [event]
  (event/publish event
                 (-> event
                     (set/rename-keys {::event/iri ::event/key
                                       ::event/model ::event/data})
                     (select-keys [::event/key ::event/data])
                     (assoc ::event/topic :gene-validity-sepio))))

;; TODO Links between scores are not obviously being created



(def gv-event-path "/users/tristan/data/genegraph-neo/all_gv_events.edn.gz")

(defn has-publish-action [m]
  (< 0 (count ((rdf/create-query "select ?x where { ?x :bfo/realizes :cg/PublisherRole } ") m))))

(defn store-curation [event]
  (println
   (str
    (subs (::event/key event) 67)
    " "
    (has-publish-action (::event/data event))))
  #_(println (keys event))
  #_(spit (storage/as-handle (assoc (::handle event)
                                    :path (str (subs (::event/key event) 67) ".nt")))
          (::event/value
           (event/serialize
            (assoc event ::event/format ::rdf/n-triples))))
  (if (has-publish-action (::event/data event))
    (event/store event :gv-tdb (::event/key event) (::event/data event))
    (event/delete event :gv-tdb (::event/key event))))

(def add-graphql-context-interceptor
  "Make sure that graphql resolvers have access to storage, and
  any other data needed from application."
  (interceptor/interceptor
   {:name ::add-graphql-context
    :enter (fn [context]
             (assoc-in context [:request :lacinia-app-context]
                       (select-keys context [::storage/storage])))}))

(def jena-transaction-interceptor
  (interceptor/interceptor
   {:name ::jena-transaction-interceptor
    :enter (fn [context]
             (let [gv-tdb (get-in context [::storage/storage :gv-tdb])]
               (println "transaction interceptor "
                        (-> (Thread/currentThread) .getId))
               (.begin gv-tdb ReadWrite/READ)
               (assoc-in context [:request :lacinia-app-context :db] gv-tdb)))
    :leave (fn [context]
             (println "transaction interceptor close "
                        (-> (Thread/currentThread) .getId))
             (.close (get-in context [:request :lacinia-app-context :db]))
             context)}))

(type (-> (Runtime/getRuntime) .availableProcessors))

(defn graphql-executor
  "Construct and return an Executor for handling GraphQL queries"
  [tdb]
  (let [pool-size (.availableProcessors (Runtime/getRuntime))]
    (proxy [ThreadPoolExecutor]
        [pool-size
         pool-size
         Long/MAX_VALUE
         TimeUnit/MILLISECONDS
         (LinkedBlockingQueue.)]
      
      (beforeExecute [^Thread t ^Runnable r]
        (proxy-super beforeExecute t r)
        )

      (afterExecute [^Runnable r ^Throwable t]
        (proxy-super afterExecute r t)
        ))))

(comment

  (def gv-app
    (p/init
     {:type :genegraph-app
      :kafka-clusters {:local
                       {:common-config {"bootstrap.servers" "localhost:9092"}
                        :producer-config {"key.serializer"
                                          "org.apache.kafka.common.serialization.StringSerializer",
                                          "value.serializer"
                                          "org.apache.kafka.common.serialization.StringSerializer"}
                        :consumer-config {"key.deserializer"
                                          "org.apache.kafka.common.serialization.StringDeserializer"
                                          "value.deserializer"
                                          "org.apache.kafka.common.serialization.StringDeserializer"}}}
      :topics {:gene-validity-gci
               {:name :gene-validity-gci
                :type :kafka-consumer-group-topic
                :serialization :json
                :kafka-consumer-group "gvt0"
                :kafka-cluster :local
                :kafka-topic "gene_validity_complete"}
               :gene-validity-sepio
               {:name :gene-validity-sepio
                :type :kafka-producer-topic
                :serialization ::rdf/n-triples
                :kafka-cluster :local
                :kafka-topic "gene_validity_sepio"}}
      :processors {:gene-validity-transform
                   {:type :processor
                    :name :gene-validity-processor
                    :interceptors `[gci-model/add-gci-model
                                    sepio-model/add-model
                                    add-iri
                                    add-publish-actions]}}}))

  (defn print-key [event]
    (println (subs (::event/key event) 67))
    (spit (storage/as-handle (assoc (::handle event)
                                    :path (str (subs (::event/key event) 67) ".nt")))
          (::event/value
           (event/serialize
            (assoc event ::event/format ::rdf/n-triples))))
    event)

  (def gcs-handle
    {:type :gcs
     :bucket "genegraph-framework-dev"})
  
  (def fs-handle
    {:type :file
     :base "/users/tristan/data/genegraph-neo/"})

  (def base-fs-handle
    {:type :file
     :base "/users/tristan/data/genegraph-neo/new-base/"})

  (def gv-test-app
    (p/init
     {:type :genegraph-app
      :topics {:gene-validity-gci
               {:name :gene-validity-gci
                :type :simple-queue-topic}
               :gene-validity-sepio
               {:name :gene-validity-sepio
                :type :simple-queue-topic}
               :fetch-base-events
               {:name :fetch-base-events
                :type :simple-queue-topic}
               :base-data
               {:name :base-data
                :type :simple-queue-topic}}
      :storage {:gv-tdb
                {:type :rdf
                 :name :gv-tdb
                 :path "/users/tristan/data/genegraph-neo/gv_tdb"}}
      :processors {:gene-validity-transform
                   {:type :processor
                    :name :gene-validity-processor
                    :subscribe :gene-validity-gci
                    :interceptors `[gci-model/add-gci-model
                                    sepio-model/add-model
                                    add-iri
                                    add-publish-actions]}
                   :gene-validity-sepio-reader
                   {:type :processor
                    :subscribe :gene-validity-sepio
                    :name :gene-validity-sepio-reader
                    :interceptors `[store-curation]}
                   :fetch-base-file
                   {:name :fetch-base-file
                    :type :processor
                    :subscribe :fetch-base-events
                    :interceptors `[base/fetch-file
                                    base/publish-base-file]
                    ::event/metadata {::base/handle base-fs-handle}}
                   :import-base-file
                   {:name :import-base-file
                    :type :processor
                    :subscribe :base-data
                    :interceptors `[base/read-base-data
                                    base/store-model]}
                   :graphql-api
                   {:name :graphql-api
                    :type :processor
                    :interceptors [#_lacinia-pedestal/initialize-tracing-interceptor
                                   lacinia-pedestal/json-response-interceptor
                                   lacinia-pedestal/error-response-interceptor
                                   lacinia-pedestal/body-data-interceptor
                                   lacinia-pedestal/graphql-data-interceptor
                                   lacinia-pedestal/status-conversion-interceptor
                                   lacinia-pedestal/missing-query-interceptor
                                   (lacinia-pedestal/query-parser-interceptor
                                    gql-schema/schema)
                                   lacinia-pedestal/disallow-subscriptions-interceptor
                                   lacinia-pedestal/prepare-query-interceptor
                                   jena-transaction-interceptor
                                   #_lacinia-pedestal/enable-tracing-interceptor
                                   lacinia-pedestal/query-executor-handler]}}
      :http-servers {:gene-validity-server
                     {:type :http-server
                      :name :gene-validity-server
                      :endpoints [{:path "/api"
                                   :processor :graphql-api
                                   :method :post}]
                      ::http/routes
                      (conj
                       (lacinia-pedestal/graphiql-asset-routes "/assets/graphiql")
                       ["/ide" :get (lacinia-pedestal/graphiql-ide-handler {})
                        :route-name ::lacinia-pedestal/graphql-ide])
                      ::http/type :jetty
                      ::http/port 8888
                      ::http/join? false
                      ::http/secure-headers nil}}}))

  (p/start gv-test-app)
  (p/stop gv-test-app)
  
  (->> (-> "base.edn" io/resource slurp edn/read-string)
       #_(filter #(isa? (:format %) ::rdf/rdf-serialization))
       #_(filter #(= "http://purl.obolibrary.org/obo/sepio.owl" (:name %)))
       (map (fn [x] {::event/data x}))
       (run! #(p/publish (get-in gv-test-app [:topics :fetch-base-events]) %)))

  (let [db @(get-in gv-test-app [:storage :gv-tdb :instance])]
    db
    (rdf/tx db
            (-> ((rdf/create-query "select ?x where { ?x a :so/Gene } limit 5")
                 (rdf/resource "https://www.ncbi.nlm.nih.gov/gene/55847" db))
                first)))



  (let [db @(get-in gv-test-app [:storage :gv-tdb :instance])]
    (rdf/tx db
            (-> (rdf/resource "https://www.ncbi.nlm.nih.gov/gene/55847" db)
                (rdf/ld-> [:skos/prefLabel]))))

  (type @(get-in gv-test-app [:storage :gv-tdb :instance]))

  (def moi-and-classification
    (rdf/create-query
     "select distinct ?a where { ?x a ?t ;
:sepio/has-qualifier ?moi .
?a :sepio/has-subject ?x ;
a ?t2 ;
:sepio/has-object ?classification ;
:sepio/has-evidence + ?p .
?p a ?t3}"))

  (let [db @(get-in gv-test-app [:storage :gv-tdb :instance])]
    db
    (rdf/tx db
            (->
             (moi-and-classification
              db
              {:t :sepio/GeneValidityProposition
               :t2 :sepio/GeneValidityEvidenceLevelAssertion
               :moi :hp/AutosomalRecessiveInheritance
               :classification :sepio/ModerateEvidence
               :t3 :sepio/NullVariantEvidenceLine})
             count)))

  (let [db @(get-in gv-test-app [:storage :gv-tdb :instance])]
    (rdf/tx db
            (->
             ((rdf/create-query "select ?p where { ?p a ?t }")
              db
              {:t :sepio/NullVariantEvidenceLine})
             first
             (rdf/ld-> [[:sepio/has-evidence :<]
                        [:sepio/has-evidence :<]]))))
  

  
  (event-store/with-event-reader [r gv-event-path]
    (->> (event-store/event-seq r)
         (run! #(p/publish (get-in gv-test-app [:topics :gene-validity-gci]) %))))
  
  (kafka/topic->event-file
   (get-in gv-app [:topics :gene-validity-gci])
   "/users/tristan/data/genegraph-neo/all_gv_events.edn.gz")

  (def gv-xform #(processor/process-event
                  gene-validity-transform-processor
                  %))

  (event-store/with-event-reader [r gv-event-path]
    (->> (event-store/event-seq r)
         #_(filter #(re-find #"7d595dc4" (::event/value %)))
         #_(take 1)
         #_(map #(-> %
                     (assoc ::event/skip-local-effects true
                            ::event/skip-publish-effects true)
                     gv-xform
                     ::event/publish))))

  (event-store/with-event-reader [r gv-event-path]
    (->> (event-store/event-seq r)
         (take 1)
         
         #_(filter #(re-find #"\"10\"" (::event/value %)))
         #_count
         
         (map #(-> %
                   (assoc ::event/skip-local-effects true
                          ::event/skip-publish-effects true)
                   gv-xform
                   ::event/publish))))
  
  )

(-> (Runtime/getRuntime) .availableProcessors)
