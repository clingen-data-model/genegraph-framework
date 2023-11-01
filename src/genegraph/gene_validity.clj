(ns genegraph.gene-validity
  (:require [genegraph.framework.app :as app]
            [genegraph.framework.protocol :as p]
            [genegraph.framework.event.store :as event-store]
            [genegraph.framework.processor :as processor]
            [genegraph.framework.event :as event]
            [genegraph.framework.kafka :as kafka]
            [genegraph.framework.storage.rdf :as rdf]
            [genegraph.gene-validity.gci-model :as gci-model]
            [genegraph.gene-validity.sepio-model :as sepio-model]
            [clojure.java.io :as io]
            [clojure.edn :as edn]))


(def prop-query
  (rdf/create-query "select ?x where {?x a ?type}"))

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


(def gene-validity-transform
  (p/init
   {:type :processor
    :name :gene-validity-processor
    :interceptors `[gci-model/add-gci-model
                    sepio-model/add-model
                    add-action
                    add-iri]
    ::event/metadata {::event/format :json}}))

;; TODO Links between scores are not obviously being created

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
              :kafka-consumer-group "gvt0"
              :kafka-cluster :local
              :kafka-topic "gene_validity_complete"}
             :gene-validity-sepio
             {:name :gene-validity-sepio
              :type :kafka-producer-topic
              :serialization :json
              :kafka-cluster :local
              :kafka-topic "gene_validity_sepio"}}
    :processors {:gene-validity-transform
                 {:type :processor
                  :name :gene-validity-processor
                  :interceptors `[gci-model/add-gci-model
                                  sepio-model/add-model
                                  add-action
                                  add-iri]
                  ::event/metadata {::event/format :json}}}}))

(def gv-event-path "/users/tristan/data/genegraph-neo/all_gv_events.edn.gz")

(comment

  
  
  (kafka/topic->event-file
   (get-in gv-app [:topics :gene-validity-gci])
   "/users/tristan/data/genegraph-neo/all_gv_events.edn.gz")

  (event/serialize {:hi :there})

  (type "this")
  
  )

