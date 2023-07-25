(ns genegraph.framework.app
    "Core include refrerencing all necessary dependencies"
    (:require [genegraph.framework.topic :as topic]
              [genegraph.framework.processor :as processor]
              [genegraph.framework.protocol :as p]
              [genegraph.framework.storage :as s]
              [genegraph.framework.storage.console-writer]
              [genegraph.framework.storage.rocksdb]
              [genegraph.framework.storage.rdf]
              [genegraph.framework.storage.gcs]
              [genegraph.framework.event :as event]
              [clojure.spec.alpha :as spec]))

(spec/def ::app
  (spec/keys :req-un [::topics]))

(defrecord App [topics storage processors]

  p/Lifecycle
  (start [this]
    (run! p/start
          (concat (vals storage)
                  (vals topics)
                  (vals processors)))
    this)
  (stop [this]
    (run! p/stop
          (concat (vals processors)
                  (vals storage)
                  (vals topics)))
    this))



(defn- init-components [def components]
  (reduce
   (fn [a k] (update a k update-vals p/init))
   def
   components))

(defn- add-cluster-refs-to-topics [app]
  (update app
          :topics
          (fn [topics]
            (update-vals
             topics
             (fn [topic]
                 (update
                  topic
                  :kafka-cluster
                  (fn [cluster]
                    (get-in app [:kafka-clusters cluster]))))))))

(defn- add-kafka-cluster-refs [app component]
  (update app
          component
          (fn [entities]
            (update-vals
             entities
             (fn [e]
                 (update
                  e
                  :kafka-cluster
                  (fn [cluster]
                    (get-in app [:kafka-clusters cluster]))))))))

(defn- add-topic-and-storage-refs [app component]
  (update
   app
   component
   update-vals
   #(merge % (select-keys app [:topics :storage :kafka-clusters]))))

(defmethod p/init :genegraph-app [app]
  (-> app
      (add-kafka-cluster-refs :topics)
      (add-kafka-cluster-refs :processors)
      (init-components [:topics :storage])
      (add-topic-and-storage-refs :processors)
      (init-components [:processors])
      map->App))


(comment
  (def a1 (p/init app-def))

  (keys a1)
  
  )

(defn- add-type-to-defs [entity-defs type]
  (update-vals entity-defs #(assoc % :type type)))

(defn- add-initialized-entity-atoms [entity-defs]
  (atom (update-vals entity-defs p/init)))

(defn- add-topic-and-storage-refs-to-processors [app]
  (update app
          :processors
          (fn [processors]
            (update-vals
             processors
             #(merge % (select-keys app [:storage :topics]))))))



(defn- add-name-to-entities [app entity-types]
  (reduce (fn [a entity-type]
            (update a
                    entity-type
                    (fn [entity-map]
                      (into
                       {}
                       (map (fn [[name def]] [name (assoc def :name name)])
                            entity-map)))))
          app
          entity-types))

(defn create
  [app-def]
  (-> app-def
      (add-name-to-entities [:kafka-clusters :topics :storage :processors])
      add-cluster-refs-to-topics
      (update :topics add-type-to-defs :topic)
      (update :topics add-initialized-entity-atoms)
      (update :storage add-initialized-entity-atoms)
      (update :processors add-type-to-defs :processor)
      add-topic-and-storage-refs-to-processors
      (update :processors add-initialized-entity-atoms)
      (assoc :state (atom :stopped))
      map->App))

(defn offer-to-test [app]
  (p/offer (get @(:entities app) :test-topic) {:key :k :value :v})
  app)

(defn test-interceptor-fn [event]
  (println "the key be " (::event/key event))
  event)

(def app-def
  {:type :genegraph-app
   :kafka-clusters {:dx-ccloud
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
                                       "org.apache.kafka.common.serialization.StringSerializer"}}
                    :local
                    {:common-config {"bootstrap.servers" "localhost:9092"}
                     :producer-config {"key.serializer"
                                       "org.apache.kafka.common.serialization.StringSerializer",
                                       "value.serializer"
                                       "org.apache.kafka.common.serialization.StringSerializer"}
                     :consumer-config {"key.deserializer"
                                       "org.apache.kafka.common.serialization.StringDeserializer"
                                       "value.deserializer"
                                     "org.apache.kafka.common.serialization.StringDeserializer"}}}
   :topics {:test-topic
            {:initial-events {:type :gcs
                              :bucket "genegraph-framework-dev"
                              :path "gene_validity_initial_events.edn.gz"}
             :type :topic
             :kafka-cluster :dx-ccloud
             :kafka-topic "actionability"
             :start-kafka true}}
   :storage {:test-rocksdb
             {:type :rocksdb
              :path "/users/tristan/desktop/test-rocks"}}
   :processors {:test-processor
                {:subscribe :test-topic
                 :type :processor
                 :interceptors `[test-interceptor-fn]}}})




(comment

  (def a2 (p/init app-def))
  (p/start a2)
  (p/stop a2)

  (p/init app-def)
  
  
  {:type :file
   :base "/users/tristan/data/genegraph-neo/"
   :path "gene_validity_initial_events.edn.gz"}
  
  (.exists (s/as-handle {:type :file
                         :base "/Users/tristan/data/genegraph-neo"
                         :path "gene_validity_initial_events.edn.gz"}))
  
  (def a (create app-def))
  (p/start a)
  (p/stop a)

  (first (cons :a [:b :c]))
  
  (:topics a)
  (-> a :topics deref :test-topic )
  (-> a
      :processors
      deref
      :test-processor
      (processor/process-event {::event/key :k
                                ::event/value "{\"object\":\"value\"}"
                                ::event/metadata {::event/format :json}}))
  (-> a
      :processors
      deref
      :test-processor
      (processor/process-event {::event/key :k
                                ::event/value "{:object \"value\"}"
                                ::event/metadata {::event/format :edn}}))
  

  (-> a :topics deref :test-topic (p/offer {:key :test :value "bork"}))
  @(:topics a))
