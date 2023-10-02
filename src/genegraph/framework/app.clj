(ns genegraph.framework.app
    "Core include refrerencing all necessary dependencies"
    (:require #_[genegraph.framework.topic :as topic]
              [genegraph.framework.processor :as processor]
              [genegraph.framework.protocol :as p]
              [genegraph.framework.storage :as s]
              [genegraph.framework.storage.console-writer]
              [genegraph.framework.storage.rocksdb]
              [genegraph.framework.storage.rdf]
              [genegraph.framework.storage.gcs]
              [genegraph.framework.kafka :as kafka]
              [genegraph.framework.topic :as topic]
              [genegraph.framework.event :as event]
              [clojure.spec.alpha :as spec]))

(spec/def ::app
  (spec/keys :req-un [::topics]))

(defrecord App [topics storage processors]

  p/Lifecycle
  (start [this]
    (run! #(when (satisfies? p/Lifecycle %) (p/start %))
          (concat (vals storage)
                  (vals topics)
                  (vals processors)))
    this)
  (stop [this]
    (run! #(when (satisfies? p/Lifecycle %) (p/stop %))
          (concat (vals processors)
                  (vals storage)
                  (vals topics)))
    this))

(defn- init-components [def components]
  (reduce
   (fn [a k] (update a k update-vals p/init))
   def
   components))

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

;;;;;
;; Stuff for testing
;;;;;

(def app-def-1
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

(defn test-interceptor-fn [event]
  (-> event
      (event/publish {::event/key (str "new-" (::event/key event))
                      ::event/data {:hgvs "NC_00000001:50000A>C"}
                      ::event/topic :test-endpoint})
      )
)

(defn test-publisher-fn [event]
  (event/publish event (:payload event)))

(def app-def-2
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
   :topics {:test-topic
            {:name :test-topic
             :type :kafka-consumer-group-topic
             :kafka-consumer-group "testcg8"
             :kafka-cluster :local
             :kafka-topic "test"}
            :test-endpoint
            {:name :test-endpoint
             :type :kafka-producer-topic
             :serialization :json
             :kafka-topic "test-out"
             :kafka-cluster :local}
            :test-input
            {:name :test-input
             :type :kafka-producer-topic
             :serialization :json
             :kafka-topic "test"
             :kafka-cluster :local}
            :publish-to-test
            {:name :publish-to-test
             :type :simple-queue-topic}}
   :storage {:test-rocksdb
             {:type :rocksdb
              :name :test-rocksdb
              :path "/users/tristan/desktop/test-rocks"}}
   :processors {:test-processor
                {:subscribe :test-topic
                 :name :test-processor
                 :type :processor
                 :kafka-cluster :local
                 :backing-store :test-rocksdb
                 :interceptors `[test-interceptor-fn]}
                :test-publisher
                {:name :test-publisher
                 :subscribe :publish-to-test
                 :kafka-cluster :local
                 :type :processor
                 :interceptors `[test-publisher-fn]}}
   :api-endpoint {"/" :test-processor
                  "/graphql" :graphql-processor}})

(comment

  (def a2 (p/init app-def-2))
  (p/start a2)
  (p/publish (get-in a2 [:topics :publish-to-test])
             {:payload
              {::event/key "k6"
               ::event/value "v10"
               ::event/topic :test-topic}
              #_#_#_#_::event/skip-local-effects true
              ::event/skip-publish-effects true})
  (s/store-offset @(get-in a2 [:storage :test-rocksdb :instance]) :test-topic 1)
  (s/retrieve-offset @(get-in a2 [:storage :test-rocksdb :instance]) :test-topic)
  (processor/starting-offset (get-in a2 [:processors :test-processor]))
  (get-in a2 [:processors :test-processor :storage :test-rocksdb :instance])
  (p/stop a2)
)
