(ns genegraph.app-lifecycle-example
  (:require [genegraph.framework.app :as app]
            [genegraph.framework.protocol :as p]
            [genegraph.framework.event :as event]
            [genegraph.framework.storage :as storage]
            [genegraph.framework.kafka.admin :as kafka-admin]
            [io.pedestal.log :as log]
            [io.pedestal.interceptor :as interceptor]
            [clojure.data.json :as json]))

(def publish-interceptor
  (interceptor/interceptor
   {:name ::publish-interceptor
    :enter (fn [e]
             (log/info :fn :publish-interceptor)
             (event/publish e (:payload e)))}))

(def cg-interceptor
  (interceptor/interceptor
   {:name ::cg-interceptor
    :enter (fn [e]
             (log/info :fn :cg-interceptor
                       :payload (::event/data e)
                       :key (::event/key e)
                       :stored-value (storage/read (get-in e [::storage/storage :test-rocksdb])
                                                   (::event/key e)))
             e)}))

(def base-interceptor
  (interceptor/interceptor
   {:name ::cg-interceptor
    :enter (fn [e]
             (log/info :fn :base-interceptor
                       :payload (::event/data e)
                       :key (::event/key e))
             (event/store e :test-rocksdb (::event/key e) (::event/data e)))}))


(def ccloud-example-app-def
  {:type :genegraph-app
   :kafka-clusters {:ccloud
                    {:type :kafka-cluster
                     :kafka-user "User:2189780"
                     :common-config {"ssl.endpoint.identification.algorithm" "https"
                                     "sasl.mechanism" "PLAIN"
                                     "request.timeout.ms" "20000"
                                     "bootstrap.servers" "pkc-4yyd6.us-east1.gcp.confluent.cloud:9092"
                                     "retry.backoff.ms" "500"
                                     "security.protocol" "SASL_SSL"
                                     "sasl.jaas.config" (System/getenv "DX_JAAS_CONFIG_DEV")}
                     :consumer-config {"key.deserializer"
                                       "org.apache.kafka.common.serialization.StringDeserializer"
                                       "value.deserializer"
                                       "org.apache.kafka.common.serialization.StringDeserializer"}
                     :producer-config {"key.serializer"
                                       "org.apache.kafka.common.serialization.StringSerializer"
                                       "value.serializer"
                                       "org.apache.kafka.common.serialization.StringSerializer"}}}
   :storage {:test-rocksdb
             {:type :rocksdb
              :name :test-rocksdb
              :path "/Users/tristan/data/genegraph-neo/test-rocks"
              :snapshot-handle {:type :file
                                :base "/Users/tristan/data/genegraph-neo"
                                :path "test-rocks-snapshot.tar.lz4"}
              :load-snapshot true}}
   :topics {:test-topic
            {:name :test-topic
             :type :kafka-consumer-group-topic
             :kafka-consumer-group "testcg9"
             :kafka-cluster :ccloud
             :serialization :json
             :kafka-topic "genegraph-test"}
            :test-base
            {:name :test-base
             :type :kafka-reader-topic
             :kafka-cluster :ccloud
             :serialization :json
             :kafka-topic "genegraph-test-base"}
            :publish-to-test
            {:name :publish-to-test
             :type :simple-queue-topic}}
   :processors {:test-processor
                {:subscribe :test-topic
                 :name :genegraph-test-processor
                 :type :processor
                 :kafka-cluster :ccloud
                 :interceptors [cg-interceptor]}
                :test-publisher
                {:name :genegraph-test-publisher
                 :subscribe :publish-to-test
                 :kafka-cluster :ccloud
                 :type :processor
                 :interceptors [publish-interceptor]}
                :test-base-processor
                {:name :genegraph-test-base-processor
                 :subscribe :test-base
                 :kafka-cluster :ccloud
                 :type :processor
                 :backing-store :test-rocksdb
                 :interceptors [base-interceptor]}}})

(comment
  (def ccloud-example-app (p/init ccloud-example-app-def))
  (kafka-admin/admin-actions-by-cluster ccloud-example-app)
  (kafka-admin/configure-kafka-for-app! ccloud-example-app)
  (p/start ccloud-example-app)
  (p/stop ccloud-example-app)

  (storage/store-snapshot (get-in ccloud-example-app-def [:storage :test-rocksdb]))
  (storage/restore-snapshot (get-in ccloud-example-app-def [:storage :test-rocksdb]))

  (p/publish (get-in ccloud-example-app [:topics :publish-to-test])
             {::event/key "k2"
              ::event/data {:b :b}
              :payload {::event/topic :test-base
                        ::event/key "k4"
                        ::event/data {:d :d}}})

  (p/publish (get-in ccloud-example-app [:topics :publish-to-test])
             {::event/key "k2"
              ::event/data {:b :b}
              :payload {::event/topic :test-topic
                        ::event/key "k4"
                        ::event/data {:c :c}}})

  (-> ccloud-example-app
      :storage
      :test-rocksdb
      :instance
      deref
      (storage/read "k3"))

  (-> ccloud-example-app
      :topics
      :test-topic
      genegraph.framework.kafka/topic-up-to-date?)
  )
