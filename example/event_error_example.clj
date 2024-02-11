(ns genegraph.event-error-example
  (:require [genegraph.framework.app :as app]
            [genegraph.framework.protocol :as p]
            [genegraph.framework.event :as event]
            [genegraph.framework.kafka.admin :as kafka-admin]
            [io.pedestal.interceptor :as interceptor]
            [io.pedestal.interceptor.chain :as chain]
            [io.pedestal.log :as log]
            [clojure.pprint :refer [pprint]]))

(defn test-interceptor-fn [event]
  (log/info :fn ::test-interceptor-fn :key (::event/key event))
  (event/publish event
                 (assoc (select-keys event [::event/data ::event/key])
                        ::event/topic :out-topic)))

(def test-interceptor
  (interceptor/interceptor
   {:name ::test-interceptor
    :enter (fn [e] (test-interceptor-fn e))}))

(defn exception-interceptor-fn [event]
  (throw (Exception. "Just a test")))

(def exception-interceptor
  (interceptor/interceptor
   {:name ::exception-interceptor
    :enter (fn [e] (exception-interceptor-fn e))}))

(defn handle-exception [cx ex]
  (log/info :fn ::handle-exception :event-keys (keys cx))
  (throw (ex-info "new exception!" {:a :exception})))

(def exception-handler-interceptor
  {:name ::exception-handler
   :error (fn [cx ex] (handle-exception cx ex))})

(defn test-recorder-fn [event]
  (log/info :fn ::test-recorder-fn :key (::event/key event))
  event)

(def test-recorder
  (interceptor/interceptor
   {:name ::test-interceptor
    :enter (fn [e] (test-recorder-fn e))}))

(def env
  {:dev-genegraph-dev-dx-jaas (System/getenv "DX_JAAS_CONFIG_DEV")
   :kafka-user "User:2189780"
   :kafka-consumer-group "genegraph-error-test"})

(def data-exchange
  {:type :kafka-cluster
   :kafka-user (:kafka-user env)
   :common-config {"ssl.endpoint.identification.algorithm" "https"
                   "sasl.mechanism" "PLAIN"
                   "request.timeout.ms" "20000"
                   "bootstrap.servers" "pkc-4yyd6.us-east1.gcp.confluent.cloud:9092"
                   "retry.backoff.ms" "500"
                   "security.protocol" "SASL_SSL"
                   "sasl.jaas.config" (:dev-genegraph-dev-dx-jaas env)}
   :consumer-config {"key.deserializer"
                     "org.apache.kafka.common.serialization.StringDeserializer"
                     "value.deserializer"
                     "org.apache.kafka.common.serialization.StringDeserializer"}
   :producer-config {"key.serializer"
                     "org.apache.kafka.common.serialization.StringSerializer"
                     "value.serializer"
                     "org.apache.kafka.common.serialization.StringSerializer"}})

(def kafka-app
  {:type :genegraph-app
   :kafka-clusters {:data-exchange data-exchange}
   :topics {:in-topic
            {:type :kafka-consumer-group-topic
             :name :in-topic
             :serialization :json
             :kafka-consumer-group (:kafka-consumer-group env)
             :kafka-cluster :data-exchange
             :kafka-topic "genegraph-test-in-topic"}
            :out-topic
            {:type :kafka-producer-topic
             :serialization :json
             :kafka-cluster :data-exchange
             :name :out-topic
             :kafka-topic "genegraph-test-out-topic"}}
   :processors {:test-processor
                {:type :processor
                 :name :test-processor
                 :subscribe :in-topic
                 :kafka-cluster :data-exchange
                 :interceptors [exception-handler-interceptor
                                test-interceptor]}}})

(def simple-queue-app
  {:type :genegraph-app
   :topics {:in-topic
            {:type :simple-queue-topic
             :name :in-topic}
            :out-topic
            {:type :simple-queue-topic
             :name :out-topic}}
   :processors {:test-processor
                {:type :processor
                 :name :test-processor
                 :subscribe :in-topic
                 :interceptors [exception-handler-interceptor
                                test-interceptor]}
                :test-recorder
                {:type :processor
                 :name :test-recorder
                 :subscribe :out-topic
                 :interceptors [test-recorder]}}})

(comment

  (def sq (p/init simple-queue-app))
  (p/start sq)
  (p/stop sq)


  (p/process (get-in sq [:processors :test-processor])
             {::event/data {:a :a}
              ::event/key :a})

  (p/publish (get-in sq [:topics :in-topic])
             {::event/data {:a :a}
              ::event/key :a})
  
  (def ka (p/init kafka-app))
  (kafka-admin/configure-kafka-for-app! ka)
  (p/start ka)
  (p/stop ka)
  )
