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

(defrecord App [topics storage processors state]

  p/Lifecycle
  (start [this]
    (run! p/start (concat (vals @storage)
                          (vals @topics)
                          (vals @processors)))
    this)
  (stop [this]
    (run! p/stop (concat (vals @processors)
                         (vals @storage)
                         (vals @topics)))
    this))

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

;; (def a (create (select-keys app-def [:kafka-clusters :topics])))
;; (p/start a)
;; (-> a :topics deref :test-topic p/poll)
;; (p/stop a)



(defn offer-to-test [app]
  (p/offer (get @(:entities app) :test-topic) {:key :k :value :v})
  app)

(defn test-interceptor-fn [event]
  (println "the value be" (::event/value event))
  event)

;; TODO should just write a function to construct
;; an interceptor with a function reference
;; mostly want to offer a level of indirection
;; given a function reference

(def test-interceptor
  {:enter (fn [e] (test-interceptor-fn e))})

#_(def test-interceptor
  {:enter
   (fn [e]
     #_(clojure.pprint/pprint e)
     (clojure.pprint/pprint (s/read (get-in e [::s/storage :test-rocksdb]) :test))
     e
     #_(update e :effects conj [:global :test-rocksdb s/write (:key e) (:value e)]))})

(def app-def
  {:kafka-clusters {:dx-ccloud
                    { :common {"ssl.endpoint.identification.algorithm" "https"
                              "sasl.mechanism" "PLAIN"
                              "request.timeout.ms" "20000"
                              "bootstrap.servers" "pkc-4yyd6.us-east1.gcp.confluent.cloud:9092"
                              "retry.backoff.ms" "500"
                              "security.protocol" "SASL_SSL"
                              "sasl.jaas.config" (System/getenv "DX_JAAS_CONFIG")}
                     :consumer {"key.deserializer"
                                "org.apache.kafka.common.serialization.StringDeserializer"
                                "value.deserializer"
                                "org.apache.kafka.common.serialization.StringDeserializer"}
                     :producer {"key.serializer"
                                "org.apache.kafka.common.serialization.StringSerializer"
                                "value.serializer"
                                "org.apache.kafka.common.serialization.StringSerializer"}}}
   :topics {:test-topic {}
            #_{:kafka-cluster :dx-ccloud
             :kafka-topic "actionability"}}
   :storage {:test-rocksdb
             {:type :rocksdb
              :path "/users/tristan/desktop/test-rocks"}}
   :processors {:test-processor
                {:subscribe :test-topic
                 :interceptors `[test-interceptor-fn]}}})

(comment
  (def a (create app-def))
  (p/start a)
  (:topics a)
  (-> a :topics deref :test-topic (p/offer {:key :k :value :v}))
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
  
  (p/stop a)
  (-> a :topics deref :test-topic (p/offer {:key :test :value "bork"}))
  @(:topics a))
