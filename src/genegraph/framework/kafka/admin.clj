(ns genegraph.framework.kafka.admin
  "Methods for managing the lifecycle of topics."
  (:require [genegraph.framework.protocol :as p]
            [clojure.set :as set]
            [io.pedestal.log :as log])
  (:import [org.apache.kafka.clients.admin
            Admin NewTopic CreateTopicsResult OffsetSpec]
           [org.apache.kafka.common KafkaFuture]
           [org.apache.kafka.common.config
            ConfigResource ConfigResource$Type]
           [org.apache.kafka.common.acl
            AclBindingFilter AclBinding AccessControlEntry AclOperation AclPermissionType]
           [org.apache.kafka.common.resource
            ResourcePattern PatternType ResourceType]))

;; TODO -- these should have timeouts that throw exceptions

(def default-topic-config
  {"retention.ms" "-1" ; retain indefinitely by default
   "max.message.bytes" "8388608"} ; maximum allowable by confluent cloud
  )

(defn create-admin-client [cluster-def]
  (Admin/create (:common-config cluster-def)))

(defn topics
  "Return set of topics existant on server"
  [admin-client]
  (-> admin-client
      .listTopics
      .namesToListings
      deref
      keys
      set))

(defn str->kafka-topic [s]
  (doto (NewTopic. s 1 (short 3))
    (.configs default-topic-config)))

(defn create-topics [admin-client topics]
  (log/info :fn :create-topics :topics topics)
  (-> (.createTopics admin-client
                     (map str->kafka-topic topics))
      .all
      deref))

(defn delete-topic [admin-client topic]
  (-> (.deleteTopics admin-client [topic])
      .all
      deref))

(defn acls [admin-client]
  (-> (.describeAcls admin-client AclBindingFilter/ANY)
      .values
      deref
      set))

(defn create-acls [admin-client acl-bindings]
  (-> (.createAcls admin-client acl-bindings)
      .all
      deref))

(defn acl-binding->map [binding]
  (let [entry (.entry binding)
        pattern (.pattern binding)]
    {:principal (.principal entry)
     :operation (.operation entry)
     :permission-type (.permissionType entry)
     :host (.host entry)
     :name (.name pattern)
     :resource-type (.resourceType pattern)
     :pattern-type (.patternType pattern)}))

(defn map->access-control-entry [m]
  (AccessControlEntry. (:principal m)
                       (:host m "'*'")
                       (:operation m AclOperation/ALL)
                       (:permission-type m AclPermissionType/ALLOW)))

(defn map->resource-pattern [m]
  (ResourcePattern. (:resource-type m ResourceType/TOPIC)
                    (:name m)
                    (:pattern-type m PatternType/LITERAL)))

(defn map->acl-binding [m]
  (AclBinding. (map->resource-pattern m)
               (map->access-control-entry m)))

(defmulti kafka-entity-def->admin-actions
  "Translate the definition of a topic in a Genegraph app into the actions
  needed to make that topic useable to a client:
  For topics, the required actions are:  create the topic, create the
  needed consumer group, create the read/write permissions on the topic.
  For processors, the kafka user needs to have permissions on the transactional-id"
  type)

(defmethod kafka-entity-def->admin-actions :default [_]
  [])

(defmethod kafka-entity-def->admin-actions :genegraph/processor
  [processor]
  [{:action :grant-permission-on-transactional-id
    :name (name (:name processor))
    :operation AclOperation/WRITE
    :principal (:kafka-user processor)}])

(defmethod kafka-entity-def->admin-actions :genegraph/topic
  [{:keys [kafka-topic kafka-consumer-group kafka-user]}]
  (->> [{:action :create-topic
         :name kafka-topic}
        {:action :create-consumer-group-grant
         :name kafka-consumer-group
         :operation AclOperation/READ
         :principal kafka-user}
        {:action :grant-permission-on-topic
         :name kafka-topic
         :operation AclOperation/WRITE
         :principal kafka-user}
        {:action :grant-permission-on-topic
         :name kafka-topic
         :operation AclOperation/READ
         :principal kafka-user}]
       (remove #(some nil? (vals %)))
       set))

(defn admin-actions-by-cluster [app-def]
  (reduce (fn [m [cluster entities]]
            (assoc m
                   cluster
                   (reduce (fn [actions entity]
                             (set/union actions
                                        (kafka-entity-def->admin-actions entity)))
                           #{}
                           (map #(assoc % :kafka-user (:kafka-user cluster))
                                entities))))
          {}
          (dissoc (group-by :kafka-cluster
                            (concat (vals (:topics app-def))
                                    (vals (:processors app-def))))
                  nil)))

(defn apply-create-topic-actions! [admin create-topic-actions]
  (create-topics admin
                 (set/difference (set (map :name create-topic-actions))
                                 (topics admin))))

(defn actions->acl-bindings [actions-by-type]
  (->> (map #(assoc % :resource-type ResourceType/TOPIC)
            (:grant-permission-on-topic actions-by-type))
       (concat (map #(assoc % :resource-type ResourceType/GROUP)
                    (:create-consumer-group-grant actions-by-type)))
       (concat (map #(assoc % :resource-type ResourceType/TRANSACTIONAL_ID)
                    (:grant-permission-on-transactional-id actions-by-type)))
       (map map->acl-binding)
       set))

(defn apply-acl-binding-actions! [admin acl-binding-actions]
  (create-acls admin
               (set/difference acl-binding-actions (acls admin))))

(defn apply-admin-actions! [[cluster-def actions]]
  (let [actions-by-type (group-by :action actions)]
    (with-open [admin (create-admin-client cluster-def)]
      (apply-create-topic-actions! admin
                                   (:create-topic actions-by-type))
      (apply-acl-binding-actions! admin
                                  (actions->acl-bindings actions-by-type)))))

(defn configure-kafka-for-app! [app-def]
  (run! apply-admin-actions! (admin-actions-by-cluster app-def)))

(comment

  (admin-actions-by-cluster test-app-def)

  (configure-kafka-for-app! test-app-def)
  
  (kafka-entity-def->admin-actions
   (p/init
    {:name :test-topic
     :type :kafka-consumer-group-topic
     :kafka-consumer-group "testcg9"
     :kafka-cluster :data-exchange
     :kafka-topic "genegraph-test"
     :kafka-user "kafka-user"}))

  (def dx-ccloud-dev
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
                       "org.apache.kafka.common.serialization.StringSerializer"}})

  (with-open [admin (create-admin-client dx-ccloud-dev)]
    (delete-topic admin "genegraph-test")
    (delete-topic admin "genegraph-test-out"))

  (def test-app-def
    (p/init
     {:type :genegraph-app
      :kafka-clusters {:data-exchange dx-ccloud-dev}
      :topics {:test-topic
               {:name :test-topic
                :type :kafka-consumer-group-topic
                :kafka-consumer-group "testcg9"
                :kafka-cluster :data-exchange
                :kafka-topic "genegraph-test"}
               :test-reader
               {:name :test-reader
                :type :kafka-reader-topic
                :kafka-cluster :data-exchange
                :kafka-topic "genegraph-test"}
               :test-endpoint
               {:name :test-endpoint
                :type :kafka-producer-topic
                :serialization :json
                :kafka-topic "genegraph-test-out"
                :kafka-cluster :data-exchange}
               :test-input
               {:name :test-input
                :type :kafka-producer-topic
                :serialization :json
                :kafka-topic "genegraph-test"
                :kafka-cluster :data-exchange}
               :publish-to-test
               {:name :publish-to-test
                :type :simple-queue-topic}}
      :processors {:test-processor
                   {:subscribe :test-topic
                    :name :test-processor
                    :type :parallel-processor
                    :kafka-cluster :data-exchange
                    :interceptors `[identity]}
                   :test-publisher
                   {:name :test-publisher
                    :subscribe :publish-to-test
                    :kafka-cluster :data-exchange
                    :type :processor
                    :interceptors `[identity]}
                   :test-reader-processor
                   {:name :test-reader-processor
                    :subscribe :test-reader
                    :kafka-cluster :data-exchange
                    :type :processor
                    :backing-store :test-jena
                    :interceptors `[identity]}
                   :test-endpoint
                   {:name :test-endpoint
                    :type :processor
                    :interceptors `[identity]}}}))
  
  )

