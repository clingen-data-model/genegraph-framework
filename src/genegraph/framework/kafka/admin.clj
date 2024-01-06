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
  {"retention.ms" "-1"} ; retain indefinitely by default
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

(defn kafka-entity-def->admin-actions
  "Translate the definition of a topic in a Genegraph app into the actions
  needed to make that topic useable to a client:
  For topics, the required actions are:  create the topic, create the
  needed consumer group, create the read/write permissions on the topic.
  For processors, the kafka user needs to have permissions on the transactional-id"
  [{:keys [kafka-topic kafka-consumer-group kafka-user]}]
  (->> [{:action :create-topic
         :name kafka-topic}
        {:action :create-consumer-group-grant
         :name kafka-consumer-group
         :principal kafka-user}
        {:action :grant-permission-on-topic
         :name kafka-topic
         :principal kafka-user}]
       (remove #(some nil? (vals %)))
       set))

#_(defn admin-actions-by-cluster [app-def]
    (update-vals (dissoc (group-by :kafka-cluster
                                   (concat (vals (:topics app-def))
                                           (vals (:processors app-def))))
                         nil)
                 (fn [topics]
                   (reduce (fn [actions topic]
                             (set/union actions
                                        (kafka-topic-def->admin-actions topic)))
                           #{}
                           topics))))

(defn admin-actions-by-cluster [app-def]
  (map (fn [[cluster entities]]
         (reduce (fn [actions entity]
                   (set/union actions
                              (kafka-entity-def->admin-actions entity)))
                 #{}
                 (map #(assoc % :kafka-user (:kafka-user cluster))
                      entities)))
       (dissoc (group-by :kafka-cluster
                         (concat (vals (:topics app-def))
                                 (vals (:processors app-def))))
               nil)))

;; refactor
(defn topics-to-create [app-def admin]
  (set/difference (app->kafka-topics app-def)
                  (topics admin)))
;;refactor
(defn create-topics-in-cluster! [cluster genegraph-app-topics]
  (with-open [admin (create-admin-client cluster)]
    (let [app-kafka-topics (set (map :kafka-topic genegraph-app-topics))] 
      (create-topics admin
                     (set/difference app-kafka-topics (topics admin))))))

(defn apply-create-topic-actions! [admin create-topic-actions]
  (create-topics admin
                 (set/difference (set (map :name create-topic-actions))
                                 (topics admin))))

(defn actions->acl-bindings [actions-by-type]
  (->> (map #(assoc % :resource-type ResourceType/TOPIC)
            (:grant-permission-on-topic actions-by-type))
       (concat (map #(assoc % :resource-type ResourceType/GROUP)
                    (:create-consumer-group-grant actions-by-type)))
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


(comment

  (admin-actions-by-cluster test-app-def)

  (-> (admin-actions-by-cluster test-app-def)
      first
      apply-admin-actions!)
  
  (kafka-topic-def->admin-actions
   {:name :test-topic
    :type :kafka-consumer-group-topic
    :kafka-consumer-group "testcg9"
    :kafka-cluster :data-exchange
    :kafka-topic "genegraph-test"
    :kafka-user "kafka-user"})


  (update-vals (group-by :kafka-cluster (vals (:topics test-app-def)))
               (fn [topics]
                 (reduce (fn [actions topic]
                           (set/union actions
                                      (kafka-topic-def->admin-actions topic)))
                         #{}
                         topics)))
  
  
  (-> (:topics test-app-def)
      (group-by :kafka-cluster)
      (update-vals))
  )

;; refactor
(defn app->kafka-topics [app-def]
  (->> (vals (:topics app-def))
       (filter :kafka-topic)
       (map #(select-keys % [:kafka-topic :kafka-cluster :kafka-user]))
       set))

(defn app->kafka-topics [app-def]
  (->> (vals (:topics app-def))
       (filter :kafka-topic)
       (map #(select-keys % [:kafka-topic :kafka-cluster :kafka-user]))
       set))

;;refacotr
(defn app->kafka-consumer-groups [app-def]
  (->> (vals (:topics app-def))
       (filter :kafka-consumer-group)
       (map #(select-keys % [:kafka-consumer-group :kafka-cluster :kafka-user]))
       set))

(comment
  

  )



(defn create-topics-for-app! [app-def]
  (run! (fn [[cluster topics]] (create-topics-in-cluster! cluster topics))
        (group-by :kafka-cluster
                  (app->kafka-topics app-def))))

(defn create-consumer-group-acls-for-app-in-cluster! [cluster consumer-groups]
  )

(defn create-consumer-group-acls-for-app! [app-def]
  (run! (fn [[cluster consumer]])))

(defn create-acls-for-app! [app-def]
  (run! (fn [[cluster topic-users]] (create-acls-in-cluster! cluster topic-users))
        (group-by :kafka-cluster ())))

(comment

  (app->kafka-consumer-groups test-app-def)

  (create-topics-for-app! test-app-def)
  
  (with-open [admin (create-admin-client dx-ccloud-dev)]
    (topics-to-create test-app-def admin))
  
  (def kafka-user "User:2189780")
  
  (->> test-acls
       (map acl-binding->map)
       (filter #(= ResourceType/GROUP (:resource-type %)))
       (filter #(= Operati (:operation %)))
       (map :operation )
       frequencies
       )
  
  (-> test-acls first acl-binding->map)
  
  (with-open [admin (create-admin-client local-cluster)]
    (create-topics admin ["test-topic"]))

  (with-open [admin (create-admin-client local-cluster)]
    (delete-topic admin "test-topic"))

  (with-open [admin (create-admin-client dx-ccloud-dev)]
    (topics admin))

  (with-open [admin (create-admin-client local-cluster)]
    (acls admin))
  
  (def test-acls
    (with-open [admin (create-admin-client dx-ccloud-dev)]
      (acls admin)))
  

  
  )

(comment
  (def local-cluster
    {:common-config {"bootstrap.servers" "localhost:9092"}
     :producer-config {"key.serializer"
                       "org.apache.kafka.common.serialization.StringSerializer",
                       "value.serializer"
                       "org.apache.kafka.common.serialization.StringSerializer"}
     :consumer-config {"key.deserializer"
                       "org.apache.kafka.common.serialization.StringDeserializer"
                       "value.deserializer"
                       "org.apache.kafka.common.serialization.StringDeserializer"}})

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

  (with-open [admin (create-admin-client local-cluster)]
    (-> (.createTopics admin
                       [(doto (NewTopic. "gene_validity_complete" 1 (short 1))
                          (.configs {"retention.ms" "-1"}))])
        .all
        .get))

  (with-open [admin (create-admin-client local-cluster)]
    (-> (.listTopics admin)
        .namesToListings
        deref
        keys))

  (with-open [admin (create-admin-client local-cluster)]
    (-> (.describeConfigs admin [(ConfigResource. ConfigResource$Type/TOPIC "gene_validity_complete")])
        .all
        derefp
        first
        val
        .entries
        first))

  (with-open [admin (create-admin-client local-cluster)]
    (-> (.createTopics admin
                       [(doto (NewTopic. "gene_validity_sepio" 1 (short 1))
                          (.configs {"retention.ms" "-1"}))])
        .all
        deref))

  (with-open [admin (create-admin-client local-cluster)]
    (-> (.deleteTopics admin ["gene_validity_sepio"])
        .all
        .get))

  (with-open [admin (create-admin-client dx-ccloud)]
    (-> (.listConsumerGroups admin)
        .all
        deref
        ))

  
  (def acls
    (with-open [admin (create-admin-client dx-ccloud)]
      (-> (.describeAcls admin AclBindingFilter/ANY)
          .values
          deref)))

  (count acls)

  (-> acls
      first
)

  

  )
