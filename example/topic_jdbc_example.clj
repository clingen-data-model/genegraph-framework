(ns genegraph.topic-jdbc-example
  (:require [genegraph.framework.app :as app]
            [genegraph.framework.protocol :as p]
            [genegraph.framework.event :as event]
            [genegraph.framework.event.store :as event-store]
            [next.jdbc :as jdbc]
            [io.pedestal.interceptor :as interceptor]
            [io.pedestal.log :as log]
            [clojure.pprint :refer [pprint]]))

(def dx-prod
  {:type :kafka-cluster
   :kafka-user "User:2189780"
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


(defn update-gpm [event]
  (let [{:keys [id name type affiliation_id]}
        (get-in event [::event/data :data :expert_panel])]
    (log/info :fn ::update-gpm :id id :name name)
    (event/execute event :pg ["
INSERT INTO expert_panels(id, name, type, affiliation_id)
VALUES(?, ?, ?, ?)
ON CONFLICT ON CONSTRAINT expert_panels_pkey
DO UPDATE SET name = ?, type = ?, affiliation_id = ?
WHERE expert_panels.id = ?; 
" id name type affiliation_id name type affiliation_id id])))

(def update-gpm-interceptor
  (interceptor/interceptor
   {:name ::update-gpm-interceptor
    :enter (fn [e] (update-gpm e))}))

(def jdbc-example-app-def
  {:type :genegraph-app
   :kafka-clusters {:dx dx-prod}
   :topics {:gpm-general
            {:name :gpm-general
             :type :kafka-reader-topic
             :kafka-topic "gpm-general-events"
             :serialization :json
             :kafka-cluster :dx}}
   :storage {:pg
             {:name :pg
              :type :jdbc
              :connection-spec {:dbtype "postgresql"
                                :dbname "genegraph"}}}
   :processors {:load-gpm-general
                {:name :load-gpm-general
                 :type :processor
                 :subscribe :gpm-general
                 :backing-store :pg
                 :interceptors [update-gpm-interceptor]}}})

(def jdbc-example-local-test-def
  {:type :genegraph-app
   :kafka-clusters {:dx dx-prod}
   :topics {:gpm-general
            {:name :gpm-general
             :type :simple-queue-topic}}
   :storage {:pg
             {:name :pg
              :type :jdbc
              :connection-spec {:dbtype "postgresql"
                                :dbname "genegraph"}}}
   :processors {:load-gpm-general
                {:name :load-gpm-general
                 :type :processor
                 :subscribe :gpm-general
                 :interceptors [update-gpm-interceptor]}}})

(defn init-db [dc]
  (jdbc/execute-one! dc ["
DROP TABLE IF EXISTS expert_panels;

CREATE TABLE expert_panels (
id varchar PRIMARY KEY,
name varchar,
type varchar,
affiliation_id varchar
);
"]))

(comment

  (def app (p/init jdbc-example-local-test-def))
  (def app (p/init jdbc-example-app-def))
  (p/start app)
  (p/stop app)
  (init-db @(get-in app [:storage :pg :instance]))
  
  (event-store/with-event-reader [r "/Users/tristan/desktop/gpm_general.edn.gz"]
    (p/publish
     (get-in app [:topics :gpm-general])
     (->> (event-store/event-seq r)
          (map event/deserialize)
          (filter #(and (= "ep_definition_approved"
                           (get-in % [::event/data :event_type]))))
          first)))

  (event-store/with-event-reader [r "/Users/tristan/desktop/gpm_general.edn.gz"]
    (def e1
      (->> (event-store/event-seq r)
           (map event/deserialize)
           (filter #(and (= "ep_definition_approved"
                            (get-in % [::event/data :event_type]))))
           first)))

  (p/process (get-in app [:processors :load-gpm-general]) e1)

  )
