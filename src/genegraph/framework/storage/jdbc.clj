(ns genegraph.framework.storage.jdbc
  (:require [genegraph.framework.storage :as storage]
            [genegraph.framework.protocol :as p]
            [next.jdbc :as jdbc]
            [next.jdbc.sql :as sql]
            [next.jdbc.connection :as connection])
  (:import [com.zaxxer.hikari HikariDataSource]))

(defrecord JdbcDb [connection-spec name instance state]
  p/Lifecycle
  (start [_]
    (reset! instance (connection/->pool HikariDataSource connection-spec))
    (swap! state assoc :status :running))
  (stop [_]
    (.close @instance)
    (swap! state assoc :status :stopped)))

(defmethod p/init :jdbc [spec]
  (map->JdbcDb (assoc spec :instance (atom nil) :state (atom {}))))

(defn store-offset
  ([this topic offset]
   (store-offset this topic offset nil))
  ([this topic offset commit-promise]
   (jdbc/execute-one! this ["
INSERT INTO kafka_topic_offsets(kafka_topic, kafka_offset)
VALUES(?, ?)
ON CONFLICT ON CONSTRAINT kafka_topic_offsets_pkey
DO UPDATE SET kafka_offset = ? WHERE kafka_topic_offsets.kafka_topic = ?;
"
                            (str topic) offset offset (str topic)])))

(defn retrieve-offset [this topic]
  (:kafka_topic_offsets/kafka_offset
   (jdbc/execute-one! this ["
SELECT kafka_offset FROM kafka_topic_offsets WHERE kafka_topic = ?"
                            (str topic)])))

(extend HikariDataSource
  storage/TopicBackingStore
  {:store-offset store-offset
   :retrieve-offset retrieve-offset})

(defn create-offets-table [ds]
  (jdbc/execute-one! ds ["
CREATE TABLE kafka_topic_offsets (
 kafka_topic varchar PRIMARY KEY,
 kafka_offset bigint);
"]))

(comment
  (def pg
    (p/init {:name :pg-test
             :type :jdbc
             :connection-spec {:dbtype "postgresql"
                               :dbname "genegraph"}}))

  (p/start pg)
  (p/stop pg)

  (create-offets-table @(:instance pg))

  (store-offset @(:instance pg) :fake-topic 2)

  (retrieve-offset @(:instance pg) :fake-topic)

  )
