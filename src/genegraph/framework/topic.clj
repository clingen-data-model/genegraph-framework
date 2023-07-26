(ns genegraph.framework.topic
  "Defines the logic for handling topics"
  (:require [clojure.spec.alpha :as spec]
            [genegraph.framework.protocol :as p]
            [genegraph.framework.storage :as storage]
            [genegraph.framework.event :as event]
            [genegraph.framework.event.store :as event-store]
            [clojure.edn :as edn]
            [clojure.java.io :as io])
  (:import  [java.util.concurrent BlockingQueue ArrayBlockingQueue TimeUnit]
            [java.util List ArrayList Properties]
            [java.time Duration]
            [java.io PushbackReader]
            [org.apache.kafka.clients.admin Admin NewTopic CreateTopicsResult]
            [org.apache.kafka.clients.consumer KafkaConsumer Consumer ConsumerRecord ConsumerRecords]
            [org.apache.kafka.clients.producer KafkaProducer Producer ProducerRecord]
            [org.apache.kafka.common PartitionInfo TopicPartition KafkaFuture]))

;; TODO start here tomorrow
;; Think about how to handle topics appropriately, consult notes
;; implement modal topics, see modality

(def topic-defaults
  {:timeout 1000
   :buffer-size 100})

(defn consumer-record->clj [record]
  {::event/key (.key record)
   ::event/value (.value record)
   ::event/timestamp (.timestamp record)
   ::event/kafka-topic (.topic record)
   ::event/source :kafka
   ::event/offset (.offset record)})

;; TODO need some logging, communication around this.
;; If this fails then the application is in a degraded state
(defn- start-kafka-consumer [topic]
  (let [topic-partitions [(TopicPartition. (:kafka-topic topic) 0)]
        consumer (doto (KafkaConsumer.
                        (merge (get-in topic [:kafka-cluster :common])
                               (get-in topic [:kafka-cluster :consumer])))
                   (.assign topic-partitions)
                   (.seekToBeginning topic-partitions))]
    (reset! (:state topic) :running)
    (reset! (:consumer topic) consumer)
    (reset! (:kafka-last-offset-at-start topic)
            (-> (.endOffsets consumer topic-partitions)
                first
                val
                dec))
    (while (= :running @(:state topic))
      (->> (.poll consumer (Duration/ofMillis 100))
           .iterator
           iterator-seq
           (map consumer-record->clj)
           (map #(assoc % ::event/topic-name (:name topic)))
           (run! #(p/offer topic %))))))

(defn- start-kafka-producer [topic]
  (reset! (:producer topic)
          (KafkaProducer. (merge (get-in topic [:kafka-cluster :common])
                                 (get-in topic [:kafka-cluster :producer])))))

(defn event-seq-from-storage [def]
  (->> (storage/scan (:instance def) (:path def))
       (map #(-> % io/reader PushbackReader. edn/read))))

(comment
  (->>
   (event-seq-from-storage
    {:instance
     (genegraph.framework.storage.filesystem/->Filesystem
      "/Users/tristan/data/genegraph/2023-04-13T1609/")
     :path "events/:gci-raw-snapshot"})
   first
   type)
  )

(defn- offer-initial-events-from-input [topic input]
  (event-store/with-event-reader [r input]
    (loop [offset 0
           initial-events (event-store/event-seq r)]
      (println offset)
      (println (type initial-events))
      (when-let [e (first initial-events)]
        (println "key " (::event/key e))
        (p/offer topic
                 (assoc e
                        ::event/offset
                        offset
                        ::event/source
                        :initial-events
                        ::event/topic-name
                        (:name topic)))
        (recur (inc offset) (rest initial-events))))))

(defn- offer-initial-events-from-seq [topic es]
  (loop [offset 0
         initial-events es]
    (when-let [e (peek initial-events)]
      (p/offer topic
               (assoc e
                      ::event/offset
                      offset
                      ::event/source
                      :initial-events
                      ::event/topic-name
                      (:name topic)))
      (recur (inc offset) (rest initial-events)))))

(defn- offer-initial-events [topic]
  (let [e (:initial-events topic)]
    (cond
      (string? e) (offer-initial-events-from-input topic e)
      (map? e) (offer-initial-events-from-input topic (storage/as-handle e))
      (sequential? e) (offer-initial-events-from-seq topic e)))
  (reset! (:initial-events-complete topic) true))

(defn- start-topic [topic]
  (.start
   (Thread.
    (fn []
      (offer-initial-events topic)
      (when (and (:run-kafka-consumer topic)
                 (:kafka-topic topic)
                 (:kafka-cluster topic))
        (start-kafka-consumer topic))))))

(defrecord Topic [name
                  buffer-size
                  timeout
                  queue
                  consumer
                  producer
                  kafka-cluster
                  state
                  initial-events
                  initial-events-complete
                  kafka-last-offset-at-start
                  kafka-most-recent-offset
                  run-kafka-consumer
                  run-kafka-producer]
  
  p/Lifecycle
  (start [this]
    (reset! (:state this) :running)
    (start-topic this))
  (stop [this]
    (reset! state :stopped))

  ;; p/Queue
  ;; (poll [this]
  ;;   (when-let [e (.poll (:queue this)
  ;;                       (:timeout this)
  ;;                       TimeUnit/MILLISECONDS)]
  ;;     (when (= :kafka (::event/source e))
  ;;       (reset! kafka-most-recent-offset (::event/offset e)))
  ;;     e))
  
  ;; (offer [this x]
  ;;   (.offer (:queue this)
  ;;           x
  ;;           (:timeout this)
  ;;           TimeUnit/MILLISECONDS)))

(defmethod p/init :topic  [topic-definition]
  (let [topic-def-with-defaults (merge topic-defaults
                                       topic-definition)]
    (map->Topic
     (assoc topic-def-with-defaults
            :queue (ArrayBlockingQueue. (:buffer-size topic-def-with-defaults))
            :consumer (atom nil)
            :producer (atom nil)
            :state (atom :stopped)
            ;; arbitrarily set > kafka-most-recent so we don't trigger up-to-date?
            :kafka-last-offset-at-start (atom 1) 
            :kafka-most-recent-offset (atom 0)
            :initial-events-complete (atom false)))))

(defn up-to-date?
  "Return true if all messages have been consumed up to the point when
  the consumer was started."
  [topic]
  (and @(:initial-events-complete topic)
       (<= @(:kafka-last-offset-at-start topic)
           @(:kafka-most-recent-offset topic))))

(defn key-for-event-store
  "Generate key that enforces ordering for events stored directly
  in a store; with topic first, source to enforce initial events
  first, followed by offset.

  Intended to be used for databases that offer ordered sequences
  of keys, such as RocksDB"
  [e]
  (let [event-source-idx {:initial-events 100
                          :kafka 200}]
    [(::event/topic-name e)
     (event-source-idx (::event/source e))
     (::event/offset e)]))

(comment
  (key-for-event-store {::event/topic-name :test
                        ::event/source :kafka
                        ::event/offset 3})
  )

(defn store-events
  "Take events from TOPIC and place them in STORAGE, in sequence and starting
  at the beginning of TOPIC, including initial seed events.
  Uses topic queue, topic must be initialized, but must not be running.
  Will store initial events prior to kafka events."
  [event-store topic-def]
  (let [topic (p/init (assoc topic-def
                             :run-kafka-consumer
                             true))]
    (p/start topic)
    (loop []
      (when-let [e (p/poll topic)]
        (storage/write event-store
                       (key-for-event-store e)
                       e))
      (when-not (up-to-date? topic)
        (recur)))
    (p/stop topic)))

(defn store-events-in-event-store
  "Same as store-events, but use a genegraph.framework.event.store"
  [event-store topic-def]
  (event-store/with-event-writer [w event-store]
    (let [topic (p/init (assoc topic-def
                               :run-kafka-consumer
                               true))]
      (p/start topic)
      (loop []
        (when-let [e (p/poll topic)]
          (prn e))
        (when-not (up-to-date? topic)
          (recur)))
      (p/stop topic))))

(comment
  (def t2
    {:name :test-topic
     :type :topic
     :initial-events [{::event/key "e1"
                       ::event/value (pr-str {:test :event})
                       ::event/format :edn}]
     :kafka-cluster { :common {"ssl.endpoint.identification.algorithm" "https"
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
                                "org.apache.kafka.common.serialization.StringSerializer"}}
     :kafka-topic "actionability"})
  (def s (p/init
          {:name :test-event-store
           :type :rocksdb
           :path "/users/tristan/desktop/test-event-store"}))
  
  (p/start s)
  (p/stop s)

  (store-events @(:instance s) t2)

  (count (storage/scan @(:instance s) :test-topic))
  
  )


(comment
  (def t (p/init {:name :test
                  :type :topic
                  :buffer-size 3}))
  (p/offer t {:hi :there})
  (p/poll t))

(comment
  (def t3 (p/init
           {:name :local-test-topic
            :type :topic
            :kafka-cluster {:common {"bootstrap.servers" "localhost:9092"}
                            :consumer {"key.deserializer"
                                       "org.apache.kafka.common.serialization.StringDeserializer"
                                       "value.deserializer"
                                       "org.apache.kafka.common.serialization.StringDeserializer"}
                            :producer {"key.serializer"
                                       "org.apache.kafka.common.serialization.StringSerializer"
                                       "value.serializer"
                                       "org.apache.kafka.common.serialization.StringSerializer"}}
            :run-kafka-producer true}))

  

  

  )

