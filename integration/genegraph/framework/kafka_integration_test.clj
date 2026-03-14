(ns genegraph.framework.kafka-integration-test
  "Infrastructure tests for Kafka topic types and producers.
   Requires a Kafka broker running at localhost:9092.
   Run with: clojure -X:kafka"
  (:require [genegraph.framework.kafka :as kafka]
            [genegraph.framework.kafka.admin :as kafka-admin]
            [genegraph.framework.protocol :as p]
            [genegraph.framework.event :as event]
            [genegraph.framework.topic]
            [clojure.test :refer [deftest testing is use-fixtures]])
  (:import [org.apache.kafka.clients.admin Admin NewTopic]
           [org.apache.kafka.clients.consumer KafkaConsumer]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.common.errors
            ApplicationRecoverableException
            InvalidConfigurationException]
           [java.time Duration]
           [java.util.concurrent TimeUnit]))

;; ---------------------------------------------------------------------------
;; Cluster config and connectivity check
;; ---------------------------------------------------------------------------

(def local-cluster
  {:common-config {"bootstrap.servers"      "localhost:9092"
                   "request.timeout.ms"     "5000"
                   "default.api.timeout.ms" "5000"}
   :consumer-config {"key.deserializer"
                     "org.apache.kafka.common.serialization.StringDeserializer"
                     "value.deserializer"
                     "org.apache.kafka.common.serialization.StringDeserializer"}
   :producer-config {"key.serializer"
                     "org.apache.kafka.common.serialization.StringSerializer"
                     "value.serializer"
                     "org.apache.kafka.common.serialization.StringSerializer"}})

(defn kafka-available?
  "Return true if a Kafka broker is reachable at localhost:9092."
  []
  (try
    (with-open [admin (Admin/create ^java.util.Map (get local-cluster :common-config))]
      (-> admin .listTopics .names deref)
      true)
    (catch Exception _ false)))

(defn kafka-fixture
  "Skip all tests if Kafka is unreachable.
   After the test suite, sleep briefly so non-daemon framework threads (consumer
   loop, status-queue monitor) have time to notice the :stopped status and exit.
   This prevents the JVM from hanging after the test-runner prints its summary."
  [f]
  (if (kafka-available?)
    (try
      (f)
      (finally
        ;; Framework topics are polled with :timeout 200ms.  After p/stop sets
        ;; status to :stopped, each thread wakes on the next poll and exits.
        ;; 1 second is ample time for all threads to drain.
        (Thread/sleep 1000)))
    (println "\nINFO: Kafka not available at localhost:9092 — skipping integration tests.")))

(use-fixtures :once kafka-fixture)

;; ---------------------------------------------------------------------------
;; Helpers: unique names
;; ---------------------------------------------------------------------------

(defn unique-topic-name [] (str "genegraph-test-" (random-uuid)))
(defn unique-cg-name    [] (str "genegraph-test-cg-" (random-uuid)))

;; ---------------------------------------------------------------------------
;; Helpers: Kafka topic lifecycle (single-partition, replication-factor 1)
;; ---------------------------------------------------------------------------

(defn create-test-topic!
  "Create a single-partition, single-replica Kafka topic (suitable for local testing)."
  [topic-name]
  (with-open [admin (Admin/create ^java.util.Map (get local-cluster :common-config))]
    (-> (.createTopics admin [(NewTopic. topic-name 1 (short 1))])
        .all
        .get)))

(defn delete-test-topic!
  "Delete a Kafka topic; silently ignore errors (topic may already be gone)."
  [topic-name]
  (try
    (with-open [admin (Admin/create ^java.util.Map (get local-cluster :common-config))]
      (-> (.deleteTopics admin [topic-name])
          .all
          .get))
    (catch Exception _)))

(defmacro with-test-topic
  "Bind topic-name-sym to a unique Kafka topic name, create it, run body, delete it."
  [[topic-name-sym] & body]
  `(let [~topic-name-sym (unique-topic-name)]
     (create-test-topic! ~topic-name-sym)
     (try
       ~@body
       (finally
         (delete-test-topic! ~topic-name-sym)))))

;; ---------------------------------------------------------------------------
;; Helpers: raw produce and consume (bypass the framework)
;; ---------------------------------------------------------------------------

(defn produce-raw!
  "Produce [key value] string pairs to a Kafka topic.
   Blocks until all records are acknowledged by the broker."
  [topic-name kv-pairs]
  (with-open [producer (KafkaProducer.
                        ^java.util.Map
                        (merge (get local-cluster :common-config)
                               (get local-cluster :producer-config)))]
    (doseq [[k v] kv-pairs]
      @(.send producer (ProducerRecord. ^String topic-name ^String k ^String v)))
    (.flush producer)))

(defn consume-n-records
  "Consume up to n records from topic-name using a raw KafkaConsumer.
   Returns a vector of ConsumerRecord objects.
   extra-config is merged into the consumer properties."
  ([topic-name cg-name n timeout-ms]
   (consume-n-records topic-name cg-name n timeout-ms {}))
  ([topic-name cg-name n timeout-ms extra-config]
   (with-open [consumer (KafkaConsumer.
                         ^java.util.Map
                         (merge (get local-cluster :common-config)
                                (get local-cluster :consumer-config)
                                {"group.id"          cg-name
                                 "auto.offset.reset"  "earliest"
                                 "enable.auto.commit" "false"}
                                extra-config))]
     (.subscribe consumer [topic-name])
     (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
       (loop [collected []]
         (if (>= (count collected) n)
           collected
           (if (> (System/currentTimeMillis) deadline)
             collected
             (recur (into collected
                          (->> (.poll consumer (Duration/ofMillis 500))
                               .iterator
                               iterator-seq
                               vec))))))))))

(defn consume-and-commit!
  "Consume exactly n records with consumer group cg-name, then commitSync.
   This sets the consumer group's committed offset so a subsequent CGT
   will see it as the 'initial-consumer-group-offset'.
   Returns the consumed ConsumerRecord objects."
  [topic-name cg-name n timeout-ms]
  (with-open [consumer (KafkaConsumer.
                        ^java.util.Map
                        (merge (get local-cluster :common-config)
                               (get local-cluster :consumer-config)
                               {"group.id"          cg-name
                                "auto.offset.reset"  "earliest"
                                "enable.auto.commit" "false"}))]
    (.subscribe consumer [topic-name])
    (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
      (loop [collected []]
        (if (>= (count collected) n)
          (do (.commitSync consumer) collected)
          (if (> (System/currentTimeMillis) deadline)
            (throw (ex-info "Timeout waiting for records to commit"
                            {:expected n :got (count collected)}))
            (recur (into collected
                         (->> (.poll consumer (Duration/ofMillis 500))
                              .iterator
                              iterator-seq
                              vec)))))))))

;; ---------------------------------------------------------------------------
;; Helpers: framework topic polling and lifecycle
;; ---------------------------------------------------------------------------

(defn poll-n-events
  "Poll up to n events from a framework topic's internal queue within timeout-ms.
   Returns however many events arrived (may be fewer than n if the timeout expires)."
  [topic n timeout-ms]
  (let [queue    (:queue topic)
        deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop [collected []]
      (if (= n (count collected))
        collected
        (let [remaining (- deadline (System/currentTimeMillis))]
          (if (<= remaining 0)
            collected
            (if-let [ev (.poll queue remaining TimeUnit/MILLISECONDS)]
              (recur (conj collected ev))
              collected)))))))

(defn deliver-all-cps!
  "Deliver true to every ::event/completion-promise in events.
   The framework's status-queue monitor blocks on each promise with a 1-hour
   default timeout; delivering them lets the monitor thread finish promptly
   so the JVM can exit after p/stop."
  [events]
  (run! #(deliver (::event/completion-promise %) true) events))

(defn await-promise
  "Deref p within timeout-ms; throw ex-info if the timeout elapses."
  ([p] (await-promise p 5000))
  ([p timeout-ms]
   (let [v (deref p timeout-ms ::timeout)]
     (when (= ::timeout v)
       (throw (ex-info "Promise not delivered within timeout" {:timeout-ms timeout-ms})))
     v)))

;; ---------------------------------------------------------------------------
;; KafkaConsumerGroupTopic — basic lifecycle
;; ---------------------------------------------------------------------------

(deftest consumer-group-topic-lifecycle-test
  (testing "start → produce events → poll events → stop"
    (with-test-topic [topic-name]
      (produce-raw! topic-name [["k0" "v0"] ["k1" "v1"] ["k2" "v2"]])
      (let [t (p/init {:name                 :test-cgt
                       :type                 :kafka-consumer-group-topic
                       :kafka-cluster        local-cluster
                       :kafka-topic          topic-name
                       :kafka-consumer-group (unique-cg-name)
                       :timeout              200})]
        ;; nil local-offset → backing-store-lags? returns false → no replay.
        (p/set-offset! t nil)
        (p/start t)
        (try
          (let [events (poll-n-events t 3 15000)]
            (is (= 3 (count events)) "all three events should arrive")
            (is (= #{"k0" "k1" "k2"} (set (map ::event/key events)))
                "all three keys should be present")
            (is (every? #(= :kafka (::event/source %)) events)
                "source should be :kafka")
            (is (= #{0 1 2} (set (map ::event/offset events)))
                "offsets should be 0, 1, 2")
            (is (every? #(= topic-name (::event/kafka-topic %)) events)
                "kafka-topic should match")
            ;; Deliver CPs so the status-queue monitor thread can finish promptly.
            (deliver-all-cps! events))
          (finally
            (p/stop t)))))))

;; ---------------------------------------------------------------------------
;; KafkaConsumerGroupTopic — last-completed-offset tracking
;; ---------------------------------------------------------------------------

(deftest consumer-group-topic-offset-tracking-test
  (testing "last-completed-offset in state advances as completion promises are delivered"
    (with-test-topic [topic-name]
      (produce-raw! topic-name [["a" "1"] ["b" "2"] ["c" "3"] ["d" "4"] ["e" "5"]])
      (let [t (p/init {:name                 :test-cgt-offsets
                       :type                 :kafka-consumer-group-topic
                       :kafka-cluster        local-cluster
                       :kafka-topic          topic-name
                       :kafka-consumer-group (unique-cg-name)
                       :timeout              200})]
        (p/set-offset! t nil)
        (p/start t)
        (try
          (let [events (poll-n-events t 5 15000)]
            (is (= 5 (count events)) "all five events should arrive")
            ;; Deliver completion promises; the status-queue monitor updates
            ;; :last-completed-offset after each delivery.
            (doseq [ev events]
              (deliver (::event/completion-promise ev) true))
            ;; Give the monitor thread time to process all five status updates.
            (Thread/sleep 500)
            (is (= 4 (:last-completed-offset @(:state t)))
                "last-completed-offset should be 4 (offset of the fifth event)"))
          (finally
            (p/stop t)))))))

;; ---------------------------------------------------------------------------
;; KafkaConsumerGroupTopic — backing-store replay
;; ---------------------------------------------------------------------------

(deftest backing-store-replay-test
  (testing "events are replayed with skip-publish-effects when backing-store lags consumer group"
    (with-test-topic [topic-name]
      (produce-raw! topic-name [["r0" "v0"] ["r1" "v1"] ["r2" "v2"]
                                ["r3" "v3"] ["r4" "v4"]])
      (let [cg-name (unique-cg-name)]
        ;; Step 1: consume all 5 events and commit offset = 5 for this CG.
        (consume-and-commit! topic-name cg-name 5 15000)
        ;; Step 2: start a new CGT instance with the same CG but local offset = 0.
        ;; This simulates a local backing store that has not processed any events.
        (let [t (p/init {:name                 :test-cgt-replay
                         :type                 :kafka-consumer-group-topic
                         :kafka-cluster        local-cluster
                         :kafka-topic          topic-name
                         :kafka-consumer-group cg-name
                         :timeout              200})]
          (p/set-offset! t 0)      ; local-offset = 0; CG offset = 5 → lags!
          (p/start t)
          (try
            (let [events (poll-n-events t 5 15000)]
              (is (= 5 (count events)) "all five replay events should arrive")
              (is (every? ::event/skip-publish-effects events)
                  "replayed events must have skip-publish-effects true")
              (deliver-all-cps! events))
            (finally
              (p/stop t))))))))

;; ---------------------------------------------------------------------------
;; KafkaReaderTopic
;; ---------------------------------------------------------------------------

(deftest kafka-reader-topic-test
  (testing "KafkaReaderTopic delivers all events from offset 0 with skip-publish-effects"
    (with-test-topic [topic-name]
      (produce-raw! topic-name [["rk0" "rv0"] ["rk1" "rv1"] ["rk2" "rv2"]
                                ["rk3" "rv3"] ["rk4" "rv4"]])
      (let [t (p/init {:name          :test-reader
                       :type          :kafka-reader-topic
                       :kafka-cluster local-cluster
                       :kafka-topic   topic-name
                       :timeout       200})]
        ;; Must set-offset! before start: KafkaReaderTopic.start blocks until
        ;; initial-local-offset is delivered before creating the consumer.
        (p/set-offset! t 0)
        (p/start t)
        (try
          (let [events (poll-n-events t 5 15000)]
            (is (= 5 (count events)) "all five events should arrive")
            (is (every? ::event/skip-publish-effects events)
                "all reader-topic events must have skip-publish-effects true")
            (is (= #{"rk0" "rk1" "rk2" "rk3" "rk4"}
                   (set (map ::event/key events)))
                "all keys should be present")
            (deliver-all-cps! events))
          (finally
            (p/stop t))))))

  (testing "KafkaReaderTopic honours a non-zero start offset"
    (with-test-topic [topic-name]
      ;; Produce 6 events: 3 to skip (offsets 0-2), 3 to read (offsets 3-5).
      (produce-raw! topic-name [["skip0" "s"] ["skip1" "s"] ["skip2" "s"]
                                ["read0" "r"] ["read1" "r"] ["read2" "r"]])
      (let [t (p/init {:name          :test-reader-partial
                       :type          :kafka-reader-topic
                       :kafka-cluster local-cluster
                       :kafka-topic   topic-name
                       :timeout       200})]
        (p/set-offset! t 3)
        (p/start t)
        (try
          (let [events (poll-n-events t 3 15000)]
            (is (= 3 (count events))
                "should receive only the three events starting at offset 3")
            (is (every? #(>= (::event/offset %) 3) events)
                "all events should have offset >= 3")
            (deliver-all-cps! events))
          (finally
            (p/stop t)))))))

;; ---------------------------------------------------------------------------
;; GenegraphKafkaProducer — non-transactional
;; ---------------------------------------------------------------------------

(deftest genegraph-producer-basic-test
  (testing "non-transactional producer delivers commit-promise and record reaches Kafka"
    (with-test-topic [topic-name]
      (let [producer (p/init {:name          :test-basic-producer
                              :type          :genegraph-kafka-producer
                              :kafka-cluster local-cluster})]
        (p/start producer)
        (try
          (let [cp (promise)]
            (p/publish producer {::event/kafka-topic topic-name
                                 ::event/key         "pk1"
                                 ::event/data        "pv1"
                                 :commit-promise     cp})
            (is (true? (await-promise cp 10000))
                "commit-promise should be delivered true when broker acks")
            (let [records (consume-n-records topic-name (unique-cg-name) 1 10000)]
              (is (= 1 (count records)) "one record should be in the Kafka topic")
              (is (= "pk1" (.key (first records))) "key should match")))
          (finally
            (p/stop producer)))))))

;; ---------------------------------------------------------------------------
;; GenegraphKafkaProducer — transactional (exactly-once)
;; ---------------------------------------------------------------------------

(deftest genegraph-transactional-producer-test
  (testing "transactional producer: record is visible with read_committed only after p/commit"
    (with-test-topic [out-topic]
      (let [producer (p/init {:name                   :test-tx-producer
                              :type                   :genegraph-kafka-producer
                              :kafka-cluster          local-cluster
                              :kafka-transactional-id (str "genegraph-test-tx-" (random-uuid))})]
        (p/start producer)
        (try
          (let [cp (promise)]
            (p/publish producer {::event/kafka-topic out-topic
                                 ::event/key         "tx-k1"
                                 ::event/data        "tx-v1"
                                 :commit-promise     cp})
            ;; Enqueue the commit; the publish thread will process it after the send.
            (p/commit producer)
            ;; Wait for the broker to ack the record (fires before transaction commit).
            (is (true? (await-promise cp 10000))
                "commit-promise should be true once the broker acks the send")
            ;; With read_committed isolation the record must now be visible.
            (let [records (consume-n-records out-topic (unique-cg-name) 1 10000
                                             {"isolation.level" "read_committed"})]
              (is (= 1 (count records))
                  "exactly one record should be visible with read_committed isolation")
              (is (= "tx-k1" (.key (first records)))
                  "the key should match the published event")))
          (finally
            (p/stop producer)))))))

(deftest genegraph-transactional-producer-abort-test
  (testing "records from an uncommitted transaction are invisible to read_committed consumers"
    ;; We enqueue a record but stop the producer before calling p/commit.
    ;; KafkaProducer.close() aborts any open transaction.
    (with-test-topic [out-topic]
      (let [producer (p/init {:name                   :test-tx-abort
                              :type                   :genegraph-kafka-producer
                              :kafka-cluster          local-cluster
                              :kafka-transactional-id (str "genegraph-test-tx-abort-" (random-uuid))})]
        (p/start producer)
        (p/publish producer {::event/kafka-topic out-topic
                             ::event/key         "uncommitted"
                             ::event/data        "should-not-appear"})
        ;; Stop WITHOUT committing — closes the KafkaProducer and aborts the transaction.
        (p/stop producer)
        (let [records (consume-n-records out-topic (unique-cg-name) 1 3000
                                         {"isolation.level" "read_committed"})]
          (is (= 0 (count records))
              "no records should be visible after transaction abort"))))))

;; ---------------------------------------------------------------------------
;; Helpers: producer error / restart tests
;; ---------------------------------------------------------------------------

(defn await-producer-status
  "Poll (:status @(:state producer)) every 100 ms until it equals expected-status
   or timeout-ms elapses. Returns the status reached. Throws ex-info on timeout."
  [producer expected-status timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (let [status (:status @(:state producer))]
        (cond
          (= expected-status status) status
          (> (System/currentTimeMillis) deadline)
          (throw (ex-info "Timeout waiting for producer status"
                          {:expected expected-status
                           :actual   status
                           :restarts (:restarts @(:state producer))}))
          :else (do (Thread/sleep 100) (recur)))))))

(defn drain-system-topic
  "Return all events currently queued in the system-topic's internal queue,
   waiting up to drain-timeout-ms for the first event to arrive."
  [system-topic drain-timeout-ms]
  (let [queue (:queue system-topic)]
    (loop [collected []]
      (if-let [e (.poll queue (if (empty? collected) drain-timeout-ms 100) TimeUnit/MILLISECONDS)]
        (recur (conj collected e))
        collected))))

;; ---------------------------------------------------------------------------
;; GenegraphKafkaProducer — fatal exception halts without restart
;; ---------------------------------------------------------------------------

(deftest producer-fatal-exception-halts-test
  (testing "InvalidConfigurationException during startup sets status to :failed with no restart"
    (let [producer (p/init {:name          :test-fatal-producer
                            :type          :genegraph-kafka-producer
                            :kafka-cluster local-cluster})]
      (with-redefs [kafka/create-producer!
                    (fn [& _]
                      (throw (InvalidConfigurationException. "simulated fatal config error")))]
        (p/start producer))
      (is (= :failed (:status @(:state producer)))
          "status should be :failed after a fatal exception")
      (is (= 0 (:restarts @(:state producer)))
          "no restart should have been attempted for a fatal exception"))))

;; ---------------------------------------------------------------------------
;; GenegraphKafkaProducer — restart after transient failure
;; ---------------------------------------------------------------------------

(deftest producer-restart-after-transient-failure-test
  (testing "producer restarts and becomes :running after ApplicationRecoverableException"
    (with-test-topic [topic-name]
      (let [real-create! kafka/create-producer!
            call-count   (atom 0)
            producer     (p/init {:name          :test-restart-producer
                                  :type          :genegraph-kafka-producer
                                  :kafka-cluster local-cluster})]
        ;; The first two calls to create-producer! throw a recoverable exception.
        ;; The third call delegates to the real implementation and connects to the broker.
        ;; p/start → create-producer! (call 1, throws) → handle-producer-exception!
        ;;   → attempt-restart! loop:
        ;;     iteration 1 (restarts=1): create-producer! (call 2, throws) → recur
        ;;     iteration 2 (restarts=2): create-producer! (call 3, succeeds) → :running
        (with-redefs [kafka/create-producer!
                      (fn [cluster opts]
                        (if (< (swap! call-count inc) 3)
                          (throw (proxy [ApplicationRecoverableException] ["simulated transient"]))
                          (real-create! cluster opts)))]
          (p/start producer))
        (try
          (is (= :running (:status @(:state producer)))
              "producer should be :running after recovery")
          (is (= 2 (:restarts @(:state producer)))
              "restart counter should reflect two loop iterations")
          ;; Verify the recovered producer can actually deliver a message.
          (let [cp (promise)]
            (p/publish producer {::event/kafka-topic topic-name
                                 ::event/key         "post-restart-key"
                                 ::event/data        "post-restart-value"
                                 :commit-promise     cp})
            (is (true? (await-promise cp 10000))
                "producer should be functional after restart"))
          (finally
            (p/stop producer)))))))

;; ---------------------------------------------------------------------------
;; GenegraphKafkaProducer — error events published to system-topic
;; ---------------------------------------------------------------------------

(deftest producer-restart-reports-to-system-topic-test
  (testing "each recoverable failure publishes an error event to the system-topic"
    (let [real-create! kafka/create-producer!
          call-count   (atom 0)
          system-topic (p/init {:name        :test-sys-topic
                                :type        :simple-queue-topic
                                :buffer-size 20})
          producer     (p/init {:name          :test-sys-reporter
                                :type          :genegraph-kafka-producer
                                :kafka-cluster local-cluster
                                :system-topic  system-topic})]
      (with-redefs [kafka/create-producer!
                    (fn [cluster opts]
                      (if (< (swap! call-count inc) 3)
                        (throw (proxy [ApplicationRecoverableException] ["simulated"]))
                        (real-create! cluster opts)))]
        (p/start producer))
      (try
        ;; Two failures in p/start + attempt-restart! should produce two error events.
        ;; Allow 500 ms for any async publishing to settle before draining.
        (let [errors (drain-system-topic system-topic 500)]
          (is (>= (count errors) 2)
              "at least two error events should have been published (one per failure)")
          (is (every? #(= :test-sys-reporter (:source %)) errors)
              "every error event should carry the producer's :name as :source")
          (is (every? #(= :warn (:severity %)) errors)
              "ApplicationRecoverableException maps to :warn severity"))
        (finally
          (p/stop producer))))))

;; ---------------------------------------------------------------------------
;; Helpers: consumer restart tests
;; ---------------------------------------------------------------------------

(defn await-topic-status
  "Poll :status in the topic state atom until it is in expected-statuses (a set),
   or throw ex-info after timeout-ms."
  [topic expected-statuses timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (let [status (:status @(:state topic))]
        (cond
          (contains? expected-statuses status) status
          (> (System/currentTimeMillis) deadline)
          (throw (ex-info "Timeout waiting for topic status"
                          {:expected expected-statuses
                           :actual   status}))
          :else (do (Thread/sleep 50) (recur)))))))

(defn await-restart-count
  "Poll :restarts in the topic state atom until >= expected-count,
   or throw ex-info after timeout-ms."
  [topic expected-count timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (let [restarts (:restarts @(:state topic))]
        (cond
          (>= restarts expected-count) restarts
          (> (System/currentTimeMillis) deadline)
          (throw (ex-info "Timeout waiting for restart count"
                          {:expected expected-count
                           :actual   restarts}))
          :else (do (Thread/sleep 50) (recur)))))))

;; ---------------------------------------------------------------------------
;; KafkaConsumerGroupTopic — restart on recoverable exception
;; ---------------------------------------------------------------------------

(deftest cgt-restart-on-recoverable-exception-test
  (testing "supervisor restarts once after ApplicationRecoverableException on first poll"
    (with-test-topic [topic-name]
      (produce-raw! topic-name [["k0" "v0"] ["k1" "v1"] ["k2" "v2"] ["k3" "v3"] ["k4" "v4"]])
      (let [real-poll  kafka/poll-kafka-consumer
            call-count (atom 0)
            t          (p/init {:name                 :test-cgt-restart
                                :type                 :kafka-consumer-group-topic
                                :kafka-cluster        local-cluster
                                :kafka-topic          topic-name
                                :kafka-consumer-group (unique-cg-name)
                                :timeout              200})]
        (p/set-offset! t nil)
        (with-redefs [kafka/poll-kafka-consumer
                      (fn [consumer opts]
                        (if (= 1 (swap! call-count inc))
                          (throw (proxy [ApplicationRecoverableException] ["simulated transient"]))
                          (real-poll consumer opts)))]
          (p/start t)
          (try
            (await-restart-count t 1 10000)
            (let [events (poll-n-events t 5 15000)]
              (is (= 1 (:restarts @(:state t)))
                  "restart counter should be 1")
              (is (= 5 (count events))
                  "all 5 events should arrive after restart")
              (is (= #{"k0" "k1" "k2" "k3" "k4"}
                     (set (map ::event/key events)))
                  "all 5 keys should be present")
              (deliver-all-cps! events))
            (finally
              (p/stop t))))))))

;; ---------------------------------------------------------------------------
;; KafkaConsumerGroupTopic — halt on InvalidConfigurationException
;; ---------------------------------------------------------------------------

(deftest cgt-halt-on-invalid-config-test
  (testing "InvalidConfigurationException during poll halts the supervisor with status :exception"
    (with-test-topic [topic-name]
      (let [t (p/init {:name                 :test-cgt-halt
                       :type                 :kafka-consumer-group-topic
                       :kafka-cluster        local-cluster
                       :kafka-topic          topic-name
                       :kafka-consumer-group (unique-cg-name)
                       :timeout              200})]
        (p/set-offset! t nil)
        (with-redefs [kafka/poll-kafka-consumer
                      (fn [& _]
                        (throw (InvalidConfigurationException. "simulated fatal config error")))]
          (p/start t)
          (try
            (await-topic-status t #{:exception} 10000)
            (is (= :exception (:status @(:state t)))
                "status should be :exception after fatal config error")
            (is (= 0 (:restarts @(:state t)))
                "no restart should have been attempted for a fatal exception")
            (finally
              (p/stop t))))))))

;; ---------------------------------------------------------------------------
;; KafkaConsumerGroupTopic — halt when restart budget exceeded
;; ---------------------------------------------------------------------------

(deftest cgt-halt-on-budget-exceeded-test
  (testing "supervisor halts when restart budget (50 restarts in 60s) is exceeded"
    (with-test-topic [topic-name]
      (let [t (p/init {:name                 :test-cgt-budget
                       :type                 :kafka-consumer-group-topic
                       :kafka-cluster        local-cluster
                       :kafka-topic          topic-name
                       :kafka-consumer-group (unique-cg-name)
                       :timeout              200})]
        ;; Pre-populate 50 recent restart timestamps to saturate the budget.
        (swap! (:state t)
               update :restart-timestamps
               into (repeat 50 (System/currentTimeMillis)))
        (p/set-offset! t nil)
        (with-redefs [kafka/poll-kafka-consumer
                      (fn [& _]
                        (throw (proxy [ApplicationRecoverableException] ["simulated"])))]
          (p/start t)
          (try
            (await-topic-status t #{:exception} 10000)
            (is (= :exception (:status @(:state t)))
                "status should be :exception once budget is exhausted")
            (finally
              (p/stop t))))))))

;; ---------------------------------------------------------------------------
;; KafkaReaderTopic — restart on recoverable exception
;; ---------------------------------------------------------------------------

(deftest reader-restart-on-recoverable-exception-test
  (testing "supervisor restarts once after ApplicationRecoverableException on first poll"
    (with-test-topic [topic-name]
      (produce-raw! topic-name [["rk0" "rv0"] ["rk1" "rv1"] ["rk2" "rv2"]
                                ["rk3" "rv3"] ["rk4" "rv4"]])
      (let [real-poll  kafka/poll-kafka-consumer
            call-count (atom 0)
            t          (p/init {:name          :test-reader-restart
                                :type          :kafka-reader-topic
                                :kafka-cluster local-cluster
                                :kafka-topic   topic-name
                                :timeout       200})]
        (p/set-offset! t 0)
        (with-redefs [kafka/poll-kafka-consumer
                      (fn [consumer opts]
                        (if (= 1 (swap! call-count inc))
                          (throw (proxy [ApplicationRecoverableException] ["simulated transient"]))
                          (real-poll consumer opts)))]
          (p/start t)
          (try
            (await-restart-count t 1 10000)
            (let [events (poll-n-events t 5 15000)]
              (is (= 1 (:restarts @(:state t)))
                  "restart counter should be 1")
              (is (= 5 (count events))
                  "all 5 events should arrive after restart")
              (is (every? ::event/skip-publish-effects events)
                  "all reader-topic events must have skip-publish-effects true")
              (is (= #{"rk0" "rk1" "rk2" "rk3" "rk4"}
                     (set (map ::event/key events)))
                  "all 5 keys should be present")
              (deliver-all-cps! events))
            (finally
              (p/stop t))))))))

;; ---------------------------------------------------------------------------
;; KafkaReaderTopic — halt on InvalidConfigurationException
;; ---------------------------------------------------------------------------

(deftest reader-halt-on-invalid-config-test
  (testing "InvalidConfigurationException during poll halts the supervisor with status :exception"
    (with-test-topic [topic-name]
      (let [t (p/init {:name          :test-reader-halt
                       :type          :kafka-reader-topic
                       :kafka-cluster local-cluster
                       :kafka-topic   topic-name
                       :timeout       200})]
        (p/set-offset! t 0)
        (with-redefs [kafka/poll-kafka-consumer
                      (fn [& _]
                        (throw (InvalidConfigurationException. "simulated fatal config error")))]
          (p/start t)
          (try
            (await-topic-status t #{:exception} 10000)
            (is (= :exception (:status @(:state t)))
                "status should be :exception after fatal config error")
            (is (= 0 (:restarts @(:state t)))
                "no restart should have been attempted for a fatal exception")
            (finally
              (p/stop t))))))))

;; ---------------------------------------------------------------------------
;; KafkaReaderTopic — post-restart resumes from last delivered offset
;; ---------------------------------------------------------------------------

(deftest reader-post-restart-resumes-from-last-offset-test
  (testing "after restart, reader seeks to last-delivered-event-offset, not initial offset"
    ;; Strategy:
    ;;  1. Produce 10 events (offsets 0-9).
    ;;  2. Start reader at offset 0.
    ;;  3. Consume 5 events via p/poll — this updates :last-delivered-event-offset in state.
    ;;  4. Trigger an ApplicationRecoverableException on the next Kafka poll.
    ;;  5. After restart, the reader should seek to last-delivered-event-offset, NOT offset 0.
    ;;     If it incorrectly restarted from 0, events with offsets < last-delivered-event-offset
    ;;     would appear in the queue, failing the assertion below.
    (with-test-topic [topic-name]
      (produce-raw! topic-name (mapv (fn [i] [(str "k" i) (str "v" i)]) (range 10)))
      (let [real-poll  kafka/poll-kafka-consumer
            throw-next (atom false)
            t          (p/init {:name          :test-reader-offset-resume
                                :type          :kafka-reader-topic
                                :kafka-cluster local-cluster
                                :kafka-topic   topic-name
                                :timeout       200})]
        (p/set-offset! t 0)
        (with-redefs [kafka/poll-kafka-consumer
                      (fn [consumer opts]
                        (if @throw-next
                          (do (reset! throw-next false)
                              (throw (proxy [ApplicationRecoverableException] ["simulated"])))
                          (real-poll consumer opts)))]
          (p/start t)
          (try
            ;; Step 3: consume 5 events via p/poll (updates :last-delivered-event-offset).
            (let [pre-events (loop [collected [] deadline (+ (System/currentTimeMillis) 10000)]
                               (if (or (= 5 (count collected))
                                       (> (System/currentTimeMillis) deadline))
                                 collected
                                 (if-let [ev (p/poll t)]
                                   (recur (conj collected ev) deadline)
                                   (recur collected deadline))))]
              (is (= 5 (count pre-events))
                  "should consume 5 events before triggering restart")
              (let [pre-restart-offset (:last-delivered-event-offset @(:state t))]
                (is (some? pre-restart-offset)
                    "last-delivered-event-offset must be set after p/poll calls")
                ;; Step 4: trigger the exception on the next Kafka poll.
                (reset! throw-next true)
                (await-restart-count t 1 10000)
                ;; Step 5: drain the queue after the restart settles.
                (Thread/sleep 500)
                (let [post-events (poll-n-events t 10 5000)]
                  (is (every? #(>= (::event/offset %) pre-restart-offset) post-events)
                      "post-restart events must not have offset < last-delivered-event-offset")
                  (deliver-all-cps! pre-events)
                  (deliver-all-cps! post-events))))
            (finally
              (p/stop t))))))))
