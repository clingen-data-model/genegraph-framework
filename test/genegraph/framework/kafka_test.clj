(ns genegraph.framework.kafka-test
  (:require [genegraph.framework.kafka :as kafka]
            [genegraph.framework.event :as event]
            [genegraph.framework.protocol :as p]
            [genegraph.framework.topic]   ; loads :simple-queue-topic defmethod
            [genegraph.framework.kafka]   ; loads :kafka-consumer-group-topic and :kafka-reader-topic defmethods
            [clojure.test :refer [deftest testing is]])
  (:import [org.apache.kafka.clients.consumer ConsumerRecord KafkaConsumer]
           [org.apache.kafka.clients.producer ProducerRecord]
           [org.apache.kafka.common.errors
            ApplicationRecoverableException
            RetriableException
            InvalidConfigurationException]))

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn make-consumer-record
  "Construct a ConsumerRecord with the given fields.
  Uses the 5-arg constructor; timestamp will be ConsumerRecord/NO_TIMESTAMP (-1)."
  [topic partition offset k v]
  (ConsumerRecord. topic (int partition) (long offset) k v))

(defn make-topic-with-state
  "Build a minimal topic-like map for testing offset-related pure functions.
  All promise-valued state keys are pre-delivered with the given values."
  [{:keys [local-offset cg-offset end-offset last-completed-offset]}]
  (let [local-p (doto (promise) (deliver local-offset))
        cg-p    (doto (promise) (deliver cg-offset))
        end-p   (doto (promise) (deliver end-offset))]
    {:state (atom {:last-completed-offset        last-completed-offset
                   :initial-local-offset         local-p
                   :initial-consumer-group-offset cg-p
                   :end-offset-at-start          end-p})}))

;; ---------------------------------------------------------------------------
;; consumer-record->event
;; ---------------------------------------------------------------------------

(deftest consumer-record->event-test
  (testing "maps ConsumerRecord fields to the correct ::event/ keys"
    (let [record (make-consumer-record "my-topic" 0 42 "my-key" "my-value")
          ev     (kafka/consumer-record->event record)]
      (is (= "my-key"   (::event/key ev)))
      (is (= "my-value" (::event/value ev)))
      (is (= "my-topic" (::event/kafka-topic ev)))
      (is (= 0          (::event/partition ev)))
      (is (= 42         (::event/offset ev)))
      (is (= :kafka     (::event/source ev)))))

  (testing "timestamp is numeric (ConsumerRecord 5-arg gives NO_TIMESTAMP = -1)"
    (let [ev (kafka/consumer-record->event (make-consumer-record "t" 0 0 "k" "v"))]
      (is (integer? (::event/timestamp ev)))))

  (testing "includes a fresh ::event/completion-promise (pending, not yet delivered)"
    (let [ev (kafka/consumer-record->event (make-consumer-record "t" 0 0 "k" "v"))]
      (is (instance? clojure.lang.IPending (::event/completion-promise ev)))
      (is (not (realized? (::event/completion-promise ev))))))

  (testing "includes a fresh ::event/execution-thread promise (pending)"
    (let [ev (kafka/consumer-record->event (make-consumer-record "t" 0 0 "k" "v"))]
      (is (instance? clojure.lang.IPending (::event/execution-thread ev)))
      (is (not (realized? (::event/execution-thread ev))))))

  (testing "each call produces independent promise objects"
    (let [record (make-consumer-record "t" 0 0 "k" "v")
          ev1    (kafka/consumer-record->event record)
          ev2    (kafka/consumer-record->event record)]
      (is (not (identical? (::event/completion-promise ev1)
                           (::event/completion-promise ev2))))))

  (testing "partition, offset, and key/value are preserved for non-zero values"
    (let [ev (kafka/consumer-record->event
              (make-consumer-record "other-topic" 2 999 "k2" "v2"))]
      (is (= "other-topic" (::event/kafka-topic ev)))
      (is (= 2   (::event/partition ev)))
      (is (= 999 (::event/offset ev)))
      (is (= "k2" (::event/key ev)))
      (is (= "v2" (::event/value ev))))))

;; ---------------------------------------------------------------------------
;; event->producer-record
;; ---------------------------------------------------------------------------

(deftest event->producer-record-test
  (testing "maps ::event/ keys to the correct ProducerRecord fields"
    (let [ev  {::event/kafka-topic "out-topic"
               ::event/key        "k1"
               ::event/value      "v1"
               ::event/partition  1
               ::event/timestamp  99999}
          rec (kafka/event->producer-record ev)]
      (is (= "out-topic" (.topic rec)))
      (is (= 1           (.partition rec)))
      (is (= 99999       (.timestamp rec)))
      (is (= "k1"        (.key rec)))
      (is (= "v1"        (.value rec)))))

  (testing "partition defaults to 0 when not supplied"
    (let [ev  {::event/kafka-topic "t"
               ::event/key        "k"
               ::event/value      "v"
               ::event/timestamp  1000}
          rec (kafka/event->producer-record ev)]
      (is (= 0 (.partition rec)))))

  (testing "timestamp defaults to a recent epoch millis when not supplied"
    (let [before (System/currentTimeMillis)
          rec    (kafka/event->producer-record {::event/kafka-topic "t"
                                               ::event/key        "k"
                                               ::event/value      "v"})
          after  (System/currentTimeMillis)]
      (is (<= before (.timestamp rec) after))))

  (testing "returns a ProducerRecord instance"
    (let [rec (kafka/event->producer-record {::event/kafka-topic "t"
                                             ::event/key        "k"
                                             ::event/value      "v"
                                             ::event/partition  0
                                             ::event/timestamp  1})]
      (is (instance? ProducerRecord rec)))))

;; ---------------------------------------------------------------------------
;; exception-outcome
;; ---------------------------------------------------------------------------

(deftest exception-outcome-test
  (testing "ApplicationRecoverableException maps to :restart-client with :warn severity"
    ;; ApplicationRecoverableException is abstract; proxy creates a concrete subclass.
    (let [e (proxy [ApplicationRecoverableException] ["test"])]
      (is (= {:outcome :restart-client :severity :warn} (kafka/exception-outcome e)))))

  (testing "a RetriableException subclass maps to :retry-action with :info severity"
    ;; RetriableException is abstract; proxy creates a concrete subclass.
    (let [e (proxy [RetriableException] ["test"])]
      (is (= {:outcome :retry-action :severity :info} (kafka/exception-outcome e)))))

  (testing "InvalidConfigurationException maps to :halt-application with :fatal severity"
    (let [e (InvalidConfigurationException. "test")]
      (is (= {:outcome :halt-application :severity :fatal} (kafka/exception-outcome e)))))

  (testing "an unrecognized exception type maps to :unknown with :error severity"
    (let [e (RuntimeException. "unknown")]
      (is (= {:outcome :unknown :severity :error} (kafka/exception-outcome e)))))

  (testing "ApplicationRecoverableException is checked before RetriableException"
    ;; ApplicationRecoverableException extends RetriableException; condp instance?
    ;; checks in order so ApplicationRecoverableException must come first.
    (let [e (proxy [ApplicationRecoverableException] ["overlapping"])]
      (is (= {:outcome :restart-client :severity :warn} (kafka/exception-outcome e))))))

;; ---------------------------------------------------------------------------
;; topic-up-to-date?
;; ---------------------------------------------------------------------------

(deftest topic-up-to-date-test
  (testing "true when last-completed-offset has caught up to end-offset-at-start"
    (let [topic (make-topic-with-state {:local-offset          nil
                                        :cg-offset             nil
                                        :end-offset            10
                                        :last-completed-offset 10})]
      (is (true? (kafka/topic-up-to-date? topic)))))

  (testing "true when last-completed-offset exceeds end-offset-at-start"
    (let [topic (make-topic-with-state {:local-offset          nil
                                        :cg-offset             nil
                                        :end-offset            10
                                        :last-completed-offset 15})]
      (is (true? (kafka/topic-up-to-date? topic)))))

  (testing "false (first branch) when last-completed-offset is behind end-offset; falls to second branch"
    ;; With no local or cg offsets, second branch is true even if last-completed lags.
    ;; This case still returns true because neither local nor cg offset is set.
    (let [topic (make-topic-with-state {:local-offset          nil
                                        :cg-offset             nil
                                        :end-offset            10
                                        :last-completed-offset 5})]
      (is (true? (kafka/topic-up-to-date? topic)))))

  (testing "true when both local-offset and cg-offset are at or past end-offset"
    (let [topic (make-topic-with-state {:local-offset          10
                                        :cg-offset             10
                                        :end-offset            10
                                        :last-completed-offset nil})]
      (is (true? (kafka/topic-up-to-date? topic)))))

  (testing "true when both offsets exceed end-offset"
    (let [topic (make-topic-with-state {:local-offset          20
                                        :cg-offset             15
                                        :end-offset            10
                                        :last-completed-offset nil})]
      (is (true? (kafka/topic-up-to-date? topic)))))

  (testing "false when local-offset is behind end-offset"
    (let [topic (make-topic-with-state {:local-offset          5
                                        :cg-offset             nil
                                        :end-offset            10
                                        :last-completed-offset nil})]
      (is (false? (kafka/topic-up-to-date? topic)))))

  (testing "false when cg-offset is behind end-offset"
    (let [topic (make-topic-with-state {:local-offset          nil
                                        :cg-offset             5
                                        :end-offset            10
                                        :last-completed-offset nil})]
      (is (false? (kafka/topic-up-to-date? topic)))))

  (testing "false when both local and cg offsets are behind end-offset"
    (let [topic (make-topic-with-state {:local-offset          3
                                        :cg-offset             4
                                        :end-offset            10
                                        :last-completed-offset nil})]
      (is (false? (kafka/topic-up-to-date? topic)))))

  (testing "true when neither local-offset nor cg-offset is set (no lag to track)"
    (let [topic (make-topic-with-state {:local-offset          nil
                                        :cg-offset             nil
                                        :end-offset            10
                                        :last-completed-offset nil})]
      (is (true? (kafka/topic-up-to-date? topic))))))

;; ---------------------------------------------------------------------------
;; backing-store-lags-consumer-group?
;; ---------------------------------------------------------------------------

(deftest backing-store-lags-consumer-group-test
  (testing "true when local-offset < cg-offset (backing store is behind)"
    (let [topic (make-topic-with-state {:local-offset 5
                                        :cg-offset    10
                                        :end-offset   10})]
      (is (true? (kafka/backing-store-lags-consumer-group? topic)))))

  (testing "false when local-offset equals cg-offset (no lag)"
    (let [topic (make-topic-with-state {:local-offset 10
                                        :cg-offset    10
                                        :end-offset   10})]
      (is (false? (kafka/backing-store-lags-consumer-group? topic)))))

  (testing "false when local-offset > cg-offset (backing store is ahead)"
    (let [topic (make-topic-with-state {:local-offset 15
                                        :cg-offset    10
                                        :end-offset   10})]
      (is (false? (kafka/backing-store-lags-consumer-group? topic)))))

  (testing "false when local-offset is nil (not a number — :else branch)"
    (let [topic (make-topic-with-state {:local-offset nil
                                        :cg-offset    10
                                        :end-offset   10})]
      (is (false? (kafka/backing-store-lags-consumer-group? topic)))))

  (testing "false when cg-offset is nil (not a number — :else branch)"
    (let [topic (make-topic-with-state {:local-offset 5
                                        :cg-offset    nil
                                        :end-offset   10})]
      (is (false? (kafka/backing-store-lags-consumer-group? topic)))))

  (testing "false when both offsets are nil"
    (let [topic (make-topic-with-state {:local-offset nil
                                        :cg-offset    nil
                                        :end-offset   10})]
      (is (false? (kafka/backing-store-lags-consumer-group? topic)))))

  (testing ":timeout when local-offset promise is not delivered within 100ms"
    ;; The function deref's with a 100ms timeout; an undelivered promise yields :timeout.
    (let [local-p (promise)   ; never delivered
          cg-p    (doto (promise) (deliver 10))
          topic   {:state (atom {:initial-local-offset         local-p
                                 :initial-consumer-group-offset cg-p})}]
      (is (= :timeout (kafka/backing-store-lags-consumer-group? topic)))))

  (testing ":timeout when cg-offset promise is not delivered within 100ms"
    (let [local-p (doto (promise) (deliver 5))
          cg-p    (promise)   ; never delivered
          topic   {:state (atom {:initial-local-offset         local-p
                                 :initial-consumer-group-offset cg-p})}]
      (is (= :timeout (kafka/backing-store-lags-consumer-group? topic))))))

;; ===========================================================================
;; Restart helpers shared across sections 1–5
;; ===========================================================================

(defn- now-ms [] (System/currentTimeMillis))

(defn- wait-for
  "Poll pred every 25 ms. Returns true as soon as pred is truthy, false on timeout."
  [pred timeout-ms]
  (let [deadline (+ (now-ms) timeout-ms)]
    (loop []
      (or (pred)
          (if (> (now-ms) deadline)
            false
            (do (Thread/sleep 25) (recur)))))))

;; ---------------------------------------------------------------------------
;; Mock KafkaConsumer: no-op subscribe / unsubscribe / close; real constructor
;; so direct Java interop in the supervisor loop doesn't throw.
;; ---------------------------------------------------------------------------

(def ^:private mock-consumer-props
  {"bootstrap.servers"  "localhost:9092"
   "group.id"           "test-group"
   "key.deserializer"   "org.apache.kafka.common.serialization.StringDeserializer"
   "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"})

(defn- make-mock-consumer []
  (proxy [KafkaConsumer] [mock-consumer-props]
    (subscribe
      ([topics]          nil)
      ([topics listener] nil))
    (unsubscribe [] nil)
    (close [] nil)))

;; ---------------------------------------------------------------------------
;; Topic builders for supervisor loop integration tests
;; ---------------------------------------------------------------------------

(defn- make-cg-topic []
  (p/init {:name                 :test-cg
           :type                 :kafka-consumer-group-topic
           :kafka-cluster        {:common-config {} :consumer-config {}}
           :kafka-consumer-group "test-group"
           :kafka-topic          "test-topic"
           :timeout              100}))

(defn- make-reader-topic []
  (p/init {:name          :test-reader
           :type          :kafka-reader-topic
           :kafka-cluster {:common-config {} :consumer-config {}}
           :kafka-topic   "test-topic"
           :timeout       100}))

(defn- deliver-cg-offsets!
  "Pre-deliver offset promises so that the up-to-date virtual thread exits promptly."
  [topic]
  (let [s @(:state topic)]
    (deliver (:initial-local-offset s) nil)
    (deliver (:initial-consumer-group-offset s) nil)
    (deliver (:end-offset-at-start s) 0)))

(defn- deliver-reader-offsets!
  "Pre-deliver initial-local-offset so the reader start thread doesn't block on deref."
  [topic]
  (deliver (:initial-local-offset @(:state topic)) 0))

;; ===========================================================================
;; Section 1 — restart-budget-exceeded?
;; ===========================================================================

(deftest restart-budget-exceeded-test
  (let [now   (now-ms)
        recent now
        old   (- now (* 61 1000))]

    (testing "false when restart-timestamps is empty"
      (is (false? (kafka/restart-budget-exceeded? {:restart-timestamps []}))))

    (testing "false when all timestamps are older than 60 seconds"
      (is (false? (kafka/restart-budget-exceeded?
                   {:restart-timestamps (vec (repeat 10 old))}))))

    (testing "false when fewer than 50 recent restarts"
      (is (false? (kafka/restart-budget-exceeded?
                   {:restart-timestamps (vec (repeat 49 recent))}))))

    (testing "true when exactly 50 recent restarts"
      (is (true? (kafka/restart-budget-exceeded?
                  {:restart-timestamps (vec (repeat 50 recent))}))))

    (testing "true when more than 50 recent restarts"
      (is (true? (kafka/restart-budget-exceeded?
                  {:restart-timestamps (vec (repeat 51 recent))}))))

    (testing "false when 49 recent and many old timestamps"
      (is (false? (kafka/restart-budget-exceeded?
                   {:restart-timestamps (concat (repeat 49 recent)
                                                (repeat 10 old))}))))))

;; ===========================================================================
;; Section 2 — consumer-should-continue?
;; ===========================================================================

(defn- make-status-topic [status]
  {:state (atom {:status status})})

(deftest consumer-should-continue-test
  (testing ":running → true"
    (is (true? (kafka/consumer-should-continue? (make-status-topic :running)))))

  (testing ":restarting → true"
    (is (true? (kafka/consumer-should-continue? (make-status-topic :restarting)))))

  (testing ":stopping → true"
    (is (true? (kafka/consumer-should-continue? (make-status-topic :stopping)))))

  (testing ":stopped → false"
    (is (false? (kafka/consumer-should-continue? (make-status-topic :stopped)))))

  (testing ":failed → false"
    (is (false? (kafka/consumer-should-continue? (make-status-topic :failed)))))

  (testing ":exception → false (halt path must terminate the loop)"
    (is (false? (kafka/consumer-should-continue? (make-status-topic :exception))))))

;; ===========================================================================
;; Section 3 — consumer-restart-action
;; ===========================================================================

(defn- make-topic-with-restart-timestamps [timestamps]
  {:state (atom (assoc (kafka/initial-state) :restart-timestamps timestamps))})

(deftest consumer-restart-action-test
  (let [now         (now-ms)
        budget-full (vec (repeat 50 now))
        budget-ok   []
        retriable   (proxy [RetriableException] ["test"])
        recoverable (proxy [ApplicationRecoverableException] ["test"])
        halting     (InvalidConfigurationException. "test")
        unknown     (RuntimeException. "test")]

    (testing "RetriableException → :retry regardless of restart budget"
      (is (= :retry (kafka/consumer-restart-action
                     (make-topic-with-restart-timestamps budget-ok) retriable)))
      (is (= :retry (kafka/consumer-restart-action
                     (make-topic-with-restart-timestamps budget-full) retriable))))

    (testing "InvalidConfigurationException → :halt regardless of restart budget"
      (is (= :halt (kafka/consumer-restart-action
                    (make-topic-with-restart-timestamps budget-ok) halting)))
      (is (= :halt (kafka/consumer-restart-action
                    (make-topic-with-restart-timestamps budget-full) halting))))

    (testing "ApplicationRecoverableException → :restart when budget not exceeded"
      (is (= :restart (kafka/consumer-restart-action
                       (make-topic-with-restart-timestamps budget-ok) recoverable))))

    (testing "ApplicationRecoverableException → :halt when budget exceeded"
      (is (= :halt (kafka/consumer-restart-action
                    (make-topic-with-restart-timestamps budget-full) recoverable))))

    (testing "unknown exception → :restart when budget not exceeded"
      (is (= :restart (kafka/consumer-restart-action
                       (make-topic-with-restart-timestamps budget-ok) unknown))))

    (testing "unknown exception → :halt when budget exceeded"
      (is (= :halt (kafka/consumer-restart-action
                    (make-topic-with-restart-timestamps budget-full) unknown))))))

;; ===========================================================================
;; Section 4 — attempt-consumer-restart!
;; Note: each call incurs ~500 ms backoff (restarts=0, base=500ms).
;; Tests are consolidated to minimise total wait time.
;; ===========================================================================

(defn- make-restartable-topic [& {:as extra-state}]
  {:name  :test-restartable
   :state (atom (merge (kafka/initial-state) extra-state))})

(deftest attempt-consumer-restart-state-test
  (testing "happy path: state is updated correctly"
    (let [before (now-ms)
          topic  (make-restartable-topic)]
      (kafka/attempt-consumer-restart! topic (constantly :mock-consumer))
      (let [s @(:state topic)]
        (is (= 1 (:restarts s)))
        (is (= :mock-consumer (:kafka-consumer s)))
        (is (= :running (:status s)))
        (is (some? (first (:restart-timestamps s))))
        (is (>= (first (:restart-timestamps s)) before)))))

  (testing "nil existing consumer — close failure is swallowed, restart still completes"
    (let [topic (make-restartable-topic :kafka-consumer nil)]
      (is (some? (kafka/attempt-consumer-restart! topic (constantly :ok))))))

  (testing "exception from factory fn propagates to caller"
    (let [topic (make-restartable-topic)]
      (is (thrown? RuntimeException
                   (kafka/attempt-consumer-restart!
                    topic #(throw (RuntimeException. "factory error")))))))

  (testing "publishes :kafka-consumer-restart event to system-topic"
    (let [sys-topic (p/init {:name :sys :type :simple-queue-topic :timeout 100})
          topic     (assoc (make-restartable-topic) :system-topic sys-topic)]
      (kafka/attempt-consumer-restart! topic (constantly :ok))
      (let [ev (p/poll sys-topic)]
        (is (some? ev))
        (is (= :kafka-consumer-restart (get-in ev [::event/data :type])))
        (is (= 1 (get-in ev [::event/data :restarts])))))))

;; ===========================================================================
;; Section 5 — Supervisor loop integration tests
;; Uses with-redefs to inject behaviour without a real Kafka broker.
;; ===========================================================================

;; ---------------------------------------------------------------------------
;; KafkaConsumerGroupTopic
;; ---------------------------------------------------------------------------

(deftest cg-topic-clean-stop-test
  (testing "supervisor loop exits cleanly on p/stop — no restarts"
    (let [topic (make-cg-topic)]
      (deliver-cg-offsets! topic)
      (with-redefs [kafka/create-kafka-consumer (fn [_] (make-mock-consumer))
                    kafka/update-local-storage! (fn [_] [])
                    kafka/poll-kafka-consumer   (fn [_ _] [])]
        (p/start topic)
        (Thread/sleep 100)
        (p/stop topic)
        (is (wait-for #(= :stopped (:status @(:state topic))) 2000))
        (is (= 0 (:restarts @(:state topic))))))))

(deftest cg-topic-restart-on-recoverable-exception-test
  (testing "supervisor loop restarts consumer on ApplicationRecoverableException"
    (let [topic      (make-cg-topic)
          poll-count (atom 0)]
      (deliver-cg-offsets! topic)
      (with-redefs [kafka/create-kafka-consumer (fn [_] (make-mock-consumer))
                    kafka/update-local-storage! (fn [_] [])
                    kafka/poll-kafka-consumer   (fn [_ _]
                                                  (when (= 1 (swap! poll-count inc))
                                                    (throw (proxy [ApplicationRecoverableException]
                                                                  ["transient failure"])))
                                                  [])]
        (p/start topic)
        (is (wait-for #(= 1 (:restarts @(:state topic))) 3000)
            "restart count should reach 1 after the recoverable exception")
        (p/stop topic)))))

(deftest cg-topic-halt-on-invalid-config-test
  (testing "supervisor loop halts with :exception status on InvalidConfigurationException"
    (let [topic (make-cg-topic)]
      (deliver-cg-offsets! topic)
      (with-redefs [kafka/create-kafka-consumer (fn [_] (make-mock-consumer))
                    kafka/update-local-storage! (fn [_] [])
                    kafka/poll-kafka-consumer   (fn [_ _]
                                                  (throw (InvalidConfigurationException. "bad config")))]
        (p/start topic)
        (is (wait-for #(= :exception (:status @(:state topic))) 2000))))))

(deftest cg-topic-halt-on-budget-exceeded-test
  (testing "supervisor loop halts immediately when restart budget is already exceeded"
    (let [topic (make-cg-topic)
          now   (now-ms)]
      ;; Pre-populate 50 recent restart timestamps to exhaust the budget upfront.
      (swap! (:state topic) assoc :restart-timestamps (vec (repeat 50 now)))
      (deliver-cg-offsets! topic)
      (with-redefs [kafka/create-kafka-consumer (fn [_] (make-mock-consumer))
                    kafka/update-local-storage! (fn [_] [])
                    kafka/poll-kafka-consumer   (fn [_ _]
                                                  (throw (proxy [ApplicationRecoverableException]
                                                                ["transient"])))]
        (p/start topic)
        ;; Budget already exceeded → first throw → :halt immediately (no backoff sleep).
        (is (wait-for #(= :exception (:status @(:state topic))) 2000))
        ;; Restart count must stay at 50 — no new restart was attempted.
        (is (= 50 (count (:restart-timestamps @(:state topic)))))))))

;; ---------------------------------------------------------------------------
;; KafkaReaderTopic
;; ---------------------------------------------------------------------------

(deftest reader-topic-clean-stop-test
  (testing "reader supervisor loop exits cleanly on p/stop — no restarts"
    (let [topic (make-reader-topic)]
      (deliver-reader-offsets! topic)
      (with-redefs [kafka/create-local-kafka-consumer (fn [_ _] (make-mock-consumer))
                    kafka/end-offset                  (fn [_] 0)
                    kafka/poll-kafka-consumer         (fn [_ _] [])]
        (p/start topic)
        (Thread/sleep 100)
        (p/stop topic)
        (is (wait-for #(= :stopped (:status @(:state topic))) 2000))
        (is (= 0 (:restarts @(:state topic))))))))

(deftest reader-topic-restart-on-recoverable-exception-test
  (testing "reader supervisor loop restarts consumer on ApplicationRecoverableException"
    (let [topic      (make-reader-topic)
          poll-count (atom 0)]
      (deliver-reader-offsets! topic)
      (with-redefs [kafka/create-local-kafka-consumer (fn [_ _] (make-mock-consumer))
                    kafka/end-offset                  (fn [_] 0)
                    kafka/poll-kafka-consumer         (fn [_ _]
                                                        (when (= 1 (swap! poll-count inc))
                                                          (throw (proxy [ApplicationRecoverableException]
                                                                        ["transient failure"])))
                                                        [])]
        (p/start topic)
        (is (wait-for #(= 1 (:restarts @(:state topic))) 3000)
            "restart count should reach 1 after the recoverable exception")
        (p/stop topic)))))

(deftest reader-topic-restart-uses-last-delivered-offset-test
  (testing "reader creates new consumer from :last-delivered-event-offset after restart"
    (let [topic        (make-reader-topic)
          create-calls (atom [])
          poll-count   (atom 0)]
      ;; Deliver initial-local-offset = 0, then override last-delivered-event-offset = 42.
      ;; The restart should use 42 (not 0) because last-delivered takes priority.
      (deliver-reader-offsets! topic)
      (swap! (:state topic) assoc :last-delivered-event-offset 42)
      (with-redefs [kafka/create-local-kafka-consumer (fn [_ offset]
                                                        (swap! create-calls conj offset)
                                                        (make-mock-consumer))
                    kafka/end-offset                  (fn [_] 0)
                    kafka/poll-kafka-consumer         (fn [_ _]
                                                        (when (= 1 (swap! poll-count inc))
                                                          (throw (proxy [ApplicationRecoverableException]
                                                                        ["transient"])))
                                                        [])]
        (p/start topic)
        ;; Wait for initial create + restart create (at least 2 calls).
        (is (wait-for #(>= (count @create-calls) 2) 3000))
        (p/stop topic)
        ;; Both calls should use last-delivered-event-offset (42), not initial-local-offset (0).
        (is (every? #(= 42 %) @create-calls)
            "all create-local-kafka-consumer calls should use last-delivered-event-offset")))))
