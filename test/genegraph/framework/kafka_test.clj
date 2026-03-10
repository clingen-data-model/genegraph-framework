(ns genegraph.framework.kafka-test
  (:require [genegraph.framework.kafka :as kafka]
            [genegraph.framework.event :as event]
            [clojure.test :refer [deftest testing is]])
  (:import [org.apache.kafka.clients.consumer ConsumerRecord]
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
  (testing "ApplicationRecoverableException maps to :restart-client"
    ;; ApplicationRecoverableException is abstract; proxy creates a concrete subclass.
    (let [e (proxy [ApplicationRecoverableException] ["test"])]
      (is (= :restart-client (kafka/exception-outcome e)))))

  (testing "a RetriableException subclass maps to :retry-action"
    ;; RetriableException is abstract; proxy creates a concrete subclass.
    (let [e (proxy [RetriableException] ["test"])]
      (is (= :retry-action (kafka/exception-outcome e)))))

  (testing "InvalidConfigurationException maps to :halt-application"
    (let [e (InvalidConfigurationException. "test")]
      (is (= :halt-application (kafka/exception-outcome e)))))

  (testing "an unrecognized exception type maps to :unknown-outcome"
    (let [e (RuntimeException. "unknown")]
      (is (= :unknown-outcome (kafka/exception-outcome e)))))

  (testing "ApplicationRecoverableException is checked before RetriableException"
    ;; ApplicationRecoverableException extends RetriableException; condp instance?
    ;; checks in order so ApplicationRecoverableException must come first.
    (let [e (proxy [ApplicationRecoverableException] ["overlapping"])]
      (is (= :restart-client (kafka/exception-outcome e))))))

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
