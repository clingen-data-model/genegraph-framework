(ns genegraph.framework.topic-test
  (:require [genegraph.framework.protocol :as p]
            [genegraph.framework.event :as event]
            [genegraph.framework.topic]           ; loads defmethod for :simple-queue-topic
            [clojure.test :refer [deftest testing is]]))

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn make-topic
  "Initialize a SimpleQueueTopic. Overrides the default 1-second timeout with
  100ms so that poll-on-empty tests finish quickly."
  [& {:as opts}]
  (p/init (merge {:name        :test-topic
                  :type        :simple-queue-topic
                  :timeout     100}
                 opts)))

;; ---------------------------------------------------------------------------
;; Publish / poll round-trip
;; ---------------------------------------------------------------------------

(deftest publish-poll-roundtrip-test
  (testing "polled event contains the original event data"
    (let [topic (make-topic)
          event {::event/key "k1" ::event/data {:x 1}}]
      (p/publish topic event)
      (let [result (p/poll topic)]
        (is (= "k1" (::event/key result)))
        (is (= {:x 1} (::event/data result))))))

  (testing "polled event has ::event/topic set to the topic's :name"
    (let [topic (make-topic :name :my-named-topic)]
      (p/publish topic {::event/data {}})
      (is (= :my-named-topic (::event/topic (p/poll topic))))))

  (testing "::event/topic is overwritten with topic name even if event had a different value"
    (let [topic (make-topic :name :real-name)]
      (p/publish topic {::event/topic :original-name})
      (is (= :real-name (::event/topic (p/poll topic))))))

  (testing "multiple events are returned in FIFO order"
    (let [topic (make-topic)]
      (p/publish topic {::event/key "first"})
      (p/publish topic {::event/key "second"})
      (p/publish topic {::event/key "third"})
      (is (= "first"  (::event/key (p/poll topic))))
      (is (= "second" (::event/key (p/poll topic))))
      (is (= "third"  (::event/key (p/poll topic))))))

  (testing "all original event keys are preserved on poll"
    (let [topic (make-topic)
          event {::event/key "k" ::event/data {:a 1} ::event/offset 5}]
      (p/publish topic event)
      (let [result (p/poll topic)]
        (is (= 5 (::event/offset result)))
        (is (= {:a 1} (::event/data result))))))

  (testing "poll on an empty queue returns nil"
    (let [topic (make-topic :timeout 50)]
      (is (nil? (p/poll topic))))))

;; ---------------------------------------------------------------------------
;; Completion promise injection
;; ---------------------------------------------------------------------------

(deftest completion-promise-injection-test
  (testing "events without ::event/completion-promise receive one on poll"
    (let [topic (make-topic)]
      (p/publish topic {::event/data {}})
      (let [result (p/poll topic)]
        (is (contains? result ::event/completion-promise))
        (is (instance? clojure.lang.IPending (::event/completion-promise result))))))

  (testing "each polled event gets its own fresh promise when none was provided"
    (let [topic (make-topic)]
      (p/publish topic {})
      (p/publish topic {})
      (let [r1 (p/poll topic)
            r2 (p/poll topic)]
        (is (not (identical? (::event/completion-promise r1)
                             (::event/completion-promise r2)))))))

  (testing "events that already carry ::event/completion-promise keep their existing promise"
    (let [topic (make-topic)
          cp    (promise)]
      (p/publish topic {::event/completion-promise cp})
      (let [result (p/poll topic)]
        (is (identical? cp (::event/completion-promise result))))))

  (testing "an existing ::event/completion-promise is not replaced by a new one"
    (let [topic (make-topic)
          cp    (promise)]
      (p/publish topic {::event/completion-promise cp})
      (let [result (p/poll topic)]
        ;; Delivering the promise from the result should work (it's the same object)
        (deliver (::event/completion-promise result) :done)
        (is (realized? cp))
        (is (= :done (deref cp 0 :not-delivered)))))))

;; ---------------------------------------------------------------------------
;; :commit-promise delivery on publish
;; ---------------------------------------------------------------------------

(deftest commit-promise-test
  (testing "publish delivers :commit-promise with true when the event carries one"
    (let [topic (make-topic)
          cp    (promise)]
      (p/publish topic {:commit-promise cp})
      (is (= true (deref cp 500 :timeout)))))

  (testing ":commit-promise is delivered before the event enters the queue"
    ;; Even with a full queue (offer would time out), the promise is still delivered
    ;; because delivery happens before .offer in the publish implementation.
    (let [topic (make-topic :buffer-size 1 :timeout 50)
          cp1   (promise)
          cp2   (promise)]
      (p/publish topic {:commit-promise cp1 :id 1}) ; fills the queue
      (p/publish topic {:commit-promise cp2 :id 2}) ; offer times out but promise still fires
      (is (= true (deref cp1 500 :timeout)))
      (is (= true (deref cp2 500 :timeout)))))

  (testing "publish without :commit-promise does not throw"
    (let [topic (make-topic)]
      (is (some? (p/publish topic {::event/data {:no :promise}}))))))

;; ---------------------------------------------------------------------------
;; Capacity and backpressure
;; ---------------------------------------------------------------------------

(deftest backpressure-test
  (testing "publish to an empty queue returns true"
    (let [topic (make-topic :buffer-size 1 :timeout 50)]
      (is (true? (p/publish topic {:id 1})))))

  (testing "publish to a full queue returns false after the offer timeout"
    (let [topic (make-topic :buffer-size 1 :timeout 50)]
      (p/publish topic {:id 1})          ; fills the single slot
      (is (false? (p/publish topic {:id 2})))))  ; offer times out

  (testing "after polling an event from a full queue, the next publish succeeds"
    (let [topic (make-topic :buffer-size 1 :timeout 50)]
      (p/publish topic {:id 1})
      (p/poll topic)                    ; free up the slot
      (is (true? (p/publish topic {:id 2})))))

  (testing "publish returns true for each event up to buffer-size"
    (let [n     4
          topic (make-topic :buffer-size n :timeout 50)]
      (is (every? true? (repeatedly n #(p/publish topic {}))))))

  (testing "publish returns false once the buffer is exhausted"
    (let [n     3
          topic (make-topic :buffer-size n :timeout 50)]
      (dotimes [_ n] (p/publish topic {}))           ; fill it up
      (is (false? (p/publish topic {}))))))           ; one too many

;; ---------------------------------------------------------------------------
;; Status
;; ---------------------------------------------------------------------------

(deftest status-test
  (testing "status includes the topic :name"
    (let [topic (make-topic :name :status-topic)]
      (is (= :status-topic (:name (p/status topic))))))

  (testing "empty topic reports :queue-size 0"
    (let [topic (make-topic :buffer-size 5)]
      (is (= 0 (:queue-size (p/status topic))))))

  (testing "empty topic reports :remaining-capacity equal to buffer-size"
    (let [topic (make-topic :buffer-size 5)]
      (is (= 5 (:remaining-capacity (p/status topic))))))

  (testing ":queue-size increases after each publish"
    (let [topic (make-topic :buffer-size 5)]
      (p/publish topic {:id 1})
      (is (= 1 (:queue-size (p/status topic))))
      (p/publish topic {:id 2})
      (is (= 2 (:queue-size (p/status topic))))))

  (testing ":remaining-capacity decreases after each publish"
    (let [topic (make-topic :buffer-size 3)]
      (p/publish topic {})
      (p/publish topic {})
      (is (= 1 (:remaining-capacity (p/status topic))))))

  (testing ":queue-size + :remaining-capacity always equals buffer-size"
    (let [topic (make-topic :buffer-size 4)]
      (p/publish topic {})
      (p/publish topic {})
      (let [s (p/status topic)]
        (is (= 4 (+ (:queue-size s) (:remaining-capacity s)))))))

  (testing ":queue-size decreases and :remaining-capacity increases after poll"
    (let [topic (make-topic :buffer-size 3)]
      (p/publish topic {:id 1})
      (p/publish topic {:id 2})
      (p/poll topic)
      (let [s (p/status topic)]
        (is (= 1 (:queue-size s)))
        (is (= 2 (:remaining-capacity s))))))

  (testing "status on a full queue shows 0 remaining-capacity"
    (let [topic (make-topic :buffer-size 2 :timeout 50)]
      (p/publish topic {:id 1})
      (p/publish topic {:id 2})
      (let [s (p/status topic)]
        (is (= 2 (:queue-size s)))
        (is (= 0 (:remaining-capacity s)))))))

;; ---------------------------------------------------------------------------
;; p/init — topic initialization
;; ---------------------------------------------------------------------------

(deftest init-test
  (testing "p/init with :type :simple-queue-topic returns a SimpleQueueTopic"
    (let [topic (p/init {:name :t :type :simple-queue-topic})]
      (is (instance? genegraph.framework.topic.SimpleQueueTopic topic))))

  (testing "p/init applies default timeout of 1000ms when not specified"
    (let [topic (p/init {:name :t :type :simple-queue-topic})]
      (is (= 1000 (:timeout topic)))))

  (testing "p/init applies default buffer-size of 10 when not specified"
    (let [topic (p/init {:name :t :type :simple-queue-topic})]
      (is (= 10 (.remainingCapacity (:queue topic))))))

  (testing "p/init respects an explicit :timeout"
    (let [topic (p/init {:name :t :type :simple-queue-topic :timeout 250})]
      (is (= 250 (:timeout topic)))))

  (testing "p/init respects an explicit :buffer-size"
    (let [topic (p/init {:name :t :type :simple-queue-topic :buffer-size 7})]
      (is (= 7 (.remainingCapacity (:queue topic))))))

  (testing "initialized topic satisfies Consumer and Publisher protocols"
    (let [topic (make-topic)]
      (is (satisfies? p/Consumer topic))
      (is (satisfies? p/Publisher topic))
      (is (satisfies? p/Status topic)))))
