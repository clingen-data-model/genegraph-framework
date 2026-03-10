(ns genegraph.framework.topic-test
  (:require [genegraph.framework.protocol :as p]
            [genegraph.framework.event :as event]
            [genegraph.framework.topic]           ; loads defmethods for :simple-queue-topic and :timer-topic
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

;; ===========================================================================
;; TimerTopic
;; ===========================================================================

(defn make-timer-topic
  "Initialize a TimerTopic with a short interval and poll timeout for fast tests."
  [& {:as opts}]
  (p/init (merge {:name     :test-timer
                  :type     :timer-topic
                  :interval 50     ; ms between ticks
                  :timeout  100}   ; ms poll blocks waiting
                 opts)))

;; ---------------------------------------------------------------------------
;; p/init
;; ---------------------------------------------------------------------------

(deftest timer-init-test
  (testing "p/init with :type :timer-topic returns a TimerTopic"
    (let [topic (make-timer-topic)]
      (is (instance? genegraph.framework.topic.TimerTopic topic))))

  (testing "interval is stored on the record"
    (let [topic (make-timer-topic :interval 200)]
      (is (= 200 (:interval topic)))))

  (testing "default timeout of 1000ms is applied when not specified"
    (let [topic (p/init {:name :t :type :timer-topic :interval 50})]
      (is (= 1000 (:timeout topic)))))

  (testing "explicit :timeout is respected"
    (let [topic (make-timer-topic :timeout 250)]
      (is (= 250 (:timeout topic)))))

  (testing "satisfies Consumer, Lifecycle, and Status protocols"
    (let [topic (make-timer-topic)]
      (is (satisfies? p/Consumer topic))
      (is (satisfies? p/Lifecycle topic))
      (is (satisfies? p/Status topic))))

  (testing "does not satisfy Publisher (events are self-generated)"
    (let [topic (make-timer-topic)]
      (is (not (satisfies? p/Publisher topic))))))

;; ---------------------------------------------------------------------------
;; p/status
;; ---------------------------------------------------------------------------

(deftest timer-status-test
  (testing "status includes :name"
    (let [topic (make-timer-topic :name :my-timer)]
      (is (= :my-timer (:name (p/status topic))))))

  (testing "status includes :interval"
    (let [topic (make-timer-topic :interval 123)]
      (is (= 123 (:interval (p/status topic))))))

  (testing "status includes :queue-size"
    (let [topic (make-timer-topic)]
      (is (contains? (p/status topic) :queue-size))))

  (testing "status is :stopped before start"
    (let [topic (make-timer-topic)]
      (is (= :stopped (:status (p/status topic))))))

  (testing "status is :running after start"
    (let [topic (make-timer-topic)]
      (p/start topic)
      (try
        (is (= :running (:status (p/status topic))))
        (finally (p/stop topic)))))

  (testing "status is :stopped after stop"
    (let [topic (make-timer-topic)]
      (p/start topic)
      (p/stop topic)
      (Thread/sleep 100) ; allow virtual thread to exit
      (is (= :stopped (:status (p/status topic)))))))

;; ---------------------------------------------------------------------------
;; poll before start
;; ---------------------------------------------------------------------------

(deftest timer-poll-before-start-test
  (testing "poll returns nil before start (queue is empty)"
    (let [topic (make-timer-topic)]
      (is (nil? (p/poll topic))))))

;; ---------------------------------------------------------------------------
;; tick delivery
;; ---------------------------------------------------------------------------

(deftest timer-tick-delivery-test
  (testing "poll returns a tick event within interval + buffer after start"
    (let [topic (make-timer-topic :interval 50)]
      (p/start topic)
      (try
        (let [event (p/poll topic)]
          (is (some? event)))
        (finally (p/stop topic)))))

  (testing "tick event has ::event/topic set to topic :name"
    (let [topic (make-timer-topic :name :my-timer :interval 50)]
      (p/start topic)
      (try
        (let [event (p/poll topic)]
          (is (= :my-timer (::event/topic event))))
        (finally (p/stop topic)))))

  (testing "tick event carries ::event/data with :type :timer-tick"
    (let [topic (make-timer-topic :interval 50)]
      (p/start topic)
      (try
        (let [event (p/poll topic)]
          (is (= :timer-tick (get-in event [::event/data :type]))))
        (finally (p/stop topic)))))

  (testing "tick event carries ::event/data with a :timestamp"
    (let [topic (make-timer-topic :interval 50)]
      (p/start topic)
      (try
        (let [event (p/poll topic)]
          (is (some? (get-in event [::event/data :timestamp]))))
        (finally (p/stop topic)))))

  (testing "tick event carries an undelivered ::event/completion-promise"
    (let [topic (make-timer-topic :interval 50)]
      (p/start topic)
      (try
        (let [event (p/poll topic)]
          (is (instance? clojure.lang.IPending (::event/completion-promise event)))
          (is (not (realized? (::event/completion-promise event)))))
        (finally (p/stop topic)))))

  (testing "each tick gets its own distinct completion-promise"
    (let [topic (make-timer-topic :interval 50)]
      (p/start topic)
      (try
        (let [e1 (p/poll topic)
              e2 (p/poll topic)]
          (is (some? e1))
          (is (some? e2))
          (is (not (identical? (::event/completion-promise e1)
                               (::event/completion-promise e2)))))
        (finally (p/stop topic))))))

;; ---------------------------------------------------------------------------
;; tick drop on slow consumer
;; ---------------------------------------------------------------------------

(deftest timer-tick-drop-test
  (testing "queue never exceeds capacity of 1 — slow consumer causes ticks to be dropped"
    ;; Start the topic, wait for multiple intervals without polling, then check
    ;; that the queue holds at most one pending tick.
    (let [topic (make-timer-topic :interval 30)]
      (p/start topic)
      (try
        (Thread/sleep 200) ; allow ~6 ticks to fire, but queue cap is 1
        (is (<= (:queue-size (p/status topic)) 1))
        (finally (p/stop topic))))))

;; ---------------------------------------------------------------------------
;; stop halts tick production
;; ---------------------------------------------------------------------------

(deftest timer-stop-test
  (testing "after stop, poll returns nil (no further ticks are enqueued)"
    (let [topic (make-timer-topic :interval 50 :timeout 50)]
      (p/start topic)
      (p/poll topic) ; drain any tick already queued
      (p/stop topic)
      (Thread/sleep 150) ; wait for virtual thread to exit and any in-flight tick to clear
      ;; Drain one more time in case a tick was in-flight at stop, then check no further ticks
      (p/poll topic)
      (is (nil? (p/poll topic))))))
