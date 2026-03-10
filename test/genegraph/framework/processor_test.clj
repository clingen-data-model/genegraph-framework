(ns genegraph.framework.processor-test
  (:require [genegraph.framework.processor :as processor]
            [genegraph.framework.protocol :as p]
            [genegraph.framework.event :as event]
            [genegraph.framework.storage :as storage]
            [genegraph.framework.storage.atom]  ; loads :atom-store defmethod
            [genegraph.framework.topic]          ; loads :simple-queue-topic defmethod
            [io.pedestal.interceptor :as interceptor]
            [clojure.test :refer [deftest testing is]]))

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn make-topic
  "Initialize a SimpleQueueTopic with a short poll timeout."
  [& {:as opts}]
  (p/init (merge {:name :test-topic :type :simple-queue-topic :timeout 100}
                 opts)))

(defn make-store
  "Initialize an AtomStore."
  [& {:as opts}]
  (p/init (merge {:name :test-store :type :atom-store} opts)))

(defn make-processor
  "Initialize a Processor with empty interceptors, topics, and storage."
  [& {:as opts}]
  (p/init (merge {:name        :test-processor
                  :type        :processor
                  :interceptors []
                  :topics      {}
                  :storage     {}}
                 opts)))

(defn deref-cp
  "Deref a promise with a 2-second timeout; returns :timeout if it elapses."
  [p]
  (deref p 2000 :timeout))

(defn base-event
  "A minimal event map with a short effect-timeout so tests don't hang."
  []
  {::event/effect-timeout 500})

;; ---------------------------------------------------------------------------
;; p/process — interceptor execution
;; ---------------------------------------------------------------------------

(deftest process-interceptor-called-test
  (testing "interceptor :enter fn is called with every processed event"
    (let [called? (atom false)
          ic      (interceptor/interceptor
                   {:name  ::capture
                    :enter (fn [e] (reset! called? true) e)})
          proc    (make-processor :interceptors [ic])]
      (p/process proc (base-event))
      (is (true? @called?))))

  (testing "interceptors run in the declared order"
    (let [order (atom [])
          ic1   (interceptor/interceptor {:name ::ic1 :enter (fn [e] (swap! order conj :a) e)})
          ic2   (interceptor/interceptor {:name ::ic2 :enter (fn [e] (swap! order conj :b) e)})
          ic3   (interceptor/interceptor {:name ::ic3 :enter (fn [e] (swap! order conj :c) e)})
          proc  (make-processor :interceptors [ic1 ic2 ic3])]
      (p/process proc (base-event))
      (is (= [:a :b :c] @order)))))

;; ---------------------------------------------------------------------------
;; p/process — app state injection
;; ---------------------------------------------------------------------------

(deftest process-app-state-injection-test
  (testing "::storage/storage refs are injected into the event"
    (let [store    (make-store)
          captured (atom nil)
          ic       (interceptor/interceptor
                    {:name  ::capture-storage
                     :enter (fn [e] (reset! captured (::storage/storage e)) e)})
          proc     (make-processor :interceptors [ic]
                                   :storage {:my-store store})]
      (p/process proc (base-event))
      (is (contains? @captured :my-store))
      (is (= (storage/instance store) (:my-store @captured)))))

  (testing "::event/topics refs are injected into the event"
    (let [out-topic (make-topic :name :out-topic)
          captured  (atom nil)
          ic        (interceptor/interceptor
                     {:name  ::capture-topics
                      :enter (fn [e] (reset! captured (::event/topics e)) e)})
          proc      (make-processor :interceptors [ic]
                                    :topics {:out-topic out-topic})]
      (p/process proc (base-event))
      (is (contains? @captured :out-topic))
      (is (identical? out-topic (:out-topic @captured))))))

;; ---------------------------------------------------------------------------
;; p/process — local storage effects
;; ---------------------------------------------------------------------------

(deftest process-store-effects-test
  (testing "store effects accumulated in the chain are executed after the chain"
    ;; update-local-storage! is synchronous; storage is settled when p/process returns.
    (let [store (make-store)
          ic    (interceptor/interceptor
                 {:name  ::do-store
                  :enter (fn [e] (event/store e :my-store :k :v))})
          proc  (make-processor :interceptors [ic]
                                :storage {:my-store store})]
      (p/process proc (base-event))
      (is (= :v (storage/read (storage/instance store) :k)))))

  (testing "skip-local-effects true suppresses storage writes"
    ;; update-local-storage! skips command execution when skip-local-effects is set;
    ;; storage is still settled synchronously when p/process returns.
    (let [store (make-store)
          ic    (interceptor/interceptor
                 {:name  ::do-store
                  :enter (fn [e] (event/store e :my-store :k :v))})
          proc  (make-processor :interceptors [ic]
                                :storage {:my-store store})]
      (p/process proc (assoc (base-event) ::event/skip-local-effects true))
      (is (nil? (storage/read (storage/instance store) :k))))))

;; ---------------------------------------------------------------------------
;; p/process — publish effects
;; ---------------------------------------------------------------------------

(deftest process-publish-effects-test
  (testing "publish effects accumulated in the chain are sent to the target topic"
    ;; publish-events! is synchronous; topic has the event when p/process returns.
    (let [out-topic (make-topic :name :out-topic)
          ic        (interceptor/interceptor
                     {:name  ::do-publish
                      :enter (fn [e]
                               (event/publish e {::event/topic :out-topic
                                                 ::event/data  {:result 1}}))})
          proc      (make-processor :interceptors [ic]
                                    :topics {:out-topic out-topic})]
      (p/process proc (base-event))
      (let [polled (p/poll out-topic)]
        (is (= {:result 1} (::event/data polled))))))

  (testing "skip-publish-effects true suppresses topic publishes"
    (let [out-topic (make-topic :name :out-topic :timeout 50)
          ic        (interceptor/interceptor
                     {:name  ::do-publish
                      :enter (fn [e]
                               (event/publish e {::event/topic :out-topic
                                                 ::event/data  {:result 1}}))})
          proc      (make-processor :interceptors [ic]
                                    :topics {:out-topic out-topic})]
      (p/process proc (assoc (base-event) ::event/skip-publish-effects true))
      (is (nil? (p/poll out-topic))))))

;; ---------------------------------------------------------------------------
;; p/process — completion promise
;; ---------------------------------------------------------------------------

(deftest process-completion-promise-test
  (testing "completion-promise is delivered true when there are no effects"
    (let [cp   (promise)
          proc (make-processor)]
      (p/process proc (assoc (base-event) ::event/completion-promise cp))
      (is (= true (deref-cp cp)))))

  (testing "completion-promise is delivered true after successful store effects"
    (let [store (make-store)
          ic    (interceptor/interceptor
                 {:name  ::do-store
                  :enter (fn [e] (event/store e :my-store :k1 :v1))})
          cp    (promise)
          proc  (make-processor :interceptors [ic]
                                :storage {:my-store store})]
      (p/process proc (assoc (base-event) ::event/completion-promise cp))
      (is (= true (deref-cp cp)))))

  (testing "no completion-promise on event does not throw"
    (is (some? (p/process (make-processor) (base-event)))))

  (testing "completion-promise is delivered false when a commit-promise is never delivered"
    ;; Inject a synthetic effect whose commit-promise is never delivered.
    ;; With effect-timeout 200ms the virtual thread will unblock quickly.
    (let [uncommitted (promise)
          cp          (promise)
          proc        (make-processor)
          ev          (assoc (base-event)
                             ::event/completion-promise cp
                             ::event/effect-timeout     200
                             ::event/effects [{:command        (fn [& _] nil)
                                               :store          :fake
                                               :args           []
                                               :commit-promise uncommitted}])]
      (p/process proc ev)
      (is (= false (deref cp 3000 :timeout))))))

;; ---------------------------------------------------------------------------
;; p/as-interceptors
;; ---------------------------------------------------------------------------

(deftest as-interceptors-test
  (testing "returns a vector"
    (is (vector? (p/as-interceptors (make-processor)))))

  (testing "with no user interceptors there are exactly 4 framework interceptors"
    (is (= 4 (count (p/as-interceptors (make-processor))))))

  (testing "first interceptor is app-state-interceptor"
    (let [ics (p/as-interceptors (make-processor))]
      (is (= :genegraph.framework.processor/app-state-interceptor
             (:name (nth ics 0))))))

  (testing "second interceptor is local-effects-interceptor"
    (let [ics (p/as-interceptors (make-processor))]
      (is (= :genegraph.framework.processor/local-effects-interceptor
             (:name (nth ics 1))))))

  (testing "third interceptor is publish-effects-interceptor"
    (let [ics (p/as-interceptors (make-processor))]
      (is (= :genegraph.framework.processor/publish-effects-interceptor
             (:name (nth ics 2))))))

  (testing "fourth interceptor is deliver-completion-promise-interceptor"
    (let [ics (p/as-interceptors (make-processor))]
      (is (= :genegraph.framework.processor/deliver-completion-promise-interceptor
             (:name (nth ics 3))))))

  (testing "user interceptors are appended after the 4 framework interceptors"
    (let [ic1  (interceptor/interceptor {:name ::user-ic-1 :enter identity})
          ic2  (interceptor/interceptor {:name ::user-ic-2 :enter identity})
          proc (make-processor :interceptors [ic1 ic2])
          ics  (p/as-interceptors proc)]
      (is (= 6 (count ics)))
      (is (= ::user-ic-1 (:name (nth ics 4))))
      (is (= ::user-ic-2 (:name (nth ics 5)))))))

;; ---------------------------------------------------------------------------
;; p/status
;; ---------------------------------------------------------------------------

(deftest processor-status-test
  (testing "status includes the processor :name"
    (let [proc (make-processor :name :my-proc)]
      (is (= :my-proc (:name (p/status proc))))))

  (testing "status includes :subscribe"
    (let [proc (make-processor :subscribe :my-topic)]
      (is (= :my-topic (:subscribe (p/status proc))))))

  (testing "status is :stopped before start"
    (let [proc (make-processor)]
      (is (= :stopped (:status (p/status proc))))))

  (testing "status is :running while running"
    (let [proc (make-processor)]
      (p/start proc)
      (try
        (is (= :running (:status (p/status proc))))
        (finally (p/stop proc)))))

  (testing "status is :stopped after stop"
    (let [proc (make-processor)]
      (p/start proc)
      (p/stop proc)
      (is (= :stopped (:status (p/status proc)))))))

;; ---------------------------------------------------------------------------
;; Processor lifecycle — polling loop
;; ---------------------------------------------------------------------------

(deftest processor-polling-loop-test
  (testing "started processor processes events published to its subscribed topic"
    (let [in-topic (make-topic :name :in-topic)
          cp       (promise)
          proc     (make-processor :subscribe :in-topic
                                   :topics {:in-topic in-topic})]
      (p/start proc)
      (try
        (p/publish in-topic (assoc (base-event) ::event/completion-promise cp))
        (is (= true (deref-cp cp)))
        (finally
          (p/stop proc)))))

  (testing "started processor executes store effects from polled events"
    (let [in-topic (make-topic :name :in-topic)
          store    (make-store)
          ic       (interceptor/interceptor
                    {:name  ::do-store
                     :enter (fn [e] (event/store e :my-store :k :from-loop))})
          cp       (promise)
          proc     (make-processor :subscribe    :in-topic
                                   :topics       {:in-topic in-topic}
                                   :storage      {:my-store store}
                                   :interceptors [ic])]
      (p/start proc)
      (try
        (p/publish in-topic (assoc (base-event) ::event/completion-promise cp))
        (is (= true (deref-cp cp)))
        (is (= :from-loop (storage/read (storage/instance store) :k)))
        (finally
          (p/stop proc)))))

  (testing "polling loop exits after stop — events published after stop are not processed"
    (let [in-topic (make-topic :name :in-topic)
          cp       (promise)
          proc     (make-processor :subscribe :in-topic
                                   :topics {:in-topic in-topic})]
      (p/start proc)
      (p/stop proc)
      ;; Allow the loop thread to notice the stop and exit (≤ 1 poll timeout ≈ 100ms).
      (Thread/sleep 300)
      (p/publish in-topic (assoc (base-event) ::event/completion-promise cp))
      (is (= :timeout (deref cp 400 :timeout))))))

;; ---------------------------------------------------------------------------
;; Processor lifecycle — exception resilience
;; ---------------------------------------------------------------------------

(deftest processor-exception-resilience-test
  (testing "exception in an interceptor is caught and the polling loop keeps running"
    (let [in-topic     (make-topic :name :in-topic)
          first-event? (atom true)
          ic           (interceptor/interceptor
                        {:name  ::throw-once
                         :enter (fn [e]
                                  (when @first-event?
                                    (reset! first-event? false)
                                    (throw (ex-info "deliberate test exception" {})))
                                  e)})
          cp           (promise)
          proc         (make-processor :subscribe    :in-topic
                                       :topics       {:in-topic in-topic}
                                       :interceptors [ic])]
      (p/start proc)
      (try
        ;; First event: interceptor throws; no completion-promise so we don't block on it.
        (p/publish in-topic (base-event))
        ;; Give the loop time to process (and catch the exception for) the first event.
        (Thread/sleep 300)
        ;; Second event: interceptor passes through; cp should be delivered true.
        (p/publish in-topic (assoc (base-event) ::event/completion-promise cp))
        (is (= true (deref-cp cp)))
        (finally
          (p/stop proc))))))

;; ---------------------------------------------------------------------------
;; ParallelProcessor — init and status
;; ---------------------------------------------------------------------------

(deftest parallel-processor-init-test
  (testing "p/init :parallel-processor returns a ParallelProcessor record"
    (let [proc (p/init {:name         :par-proc
                        :type         :parallel-processor
                        :interceptors []
                        :topics       {}
                        :storage      {}})]
      (is (instance? genegraph.framework.processor.ParallelProcessor proc))))

  (testing "status includes :deserialized-event-queue-size and :effect-queue-size"
    (let [proc (p/init {:name         :par-proc
                        :type         :parallel-processor
                        :interceptors []
                        :topics       {}
                        :storage      {}})
          s    (p/status proc)]
      (is (contains? s :deserialized-event-queue-size))
      (is (contains? s :effect-queue-size))
      (is (= 0 (:deserialized-event-queue-size s)))
      (is (= 0 (:effect-queue-size s)))))

  (testing "status is :stopped before start"
    (let [proc (p/init {:name         :par-proc
                        :type         :parallel-processor
                        :interceptors []
                        :topics       {}
                        :storage      {}})]
      (is (= :stopped (:status (p/status proc)))))))

;; ---------------------------------------------------------------------------
;; ParallelProcessor — event processing
;; ---------------------------------------------------------------------------

(deftest parallel-processor-event-test
  (testing "processes a single event end-to-end and delivers the completion promise"
    (let [in-topic (make-topic :name :in-topic)
          cp       (promise)
          proc     (p/init {:name         :par-proc
                             :type         :parallel-processor
                             :subscribe    :in-topic
                             :topics       {:in-topic in-topic}
                             :interceptors []
                             :storage      {}})]
      (p/start proc)
      (try
        (p/publish in-topic (assoc (base-event) ::event/completion-promise cp))
        ;; Parallel pipeline has 3 stages — allow extra time.
        (is (= true (deref cp 3000 :timeout)))
        (finally
          (p/stop proc)))))

  (testing "executes store effects for events processed via the parallel pipeline"
    (let [in-topic (make-topic :name :in-topic)
          store    (make-store)
          ic       (interceptor/interceptor
                    {:name  ::par-store
                     :enter (fn [e] (event/store e :my-store :par-key :par-val))})
          cp       (promise)
          proc     (p/init {:name         :par-proc
                             :type         :parallel-processor
                             :subscribe    :in-topic
                             :topics       {:in-topic in-topic}
                             :interceptors [ic]
                             :storage      {:my-store store}})]
      (p/start proc)
      (try
        (p/publish in-topic (assoc (base-event) ::event/completion-promise cp))
        (is (= true (deref cp 3000 :timeout)))
        (is (= :par-val (storage/read (storage/instance store) :par-key)))
        (finally
          (p/stop proc)))))

  (testing "multiple events are all processed and their completion promises delivered"
    (let [in-topic (make-topic :name :in-topic :buffer-size 20)
          n        5
          cps      (vec (repeatedly n promise))
          proc     (p/init {:name         :par-proc
                             :type         :parallel-processor
                             :subscribe    :in-topic
                             :topics       {:in-topic in-topic}
                             :interceptors []
                             :storage      {}})]
      (p/start proc)
      (try
        (doseq [cp cps]
          (p/publish in-topic (assoc (base-event) ::event/completion-promise cp)))
        (is (every? #(= true (deref % 3000 :timeout)) cps))
        (finally
          (p/stop proc))))))
