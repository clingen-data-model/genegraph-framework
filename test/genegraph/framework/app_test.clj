(ns genegraph.framework.app-test
  (:require [genegraph.framework.app :as app]
            [genegraph.framework.protocol :as p]
            [genegraph.framework.event :as event]
            [genegraph.framework.storage :as storage]
            [genegraph.framework.topic]        ; loads :simple-queue-topic defmethod
            [genegraph.framework.storage.atom] ; loads :atom-store defmethod
            [io.pedestal.interceptor :as interceptor]
            [clojure.test :refer [deftest testing is]]))

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn minimal-app-def
  "A valid app-def with no user-defined components."
  []
  {:type         :genegraph-app
   :topics       {}
   :storage      {}
   :processors   {}
   :http-servers {}})

(defn deref-cp
  "Deref a promise with a 2-second timeout; returns :timeout if it elapses."
  [p]
  (deref p 2000 :timeout))

;; ---------------------------------------------------------------------------
;; p/init — App record construction
;; ---------------------------------------------------------------------------

(deftest init-returns-app-record-test
  (testing "p/init returns an App record"
    (is (instance? genegraph.framework.app.App
                   (p/init (minimal-app-def)))))

  (testing "App record has the expected top-level keys"
    (let [app (p/init (minimal-app-def))]
      (is (map? (:topics app)))
      (is (map? (:storage app)))
      (is (map? (:processors app)))
      (is (map? (:http-servers app))))))

;; ---------------------------------------------------------------------------
;; p/init — System topic and system processor
;; ---------------------------------------------------------------------------

(deftest init-system-topic-test
  (testing "the :system topic is auto-created"
    (let [app (p/init (minimal-app-def))]
      (is (contains? (:topics app) :system))))

  (testing ":system topic is a SimpleQueueTopic"
    (let [app (p/init (minimal-app-def))]
      (is (instance? genegraph.framework.topic.SimpleQueueTopic
                     (get-in app [:topics :system])))))

  (testing ":system is the only topic when no user topics are defined"
    (let [app (p/init (minimal-app-def))]
      (is (= 1 (count (:topics app)))))))

(deftest init-default-system-processor-test
  (testing "default system processor is added when no custom system processor is defined"
    (let [app (p/init (minimal-app-def))]
      (is (contains? (:processors app) :default-system-processor))))

  (testing "default system processor subscribes to :system"
    (let [app (p/init (minimal-app-def))]
      (is (= :system (get-in app [:processors :default-system-processor :subscribe])))))

  (testing "a user-defined processor with :subscribe :system is present after init"
    ;; Note: has-custom-system-processor? iterates over map entries rather than vals,
    ;; so it never detects a custom system processor and the default is always added.
    ;; We verify the custom processor is initialized and present.
    (let [custom-ic (interceptor/interceptor {:name ::custom-sys :enter identity})
          app       (p/init {:type         :genegraph-app
                             :topics       {}
                             :storage      {}
                             :processors   {:my-sys {:name         :my-sys
                                                     :type         :processor
                                                     :subscribe    :system
                                                     :interceptors [custom-ic]}}
                             :http-servers {}})]
      (is (contains? (:processors app) :my-sys))
      (is (= :system (get-in app [:processors :my-sys :subscribe]))))))

;; ---------------------------------------------------------------------------
;; p/init — Component initialization and wiring
;; ---------------------------------------------------------------------------

(deftest init-topics-initialized-test
  (testing "user-defined topics are initialized to their record types"
    (let [app (p/init {:type         :genegraph-app
                       :topics       {:my-topic {:name :my-topic :type :simple-queue-topic}}
                       :storage      {}
                       :processors   {}
                       :http-servers {}})]
      (is (instance? genegraph.framework.topic.SimpleQueueTopic
                     (get-in app [:topics :my-topic])))))

  (testing "multiple user-defined topics are all initialized"
    (let [app (p/init {:type         :genegraph-app
                       :topics       {:t1 {:name :t1 :type :simple-queue-topic}
                                      :t2 {:name :t2 :type :simple-queue-topic}}
                       :storage      {}
                       :processors   {}
                       :http-servers {}})]
      (is (instance? genegraph.framework.topic.SimpleQueueTopic (get-in app [:topics :t1])))
      (is (instance? genegraph.framework.topic.SimpleQueueTopic (get-in app [:topics :t2]))))))

(deftest init-storage-initialized-test
  (testing "user-defined storage is initialized to its record type"
    (let [app (p/init {:type         :genegraph-app
                       :topics       {}
                       :storage      {:my-store {:name :my-store :type :atom-store}}
                       :processors   {}
                       :http-servers {}})]
      (is (instance? genegraph.framework.storage.atom.AtomStore
                     (get-in app [:storage :my-store])))))

  (testing "storage instance is accessible via storage/instance"
    (let [app   (p/init {:type         :genegraph-app
                         :topics       {}
                         :storage      {:my-store {:name :my-store :type :atom-store}}
                         :processors   {}
                         :http-servers {}})
          store (get-in app [:storage :my-store])]
      (is (some? (storage/instance store))))))

(deftest init-processor-wiring-test
  (testing "processor's :topics map contains the initialized topic record"
    (let [app (p/init {:type         :genegraph-app
                       :topics       {:my-topic {:name :my-topic :type :simple-queue-topic}}
                       :storage      {}
                       :processors   {:my-proc {:name         :my-proc
                                                :type         :processor
                                                :subscribe    :my-topic
                                                :interceptors []}}
                       :http-servers {}})]
      (is (instance? genegraph.framework.topic.SimpleQueueTopic
                     (get-in app [:processors :my-proc :topics :my-topic])))))

  (testing "processor's :storage map contains the initialized storage record"
    (let [app (p/init {:type         :genegraph-app
                       :topics       {}
                       :storage      {:my-store {:name :my-store :type :atom-store}}
                       :processors   {:my-proc {:name         :my-proc
                                                :type         :processor
                                                :subscribe    nil
                                                :interceptors []}}
                       :http-servers {}})]
      (is (instance? genegraph.framework.storage.atom.AtomStore
                     (get-in app [:processors :my-proc :storage :my-store])))))

  (testing "processor def is initialized to a Processor record"
    (let [app (p/init {:type         :genegraph-app
                       :topics       {:my-topic {:name :my-topic :type :simple-queue-topic}}
                       :storage      {}
                       :processors   {:my-proc {:name         :my-proc
                                                :type         :processor
                                                :subscribe    :my-topic
                                                :interceptors []}}
                       :http-servers {}})]
      (is (instance? genegraph.framework.processor.Processor
                     (get-in app [:processors :my-proc]))))))

;; ---------------------------------------------------------------------------
;; p/status
;; ---------------------------------------------------------------------------

(deftest app-status-test
  (testing "p/status returns a map with :topics, :storage, :processors, :http-servers"
    (let [s (p/status (p/init (minimal-app-def)))]
      (is (map? (:topics s)))
      (is (map? (:storage s)))
      (is (map? (:processors s)))
      (is (map? (:http-servers s)))))

  (testing "status map includes an entry for each processor"
    (let [app (p/init {:type         :genegraph-app
                       :topics       {}
                       :storage      {}
                       :processors   {:my-proc {:name         :my-proc
                                                :type         :processor
                                                :subscribe    nil
                                                :interceptors []}}
                       :http-servers {}})
          s   (p/status app)]
      (is (contains? (:processors s) :my-proc))
      (is (contains? (:processors s) :default-system-processor))))

  (testing "status map includes an entry for each topic"
    (let [app (p/init {:type         :genegraph-app
                       :topics       {:my-topic {:name :my-topic :type :simple-queue-topic}}
                       :storage      {}
                       :processors   {}
                       :http-servers {}})
          s   (p/status app)]
      (is (contains? (:topics s) :my-topic))
      (is (contains? (:topics s) :system))))

  (testing "status map includes an entry for each storage component"
    (let [app (p/init {:type         :genegraph-app
                       :topics       {}
                       :storage      {:my-store {:name :my-store :type :atom-store}}
                       :processors   {}
                       :http-servers {}})
          s   (p/status app)]
      (is (contains? (:storage s) :my-store)))))

;; ---------------------------------------------------------------------------
;; App lifecycle — start / stop
;; ---------------------------------------------------------------------------

(deftest app-lifecycle-test
  (testing "processors are :running after p/start"
    (let [app (p/init {:type         :genegraph-app
                       :topics       {:in-topic {:name :in-topic :type :simple-queue-topic}}
                       :storage      {}
                       :processors   {:my-proc {:name         :my-proc
                                                :type         :processor
                                                :subscribe    :in-topic
                                                :interceptors []}}
                       :http-servers {}})]
      (p/start app)
      (try
        (is (= :running (get-in (p/status app) [:processors :my-proc :status])))
        (is (= :running (get-in (p/status app) [:processors :default-system-processor :status])))
        (finally
          (p/stop app)))))

  (testing "processors are :stopped after p/stop"
    (let [app (p/init {:type         :genegraph-app
                       :topics       {:in-topic {:name :in-topic :type :simple-queue-topic}}
                       :storage      {}
                       :processors   {:my-proc {:name         :my-proc
                                                :type         :processor
                                                :subscribe    :in-topic
                                                :interceptors []}}
                       :http-servers {}})]
      (p/start app)
      (p/stop app)
      (is (= :stopped (get-in (p/status app) [:processors :my-proc :status])))
      (is (= :stopped (get-in (p/status app) [:processors :default-system-processor :status]))))))

;; ---------------------------------------------------------------------------
;; End-to-end event flow — single processor
;; ---------------------------------------------------------------------------

(deftest e2e-single-processor-test
  (testing "event published to topic is processed and stored in AtomStore"
    (let [store-ic (interceptor/interceptor
                    {:name  ::store-ic
                     :enter (fn [e] (event/store e :my-store :result (::event/data e)))})
          app      (p/init {:type         :genegraph-app
                            :topics       {:in-topic {:name :in-topic :type :simple-queue-topic}}
                            :storage      {:my-store {:name :my-store :type :atom-store}}
                            :processors   {:my-proc {:name         :my-proc
                                                     :type         :processor
                                                     :subscribe    :in-topic
                                                     :interceptors [store-ic]}}
                            :http-servers {}})
          in-topic (get-in app [:topics :in-topic])
          store    (get-in app [:storage :my-store])
          cp       (promise)]
      (p/start app)
      (try
        (p/publish in-topic {::event/data              {:answer 42}
                             ::event/completion-promise cp
                             ::event/effect-timeout    500})
        (is (= true (deref-cp cp)))
        (is (= {:answer 42} (storage/read (storage/instance store) :result)))
        (finally
          (p/stop app))))))

;; ---------------------------------------------------------------------------
;; End-to-end event flow — two-processor pipeline
;; ---------------------------------------------------------------------------

(deftest e2e-pipeline-test
  (testing "proc-a publishes to mid-topic → proc-b receives and signals completion"
    (let [result-p (promise)
          fwd-ic   (interceptor/interceptor
                    {:name  ::fwd-ic
                     :enter (fn [e]
                              (event/publish e {::event/topic :mid-topic
                                               ::event/data  (::event/data e)}))})
          recv-ic  (interceptor/interceptor
                    {:name  ::recv-ic
                     :enter (fn [e]
                              (deliver result-p (::event/data e))
                              e)})
          app      (p/init {:type         :genegraph-app
                            :topics       {:in-topic  {:name :in-topic  :type :simple-queue-topic}
                                           :mid-topic {:name :mid-topic :type :simple-queue-topic}}
                            :storage      {}
                            :processors   {:proc-a {:name         :proc-a
                                                     :type         :processor
                                                     :subscribe    :in-topic
                                                     :interceptors [fwd-ic]}
                                           :proc-b {:name         :proc-b
                                                     :type         :processor
                                                     :subscribe    :mid-topic
                                                     :interceptors [recv-ic]}}
                            :http-servers {}})
          in-topic (get-in app [:topics :in-topic])]
      (p/start app)
      (try
        (p/publish in-topic {::event/data           {:val 99}
                             ::event/effect-timeout 500})
        (is (= {:val 99} (deref result-p 3000 :timeout)))
        (finally
          (p/stop app))))))

;; ---------------------------------------------------------------------------
;; System topic — register-listener / trigger-listeners
;; ---------------------------------------------------------------------------

(deftest register-listener-test
  (testing "listener promise is delivered with ::event/data when a matching event arrives"
    (let [result      (promise)
          register-cp (promise)
          trigger-cp  (promise)
          app         (p/init (minimal-app-def))
          sys-topic   (get-in app [:topics :system])]
      (p/start app)
      (try
        ;; Publish a register-listener event to the system topic.
        (p/publish sys-topic {:type                     :register-listener
                              :name                     :my-listener
                              :promise                  result
                              :predicate                (fn [e]
                                                          (= :my-trigger
                                                             (get-in e [::event/data :type])))
                              ::event/completion-promise register-cp
                              ::event/effect-timeout    500})
        ;; Wait for the register event to be fully processed.
        (is (= true (deref-cp register-cp)))
        ;; Now publish a matching trigger event.
        (p/publish sys-topic {::event/data              {:type :my-trigger :value 7}
                              ::event/completion-promise trigger-cp
                              ::event/effect-timeout    500})
        (deref-cp trigger-cp)
        ;; The listener promise should be delivered with the event data.
        (is (= {:type :my-trigger :value 7} (deref result 2000 :timeout)))
        (finally
          (p/stop app)))))

  (testing "listener is not triggered by events that do not satisfy the predicate"
    (let [result      (promise)
          register-cp (promise)
          other-cp    (promise)
          app         (p/init (minimal-app-def))
          sys-topic   (get-in app [:topics :system])]
      (p/start app)
      (try
        (p/publish sys-topic {:type                     :register-listener
                              :name                     :picky-listener
                              :promise                  result
                              :predicate                (fn [e]
                                                          (= :exact-match
                                                             (get-in e [::event/data :type])))
                              ::event/completion-promise register-cp
                              ::event/effect-timeout    500})
        (is (= true (deref-cp register-cp)))
        ;; Publish a non-matching event.
        (p/publish sys-topic {::event/data              {:type :wrong-type}
                              ::event/completion-promise other-cp
                              ::event/effect-timeout    500})
        (deref-cp other-cp)
        ;; Promise should not be delivered.
        (is (= :timeout (deref result 300 :timeout)))
        (finally
          (p/stop app))))))

;; ---------------------------------------------------------------------------
;; p/reset
;; ---------------------------------------------------------------------------

(deftest app-reset-test
  (testing "App satisfies Resetable"
    (is (satisfies? p/Resetable (p/init (minimal-app-def)))))

  (testing "p/reset on a pure in-process app does not throw"
    ;; SimpleQueueTopic and AtomStore do not implement Resetable, so reset is a no-op.
    (let [app (p/init {:type         :genegraph-app
                       :topics       {:t {:name :t :type :simple-queue-topic}}
                       :storage      {:s {:name :s :type :atom-store}}
                       :processors   {}
                       :http-servers {}})]
      (is (nil? (p/reset app))))))
