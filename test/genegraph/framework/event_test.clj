(ns genegraph.framework.event-test
  (:require [genegraph.framework.event :as event]
            [genegraph.framework.storage :as storage]
            [clojure.test :refer [deftest testing is]]))

;; ---------------------------------------------------------------------------
;; Serialization / Deserialization
;; ---------------------------------------------------------------------------

(deftest deserialize-json-test
  (testing "parses JSON string in ::value into ::data with keyword keys"
    (let [result (event/deserialize {::event/format :json
                                     ::event/value  "{\"a\":1,\"b\":\"hello\"}"})]
      (is (= {:a 1 :b "hello"} (::event/data result)))))

  (testing "nested JSON objects are also keywordized"
    (let [result (event/deserialize {::event/format :json
                                     ::event/value  "{\"outer\":{\"inner\":42}}"})]
      (is (= {:outer {:inner 42}} (::event/data result)))))

  (testing "preserves all other event keys unchanged"
    (let [result (event/deserialize {::event/format :json
                                     ::event/value  "{\"x\":true}"
                                     ::event/offset 7})]
      (is (= 7 (::event/offset result)))
      (is (= :json (::event/format result)))))

  (testing "does not modify ::value"
    (let [raw    "{\"k\":\"v\"}"
          result (event/deserialize {::event/format :json
                                     ::event/value  raw})]
      (is (= raw (::event/value result))))))

(deftest serialize-json-test
  (testing "writes ::data map to ::value as a JSON string"
    (let [result (event/serialize {::event/format :json
                                   ::event/data   {:a 1}})]
      (is (string? (::event/value result)))))

  (testing "round-trip: serialize then deserialize returns original data"
    (let [data   {:foo "bar" :n 3}
          result (-> {::event/format :json
                      ::event/data   data}
                     event/serialize
                     event/deserialize)]
      (is (= data (::event/data result)))))

  (testing "preserves other event keys"
    (let [result (event/serialize {::event/format :json
                                   ::event/data   {:x 1}
                                   ::event/offset 5})]
      (is (= 5 (::event/offset result))))))

(deftest deserialize-edn-test
  (testing "parses EDN string in ::value into ::data"
    (let [result (event/deserialize {::event/format :edn
                                     ::event/value  (pr-str {:a 1 :b [2 3]})})]
      (is (= {:a 1 :b [2 3]} (::event/data result)))))

  (testing "round-trip: serialize then deserialize returns original data"
    (let [data   {:nested {:x #{:a :b}} :count 99}
          result (-> {::event/format :edn
                      ::event/data   data}
                     event/serialize
                     event/deserialize)]
      (is (= data (::event/data result)))))

  (testing "preserves other event keys"
    (let [result (event/deserialize {::event/format :edn
                                     ::event/value  (pr-str 42)
                                     ::event/offset 3})]
      (is (= 3 (::event/offset result))))))

(deftest serialize-edn-test
  (testing "writes ::data to ::value using pr-str"
    (let [data   {:a :b}
          result (event/serialize {::event/format :edn
                                   ::event/data   data})]
      (is (= (pr-str data) (::event/value result))))))

(deftest deserialize-default-test
  (testing "when no format is set, deserialize returns the event unchanged"
    (let [event {::event/value "raw-string"}]
      (is (= event (event/deserialize event)))))

  (testing "does not add ::data when no format"
    (let [result (event/deserialize {::event/value "x"})]
      (is (not (contains? result ::event/data))))))

(deftest serialize-default-test
  (testing "when no format is set, serialize passes ::data through as ::value"
    (let [result (event/serialize {::event/data {:some :thing}})]
      (is (= {:some :thing} (::event/value result)))))

  (testing "preserves other keys"
    (let [result (event/serialize {::event/data {:x 1} ::event/offset 9})]
      (is (= 9 (::event/offset result))))))

;; ---------------------------------------------------------------------------
;; Effect accumulation — event/store
;; ---------------------------------------------------------------------------

(def mock-store-instance
  "A plain value used as a stand-in storage instance. Tests verify it is
  passed through into :args without actually performing any I/O."
  ::mock-store)

(defn base-event-with-store
  "Returns an event map that has a mock storage instance registered under :my-store."
  []
  {::storage/storage {:my-store mock-store-instance}})

(deftest store-effect-test
  (testing "adds a single entry to ::effects"
    (let [result (event/store (base-event-with-store) :my-store :the-key :the-val)]
      (is (= 1 (count (::event/effects result))))))

  (testing "effect :command is storage/write"
    (let [effect (-> (base-event-with-store)
                     (event/store :my-store :k :v)
                     ::event/effects
                     first)]
      (is (= storage/write (:command effect)))))

  (testing "effect :store names the store key"
    (let [effect (-> (base-event-with-store)
                     (event/store :my-store :k :v)
                     ::event/effects
                     first)]
      (is (= :my-store (:store effect)))))

  (testing "effect :args contains the resolved storage instance, key, value, and promise"
    (let [effect (-> (base-event-with-store)
                     (event/store :my-store :k :v)
                     ::event/effects
                     first)
          [store-arg key-arg val-arg promise-arg] (:args effect)]
      (is (= mock-store-instance store-arg))
      (is (= :k key-arg))
      (is (= :v val-arg))
      (is (instance? clojure.lang.IPending promise-arg))))

  (testing "effect has a :commit-promise that is a promise"
    (let [effect (-> (base-event-with-store)
                     (event/store :my-store :k :v)
                     ::event/effects
                     first)]
      (is (instance? clojure.lang.IPending (:commit-promise effect)))))

  (testing "the :commit-promise in :args and the top-level :commit-promise are the same object"
    (let [effect (-> (base-event-with-store)
                     (event/store :my-store :k :v)
                     ::event/effects
                     first)
          promise-in-args (last (:args effect))]
      (is (identical? (:commit-promise effect) promise-in-args))))

  (testing "multiple store calls accumulate separate entries"
    (let [result (-> (base-event-with-store)
                     (event/store :my-store :k1 :v1)
                     (event/store :my-store :k2 :v2)
                     (event/store :my-store :k3 :v3))]
      (is (= 3 (count (::event/effects result))))))

  (testing "each store call gets its own independent :commit-promise"
    (let [effects (-> (base-event-with-store)
                      (event/store :my-store :k1 :v1)
                      (event/store :my-store :k2 :v2)
                      ::event/effects)]
      (is (not (identical? (:commit-promise (first effects))
                           (:commit-promise (second effects)))))))

  (testing "does not modify the event beyond ::effects"
    (let [base   (assoc (base-event-with-store) ::event/offset 10)
          result (event/store base :my-store :k :v)]
      (is (= 10 (::event/offset result)))))

  (testing "event without that store key puts nil as the storage instance in :args"
    ;; The framework resolves the instance via get-in; if the key is absent
    ;; the first :args element will be nil.
    (let [effect (-> {}
                     (event/store :missing-store :k :v)
                     ::event/effects
                     first)
          [store-arg] (:args effect)]
      (is (nil? store-arg)))))

;; ---------------------------------------------------------------------------
;; Effect accumulation — event/delete
;; ---------------------------------------------------------------------------

(deftest delete-effect-test
  (testing "adds a single entry to ::effects"
    (let [result (event/delete (base-event-with-store) :my-store :the-key)]
      (is (= 1 (count (::event/effects result))))))

  (testing "effect :command is storage/delete"
    (let [effect (-> (base-event-with-store)
                     (event/delete :my-store :k)
                     ::event/effects
                     first)]
      (is (= storage/delete (:command effect)))))

  (testing "effect :args contains the resolved storage instance, key, and promise"
    (let [effect (-> (base-event-with-store)
                     (event/delete :my-store :k)
                     ::event/effects
                     first)
          [store-arg key-arg promise-arg] (:args effect)]
      (is (= mock-store-instance store-arg))
      (is (= :k key-arg))
      (is (instance? clojure.lang.IPending promise-arg))))

  (testing "effect has a :commit-promise that is a promise"
    (let [effect (-> (base-event-with-store)
                     (event/delete :my-store :k)
                     ::event/effects
                     first)]
      (is (instance? clojure.lang.IPending (:commit-promise effect)))))

  (testing "the :commit-promise in :args and the top-level :commit-promise are the same object"
    (let [effect (-> (base-event-with-store)
                     (event/delete :my-store :k)
                     ::event/effects
                     first)
          promise-in-args (last (:args effect))]
      (is (identical? (:commit-promise effect) promise-in-args))))

  (testing "multiple delete calls accumulate separate entries"
    (let [result (-> (base-event-with-store)
                     (event/delete :my-store :k1)
                     (event/delete :my-store :k2))]
      (is (= 2 (count (::event/effects result))))))

  (testing "store and delete effects accumulate together in order"
    (let [result (-> (base-event-with-store)
                     (event/store :my-store :k1 :v1)
                     (event/delete :my-store :k2)
                     (event/store :my-store :k3 :v3))]
      (is (= 3 (count (::event/effects result))))
      (is (= storage/write  (:command (nth (::event/effects result) 0))))
      (is (= storage/delete (:command (nth (::event/effects result) 1))))
      (is (= storage/write  (:command (nth (::event/effects result) 2)))))))

;; ---------------------------------------------------------------------------
;; Effect accumulation — event/publish
;; ---------------------------------------------------------------------------

(deftest publish-effect-test
  (testing "adds a single entry to ::publish"
    (let [result (event/publish {} {::event/topic :out-topic ::event/data {:x 1}})]
      (is (= 1 (count (::event/publish result))))))

  (testing "the publish entry contains all fields from the publish-event"
    (let [publish-event {::event/topic :out-topic
                         ::event/key   "k1"
                         ::event/data  {:x 1}}
          entry         (-> {}
                            (event/publish publish-event)
                            ::event/publish
                            first)]
      (is (= :out-topic (::event/topic entry)))
      (is (= "k1" (::event/key entry)))
      (is (= {:x 1} (::event/data entry)))))

  (testing "adds a :commit-promise to the publish entry"
    (let [entry (-> {}
                    (event/publish {::event/topic :t})
                    ::event/publish
                    first)]
      (is (contains? entry :commit-promise))
      (is (instance? clojure.lang.IPending (:commit-promise entry)))))

  (testing "does not modify the original publish-event map"
    (let [publish-event {::event/topic :t}]
      (event/publish {} publish-event)
      (is (not (contains? publish-event :commit-promise)))))

  (testing "multiple publish calls accumulate separate entries"
    (let [result (-> {}
                     (event/publish {::event/topic :t1})
                     (event/publish {::event/topic :t2})
                     (event/publish {::event/topic :t3}))
          topics (into #{} (map ::event/topic) (::event/publish result))]
      (is (= 3 (count (::event/publish result))))
      (is (contains? topics :t1))
      (is (contains? topics :t2))
      (is (contains? topics :t3))))

  (testing "each publish call gets its own independent :commit-promise"
    (let [entries (-> {}
                      (event/publish {::event/topic :t1})
                      (event/publish {::event/topic :t2})
                      ::event/publish)]
      (is (not (identical? (:commit-promise (first entries))
                           (:commit-promise (second entries)))))))

  (testing "publish does not affect ::effects"
    (let [result (event/publish {} {::event/topic :t})]
      (is (nil? (::event/effects result)))))

  (testing "store does not affect ::publish"
    (let [result (event/store (base-event-with-store) :my-store :k :v)]
      (is (nil? (::event/publish result))))))

;; ---------------------------------------------------------------------------
;; Offset recording
;; ---------------------------------------------------------------------------

(deftest record-offset-test
  (testing "appends one entry to ::effects"
    (let [base   {::event/backing-store ::mock-backing-store
                  ::event/topic         :my-topic
                  ::event/offset        42}
          result (event/record-offset base)]
      (is (= 1 (count (::event/effects result))))))

  (testing "effect :command is storage/store-offset"
    (let [effect (-> {::event/backing-store ::mock-backing-store
                      ::event/topic         :my-topic
                      ::event/offset        42}
                     event/record-offset
                     ::event/effects
                     first)]
      (is (= storage/store-offset (:command effect)))))

  (testing "effect :args are [backing-store topic offset] in that order"
    (let [effect (-> {::event/backing-store ::mock-backing-store
                      ::event/topic         :my-topic
                      ::event/offset        42}
                     event/record-offset
                     ::event/effects
                     first)
          [bs-arg topic-arg offset-arg] (:args effect)]
      (is (= ::mock-backing-store bs-arg))
      (is (= :my-topic topic-arg))
      (is (= 42 offset-arg))))

  (testing "accumulates with existing effects"
    ;; conj on a list prepends, so effects are in LIFO order:
    ;; record-offset (called second) ends up first, store (called first) ends up second.
    (let [result (-> {::event/backing-store ::mock-backing-store
                      ::event/topic         :my-topic
                      ::event/offset        1
                      ::storage/storage     {:s ::mock-store}}
                     (event/store :s :k :v)
                     event/record-offset)
          commands (into #{} (map :command) (::event/effects result))]
      (is (= 2 (count (::event/effects result))))
      (is (contains? commands storage/write))
      (is (contains? commands storage/store-offset))))

  (testing "does not affect other event keys"
    (let [base   {::event/backing-store ::mock-backing-store
                  ::event/topic         :my-topic
                  ::event/offset        7
                  ::event/data          {:important :value}}
          result (event/record-offset base)]
      (is (= {:important :value} (::event/data result))))))
