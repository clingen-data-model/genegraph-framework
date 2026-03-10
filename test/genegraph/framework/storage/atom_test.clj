(ns genegraph.framework.storage.atom-test
  (:require [genegraph.framework.storage :as storage]
            [genegraph.framework.protocol :as p]
            [genegraph.framework.storage.atom] ; loads defmethod for :atom-store
            [clojure.test :refer [deftest testing is]]))

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn make-store
  "Initialize an AtomStore and return it."
  [& {:as opts}]
  (p/init (merge {:name :test-store :type :atom-store} opts)))

(defn inst
  "Return the underlying atom instance from an AtomStore."
  [store]
  (storage/instance store))

;; ---------------------------------------------------------------------------
;; p/init
;; ---------------------------------------------------------------------------

(deftest init-test
  (testing "p/init returns an AtomStore record"
    (is (instance? genegraph.framework.storage.atom.AtomStore (make-store))))

  (testing "the instance starts as an empty map"
    (let [store (make-store)]
      (is (= {} @(inst store)))))

  (testing "satisfies HasInstance"
    (is (satisfies? storage/HasInstance (make-store))))

  (testing "satisfies Status"
    (is (satisfies? p/Status (make-store))))

  (testing "each init call produces an independent store"
    (let [a (make-store)
          b (make-store)]
      (storage/write (inst a) :k :v)
      (is (nil? (storage/read (inst b) :k))))))

;; ---------------------------------------------------------------------------
;; storage/write (2-arity) and storage/read
;; ---------------------------------------------------------------------------

(deftest write-and-read-test
  (testing "a written value can be read back"
    (let [db (inst (make-store))]
      (storage/write db :k :v)
      (is (= :v (storage/read db :k)))))

  (testing "reading a key that was never written returns nil"
    (let [db (inst (make-store))]
      (is (nil? (storage/read db :missing-key)))))

  (testing "writing to the same key overwrites the previous value"
    (let [db (inst (make-store))]
      (storage/write db :k :first)
      (storage/write db :k :second)
      (is (= :second (storage/read db :k)))))

  (testing "multiple distinct keys are stored independently"
    (let [db (inst (make-store))]
      (storage/write db :a 1)
      (storage/write db :b 2)
      (storage/write db :c 3)
      (is (= 1 (storage/read db :a)))
      (is (= 2 (storage/read db :b)))
      (is (= 3 (storage/read db :c)))))

  (testing "any Clojure value can be stored and retrieved"
    (let [db  (inst (make-store))
          val {:nested {:map [1 2 3]} :set #{:a :b}}]
      (storage/write db :complex val)
      (is (= val (storage/read db :complex)))))

  (testing "nil is a valid value to store"
    (let [db (inst (make-store))]
      (storage/write db :k nil)
      ;; The key must be present in the underlying map (not just absent with nil default)
      (is (true? (contains? @db :k)))
      (is (nil? (storage/read db :k))))))

;; ---------------------------------------------------------------------------
;; storage/write (3-arity, with commit-promise)
;; ---------------------------------------------------------------------------

(deftest write-with-commit-promise-test
  (testing "delivers commit-promise with true after writing"
    (let [db (inst (make-store))
          cp (promise)]
      (storage/write db :k :v cp)
      (is (= true (deref cp 500 :timeout)))))

  (testing "the value is stored before the promise is delivered"
    (let [db (inst (make-store))
          cp (promise)]
      (storage/write db :k :stored cp)
      @cp
      (is (= :stored (storage/read db :k)))))

  (testing "each write delivers its own independent promise"
    (let [db  (inst (make-store))
          cp1 (promise)
          cp2 (promise)]
      (storage/write db :k1 :v1 cp1)
      (storage/write db :k2 :v2 cp2)
      (is (= true (deref cp1 500 :timeout)))
      (is (= true (deref cp2 500 :timeout))))))

;; ---------------------------------------------------------------------------
;; storage/delete (2-arity)
;; ---------------------------------------------------------------------------

(deftest delete-test
  (testing "a deleted key is no longer readable"
    (let [db (inst (make-store))]
      (storage/write db :k :v)
      (storage/delete db :k)
      (is (nil? (storage/read db :k)))))

  (testing "deleting a key removes it from the underlying map"
    (let [db (inst (make-store))]
      (storage/write db :k :v)
      (storage/delete db :k)
      (is (not (contains? @db :k)))))

  (testing "deleting a non-existent key is a no-op"
    (let [db (inst (make-store))]
      (is (some? (storage/delete db :never-written)))))

  (testing "deleting one key does not affect other keys"
    (let [db (inst (make-store))]
      (storage/write db :a 1)
      (storage/write db :b 2)
      (storage/delete db :a)
      (is (nil? (storage/read db :a)))
      (is (= 2 (storage/read db :b))))))

;; ---------------------------------------------------------------------------
;; storage/delete (3-arity, with commit-promise)
;; ---------------------------------------------------------------------------

(deftest delete-with-commit-promise-test
  (testing "delivers commit-promise with true after deleting"
    (let [db (inst (make-store))
          cp (promise)]
      (storage/write db :k :v)
      (storage/delete db :k cp)
      (is (= true (deref cp 500 :timeout)))))

  (testing "the key is removed before the promise is delivered"
    (let [db (inst (make-store))
          cp (promise)]
      (storage/write db :k :v)
      (storage/delete db :k cp)
      @cp
      (is (nil? (storage/read db :k)))))

  (testing "delivers promise even when deleting a non-existent key"
    (let [db (inst (make-store))
          cp (promise)]
      (storage/delete db :never-written cp)
      (is (= true (deref cp 500 :timeout))))))

;; ---------------------------------------------------------------------------
;; p/status
;; ---------------------------------------------------------------------------

(deftest status-test
  (testing "status includes the store :name"
    (let [store (make-store :name :my-store)]
      (is (= :my-store (:name (p/status store))))))

  (testing "status reports :status :running immediately after init"
    (let [store (make-store)]
      (is (= :running (:status (p/status store))))))

  (testing "status :count is 0 for an empty store"
    (let [store (make-store)]
      (is (= 0 (:count (p/status store))))))

  (testing "status :count reflects the number of written keys"
    (let [store (make-store)
          db    (inst store)]
      (storage/write db :a 1)
      (storage/write db :b 2)
      (storage/write db :c 3)
      (is (= 3 (:count (p/status store))))))

  (testing "status :count decreases after a key is deleted"
    (let [store (make-store)
          db    (inst store)]
      (storage/write db :a 1)
      (storage/write db :b 2)
      (storage/delete db :a)
      (is (= 1 (:count (p/status store))))))

  (testing "overwriting a key does not increase :count"
    (let [store (make-store)
          db    (inst store)]
      (storage/write db :k :v1)
      (storage/write db :k :v2)
      (is (= 1 (:count (p/status store)))))))
