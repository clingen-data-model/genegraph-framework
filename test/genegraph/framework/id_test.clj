(ns genegraph.framework.id-test
  (:require [genegraph.framework.id :as id]
            [genegraph.framework.storage.rdf.names :as names]
            [clojure.string :as str]
            [clojure.test :refer [deftest testing is use-fixtures]])
  (:import [java.util Arrays]))

;; ---------------------------------------------------------------------------
;; Test-local types and prefix setup
;; ---------------------------------------------------------------------------

;; All types used in this namespace are namespaced under :id-test/ to avoid
;; colliding with types registered by other code in the same JVM.

(def widget-type :id-test/Widget)
(def gadget-type :id-test/Gadget)
(def container-type :id-test/Container)

(defn- setup-test-types! []
  (names/add-prefixes {"id-test" "https://id-test.example.org/"})
  (id/register-type {:type widget-type
                     :defining-attributes [:id-test/color :id-test/size]})
  (id/register-type {:type gadget-type
                     :defining-attributes [:id-test/label :id-test/count]})
  (id/register-type {:type container-type
                     :defining-attributes [:id-test/widget :id-test/tag]}))

(use-fixtures :once (fn [f] (setup-test-types!) (f)))

;; ---------------------------------------------------------------------------
;; attr->hashable-primitive
;; ---------------------------------------------------------------------------

(deftest string-hashable-primitive-test
  (testing "String passes through unchanged"
    (is (= "hello" (id/attr->hashable-primitive "hello"))))

  (testing "Empty string passes through"
    (is (= "" (id/attr->hashable-primitive ""))))

  (testing "Returns the identical object"
    (let [s "same"]
      (is (identical? s (id/attr->hashable-primitive s))))))

(deftest long-hashable-primitive-test
  (testing "Long passes through unchanged"
    (is (= 42 (id/attr->hashable-primitive (long 42)))))

  (testing "Zero passes through"
    (is (= 0 (id/attr->hashable-primitive (long 0)))))

  (testing "Negative long passes through"
    (is (= -1 (id/attr->hashable-primitive (long -1))))))

(deftest keyword-hashable-primitive-test
  (testing "Keyword with registered prefix expands to IRI"
    ;; add-prefixes sets "id-test" -> "https://id-test.example.org/"
    (is (= "https://id-test.example.org/color"
           (id/attr->hashable-primitive :id-test/color))))

  (testing "Different keywords produce different IRI strings"
    (is (not= (id/attr->hashable-primitive :id-test/color)
              (id/attr->hashable-primitive :id-test/size))))

  (testing "Keyword with direct mapping uses the mapped IRI"
    (names/add-keyword-mappings {:id-test/special-kw "https://special.example.org/exact"})
    (is (= "https://special.example.org/exact"
           (id/attr->hashable-primitive :id-test/special-kw)))))

(deftest vector-hashable-primitive-test
  (testing "Vector returns a long array of length 2"
    (let [result (id/attr->hashable-primitive ["a" "b"])]
      (is (= (class (long-array 0)) (class result)))
      (is (= 2 (alength result)))))

  (testing "Same vector always returns the same hash"
    (is (Arrays/equals
         ^longs (id/attr->hashable-primitive ["a" "b"])
         ^longs (id/attr->hashable-primitive ["a" "b"]))))

  (testing "Different vectors return different hashes"
    (is (not (Arrays/equals
              ^longs (id/attr->hashable-primitive ["a" "b"])
              ^longs (id/attr->hashable-primitive ["b" "a"])))))

  (testing "Order matters in vectors"
    (is (not (Arrays/equals
              ^longs (id/attr->hashable-primitive [(long 1) (long 2)])
              ^longs (id/attr->hashable-primitive [(long 2) (long 1)])))))

  (testing "Vector of Longs works"
    (is (= 2 (alength (id/attr->hashable-primitive [(long 10) (long 20)])))))

  (testing "Nested vector works recursively"
    (is (= 2 (alength (id/attr->hashable-primitive [["x" "y"] "z"]))))))

(deftest map-hashable-primitive-test
  (testing "Map with :iri key returns the iri value directly"
    (is (= "http://example.org/thing"
           (id/attr->hashable-primitive {:iri "http://example.org/thing"}))))

  (testing "Map without :iri computes an IRI string via iri"
    (let [result (id/attr->hashable-primitive
                  {:type widget-type
                   :id-test/color "red"
                   :id-test/size (long 5)})]
      (is (string? result))
      (is (str/starts-with? result "https://genegraph.clinicalgenome.org/r/"))))

  (testing "Same map without :iri always returns the same IRI"
    (let [m {:type widget-type :id-test/color "red" :id-test/size (long 5)}]
      (is (= (id/attr->hashable-primitive m)
             (id/attr->hashable-primitive m))))))

;; ---------------------------------------------------------------------------
;; attr->hashable-primitive-capture-nil
;; ---------------------------------------------------------------------------

(deftest capture-nil-test
  (testing "nil returns the sentinel URI"
    (is (= "https://genegraph.clinicalgenome.org/r/nil"
           (id/attr->hashable-primitive-capture-nil nil))))

  (testing "non-nil String delegates to attr->hashable-primitive"
    (is (= "hello" (id/attr->hashable-primitive-capture-nil "hello"))))

  (testing "non-nil Long delegates to attr->hashable-primitive"
    (is (= (long 7) (id/attr->hashable-primitive-capture-nil (long 7)))))

  (testing "non-nil Keyword delegates to attr->hashable-primitive"
    (is (= (id/attr->hashable-primitive :id-test/color)
           (id/attr->hashable-primitive-capture-nil :id-test/color)))))

;; ---------------------------------------------------------------------------
;; hash-buffer-length
;; ---------------------------------------------------------------------------

(deftest hash-buffer-length-test
  (testing "Long contributes exactly Long/BYTES (8) bytes"
    (is (= 8 (id/hash-buffer-length [(long 42)]))))

  (testing "String contributes its UTF-8 byte count"
    (is (= 5 (id/hash-buffer-length ["hello"]))))

  (testing "Empty string contributes 0 bytes"
    (is (= 0 (id/hash-buffer-length [""]))))

  (testing "Multiple elements sum their contributions"
    (is (= 13 (id/hash-buffer-length [(long 42) "hello"]))))

  (testing "long-array (nested vector hash) contributes 16 bytes"
    (is (= 16 (id/hash-buffer-length [(long-array 2)]))))

  (testing "Mixed Long, String, and long-array sum correctly"
    (is (= 29 (id/hash-buffer-length [(long 42) "hello" (long-array 2)]))))

  (testing "Empty sequence gives 0"
    (is (= 0 (id/hash-buffer-length [])))))

;; ---------------------------------------------------------------------------
;; attrs->hash
;; ---------------------------------------------------------------------------

(deftest attrs->hash-test
  (testing "Returns a long array of length 2"
    (let [result (id/attrs->hash ["hello"])]
      (is (= (class (long-array 0)) (class result)))
      (is (= 2 (alength result)))))

  (testing "Deterministic for the same inputs"
    (let [h1 (id/attrs->hash ["hello" (long 42)])
          h2 (id/attrs->hash ["hello" (long 42)])]
      (is (= (aget h1 0) (aget h2 0)))
      (is (= (aget h1 1) (aget h2 1)))))

  (testing "Different inputs produce different hashes"
    (let [h1 (id/attrs->hash ["hello"])
          h2 (id/attrs->hash ["world"])]
      (is (not (and (= (aget h1 0) (aget h2 0))
                    (= (aget h1 1) (aget h2 1)))))))

  (testing "Order of inputs matters"
    (let [h1 (id/attrs->hash ["a" "b"])
          h2 (id/attrs->hash ["b" "a"])]
      (is (not (and (= (aget h1 0) (aget h2 0))
                    (= (aget h1 1) (aget h2 1)))))))

  (testing "Works with only Longs"
    (is (= 2 (alength (id/attrs->hash [(long 1) (long 2)])))))

  (testing "Works with mixed String and Long"
    (is (= 2 (alength (id/attrs->hash ["x" (long 99)]))))))

;; ---------------------------------------------------------------------------
;; hash->id
;; ---------------------------------------------------------------------------

(deftest hash->id-test
  (testing "Returns a String"
    (is (string? (id/hash->id (long-array [0 0])))))

  (testing "Contains only base64url characters (no padding)"
    (let [id (id/hash->id (long-array [12345678 87654321]))]
      (is (re-matches #"[A-Za-z0-9_\-]+" id))
      (is (not (str/includes? id "=")))))

  (testing "Deterministic for the same input"
    (is (= (id/hash->id (long-array [100 200]))
           (id/hash->id (long-array [100 200])))))

  (testing "Different hashes produce different IDs"
    (is (not= (id/hash->id (long-array [1 0]))
              (id/hash->id (long-array [2 0])))))

  (testing "Encodes 16 bytes as 22 base64url characters (no padding)"
    ;; 16 bytes → ceil(16/3)*4 = 24, minus 2 padding chars = 22
    (is (= 22 (count (id/hash->id (long-array [0 0])))))))

;; ---------------------------------------------------------------------------
;; register-type / defining-attributes-for-type / defining-attributes
;; ---------------------------------------------------------------------------

(deftest register-type-test
  (testing "registered type can be retrieved by defining-attributes-for-type"
    (is (= [:id-test/color :id-test/size]
           (id/defining-attributes-for-type widget-type))))

  (testing "unregistered type returns nil"
    (is (nil? (id/defining-attributes-for-type :id-test/NonExistent))))

  (testing "re-registering a type with new attributes updates the registration"
    (let [temp-type :id-test/TempType]
      (id/register-type {:type temp-type :defining-attributes [:id-test/a]})
      (is (= [:id-test/a] (id/defining-attributes-for-type temp-type)))
      (id/register-type {:type temp-type :defining-attributes [:id-test/b]})
      (is (= [:id-test/b] (id/defining-attributes-for-type temp-type))))))

(deftest defining-attributes-test
  (testing "returns values for each defining attribute followed by :type"
    (let [obj {:type widget-type :id-test/color "red" :id-test/size (long 5)}
          result (id/defining-attributes obj)]
      (is (= "red"       (nth result 0)))
      (is (= (long 5)    (nth result 1)))
      (is (= widget-type (nth result 2)))))

  (testing ":type is appended as the last element"
    (let [obj {:type widget-type :id-test/color "blue" :id-test/size (long 10)}]
      (is (= widget-type (last (id/defining-attributes obj))))))

  (testing "missing defining attribute produces nil in that position"
    (let [obj {:type widget-type :id-test/color "green"}] ; :id-test/size missing
      (is (nil? (second (id/defining-attributes obj))))))

  (testing "extra keys beyond defining attributes are ignored"
    (let [base {:type widget-type :id-test/color "red" :id-test/size (long 5)}
          extra (assoc base :id-test/extra-key "ignored")]
      (is (= (id/defining-attributes base)
             (id/defining-attributes extra))))))

;; ---------------------------------------------------------------------------
;; iri
;; ---------------------------------------------------------------------------

(deftest iri-test
  (testing "returns a string"
    (is (string? (id/iri {:type widget-type
                          :id-test/color "red"
                          :id-test/size (long 5)}))))

  (testing "result starts with the base IRI prefix"
    (is (str/starts-with?
         (id/iri {:type widget-type :id-test/color "red" :id-test/size (long 5)})
         "https://genegraph.clinicalgenome.org/r/")))

  (testing "same object always produces the same IRI"
    (let [obj {:type widget-type :id-test/color "red" :id-test/size (long 5)}]
      (is (= (id/iri obj) (id/iri obj)))))

  (testing "objects differing in a defining attribute produce different IRIs"
    (is (not= (id/iri {:type widget-type :id-test/color "red"  :id-test/size (long 5)})
              (id/iri {:type widget-type :id-test/color "blue" :id-test/size (long 5)}))))

  (testing "objects differing only in non-defining attributes produce the same IRI"
    (let [base  {:type widget-type :id-test/color "red" :id-test/size (long 5)}
          extra (assoc base :id-test/weight (long 100))]
      (is (= (id/iri base) (id/iri extra)))))

  (testing "nil defining attribute is handled via sentinel and produces a stable IRI"
    (let [obj {:type widget-type :id-test/color nil :id-test/size (long 5)}]
      (is (= (id/iri obj) (id/iri obj)))))

  (testing "nil and a non-nil value produce different IRIs"
    (is (not= (id/iri {:type widget-type :id-test/color nil  :id-test/size (long 5)})
              (id/iri {:type widget-type :id-test/color "red" :id-test/size (long 5)}))))

  (testing "vector defining attribute is hashed, same vector gives same IRI"
    (let [obj {:type gadget-type
               :id-test/label "g1"
               :id-test/count [(long 1) (long 2)]}]
      (is (= (id/iri obj) (id/iri obj)))))

  (testing "different vector values produce different IRIs"
    (is (not= (id/iri {:type gadget-type :id-test/label "g1"
                       :id-test/count [(long 1) (long 2)]})
              (id/iri {:type gadget-type :id-test/label "g1"
                       :id-test/count [(long 2) (long 1)]}))))

  (testing "nested map attribute without :iri is recursively hashed"
    (let [inner {:type widget-type :id-test/color "red" :id-test/size (long 5)}
          outer {:type container-type
                 :id-test/widget inner
                 :id-test/tag "t1"}]
      (is (str/starts-with? (id/iri outer) "https://genegraph.clinicalgenome.org/r/"))))

  (testing "nested map attribute with :iri uses that IRI directly"
    (let [inner-with-iri {:iri "https://example.org/precomputed"}
          outer {:type container-type
                 :id-test/widget inner-with-iri
                 :id-test/tag "t1"}]
      (is (= (id/iri outer) (id/iri outer))))))

;; ---------------------------------------------------------------------------
;; random-iri
;; ---------------------------------------------------------------------------

(deftest random-iri-test
  (testing "returns a string"
    (is (string? (id/random-iri))))

  (testing "starts with the base IRI prefix"
    (is (str/starts-with? (id/random-iri) "https://genegraph.clinicalgenome.org/r/")))

  (testing "two successive calls produce different IRIs"
    (is (not= (id/random-iri) (id/random-iri))))

  (testing "result contains only valid IRI characters after the prefix"
    (let [suffix (subs (id/random-iri)
                       (count "https://genegraph.clinicalgenome.org/r/"))]
      (is (re-matches #"[A-Za-z0-9_\-]+" suffix)))))
