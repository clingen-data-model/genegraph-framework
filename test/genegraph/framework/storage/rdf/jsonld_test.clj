(ns genegraph.framework.storage.rdf.jsonld-test
  (:require [genegraph.framework.storage.rdf.jsonld :as jsonld]
            [charred.api :as json]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest testing is]])
  (:import [org.apache.jena.riot RDFParser Lang]
           [org.apache.jena.query Dataset]
           [org.apache.jena.rdf.model Model]
           [com.apicatalog.jsonld.document JsonDocument]
           [jakarta.json JsonArray JsonObject]
           [java.io File StringReader]))

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn parse
  "Parse string s in the given Jena Lang into a Model."
  [^String s ^Lang lang]
  (.toModel (RDFParser/fromString s lang)))

(defn ttl->model [s] (parse s Lang/TURTLE))

(defn json-ld->model [s] (parse s Lang/JSONLD))

(defn json-ld->named-model
  "Parse JSON-LD string s as a dataset and return the named graph graph-name."
  [^String s ^String graph-name]
  (let [^Dataset ds (.toDataset (RDFParser/fromString s Lang/JSONLD))]
    (.getNamedModel ds graph-name)))

(defn json-str->doc [s]
  (JsonDocument/of (StringReader. s)))

(defn round-trip
  "Serialize model to JSON-LD using frame-str, then parse it back into a Model."
  [^Model model frame-str]
  (-> model
      (jsonld/model->json-ld (json-str->doc frame-str))
      json-ld->model))

(defn isomorphic? [^Model a ^Model b]
  (.isIsomorphicWith a b))

(def prefixes
  "@prefix ex: <http://example.org/> .
   @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n")

(def library-ttl
  (str prefixes
       "ex:book1 a ex:Book ;
          ex:title \"Dune\" ;
          ex:pages 412 ;
          ex:published \"1965-08-01\"^^xsd:date ;
          ex:author ex:herbert .
        ex:herbert a ex:Person ;
          ex:name \"Frank Herbert\" ."))

(def book-frame
  "{\"@context\": {\"ex\": \"http://example.org/\"},
    \"@type\": \"ex:Book\"}")

;; ---------------------------------------------------------------------------
;; model->json-array
;; ---------------------------------------------------------------------------

(deftest model->json-array-test
  (let [arr (jsonld/model->json-array (ttl->model library-ttl))]
    (testing "returns expanded JSON-LD as a JsonArray"
      (is (instance? JsonArray arr)))

    (testing "expanded JSON-LD parses back to an isomorphic named graph"
      (is (isomorphic? (ttl->model library-ttl)
                       (json-ld->named-model (str arr) "http://example/"))))

    (testing "uses native JSON types for numbers"
      (is (re-find #"\{\"@value\":412\}" (str arr))))))

;; ---------------------------------------------------------------------------
;; model->json-ld round trips
;; ---------------------------------------------------------------------------

(deftest model->json-ld-round-trip-test
  (testing "embedded resources, typed literals and native numbers survive"
    (let [model (ttl->model library-ttl)]
      (is (isomorphic? model (round-trip model book-frame)))))

  (testing "framed output embeds linked resources under the framed root"
    (let [out (-> (ttl->model library-ttl)
                  (jsonld/model->json-ld (json-str->doc book-frame))
                  json/read-json)]
      (is (= "ex:book1" (get out "@id")))
      (is (= "Frank Herbert" (get-in out ["ex:author" "ex:name"])))
      (is (= 412 (get out "ex:pages")))))

  (testing "terms defined in the frame context compact and expand back"
    (let [model (ttl->model library-ttl)
          frame "{\"@context\": {\"@vocab\": \"http://example.org/\",
                                 \"ex\": \"http://example.org/\",
                                 \"author\": {\"@type\": \"@id\"}},
                  \"@type\": \"Book\"}"
          out (json/read-json (jsonld/model->json-ld model (json-str->doc frame)))]
      (is (= "Dune" (get out "title")))
      (is (isomorphic? model (round-trip model frame)))))

  (testing "multiple values for a property survive"
    (let [model (ttl->model
                 (str prefixes
                      "ex:book1 a ex:Book ;
                         ex:keyword \"sand\", \"spice\", \"worms\" ;
                         ex:author ex:a, ex:b .
                       ex:a ex:name \"A\" .
                       ex:b ex:name \"B\" ."))]
      (is (isomorphic? model (round-trip model book-frame)))))

  (testing "blank nodes survive"
    (let [model (ttl->model
                 (str prefixes
                      "ex:book1 a ex:Book ;
                         ex:edition [ ex:year 1965 ; ex:publisher \"Chilton\" ] ."))]
      (is (isomorphic? model (round-trip model book-frame)))))

  (testing "boolean and canonical double literals survive"
    (let [model (ttl->model
                 (str prefixes
                      "ex:book1 a ex:Book ;
                         ex:inPrint true ;
                         ex:rating \"4.5E0\"^^xsd:double ."))]
      (is (isomorphic? model (round-trip model book-frame)))))

  (testing "native types canonicalize non-canonical double lexical forms"
    (let [model (ttl->model
                 (str prefixes
                      "ex:book1 a ex:Book ; ex:rating \"4.5\"^^xsd:double ."))]
      (is (isomorphic? (ttl->model
                        (str prefixes
                             "ex:book1 a ex:Book ; ex:rating \"4.5E0\"^^xsd:double ."))
                       (round-trip model book-frame)))))

  (testing "frame selecting every Book includes all of them"
    (let [model (ttl->model
                 (str prefixes
                      "ex:book1 a ex:Book ; ex:title \"Dune\" .
                       ex:book2 a ex:Book ; ex:title \"Emma\" ."))]
      (is (isomorphic? model (round-trip model book-frame))))))

(deftest json-ld->model->json-ld-test
  (testing "a compacted JSON-LD document survives a trip through a Jena model"
    (let [doc "{\"@context\": {\"ex\": \"http://example.org/\"},
                \"@id\": \"ex:book1\",
                \"@type\": \"ex:Book\",
                \"ex:title\": \"Dune\",
                \"ex:pages\": 412,
                \"ex:author\": {\"@id\": \"ex:herbert\",
                               \"@type\": \"ex:Person\",
                               \"ex:name\": \"Frank Herbert\"}}"
          out (-> doc
                  json-ld->model
                  (jsonld/model->json-ld (json-str->doc book-frame))
                  json/read-json)]
      (is (= (dissoc (json/read-json doc) "@context")
             (dissoc out "@context"))))))

;; ---------------------------------------------------------------------------
;; Helpers for documents and serialization
;; ---------------------------------------------------------------------------

(deftest append-context-test
  (let [ctx (.asJsonObject
             (.get
              (.getJsonContent
               (json-str->doc "{\"ex\": \"http://example.org/\"}"))))
        arr (jsonld/model->json-array (ttl->model library-ttl))
        obj (jsonld/append-context arr ctx)]
    (testing "builds an object with @context and @graph"
      (is (instance? JsonObject obj))
      (is (= #{"@context" "@graph"} (set (keys obj)))))

    (testing "the resulting document parses back to an isomorphic model"
      (is (isomorphic? (ttl->model library-ttl)
                       (json-ld->named-model (jsonld/serialize-json-object obj)
                                             "http://example/"))))))

(deftest json-file->doc-test
  (let [f (File/createTempFile "frame" ".jsonld")]
    (try
      (spit f book-frame)
      (testing "a frame read from a file works like one read from a string"
        (let [model (ttl->model library-ttl)]
          (is (= (jsonld/model->json-ld model (json-str->doc book-frame))
                 (jsonld/model->json-ld model (jsonld/json-file->doc
                                               (io/file f)))))))
      (finally
        (.delete f)))))
