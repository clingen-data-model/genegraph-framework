(ns genegraph.framework.id
  "Functions for generating hash values to be used for identifying
  value objects that do not have a natural identifier of their own,
  among other purposes."
  (:require [genegraph.framework.storage.rdf.names :as names]
            [clojure.spec.alpha :as spec])
  (:import [net.openhft.hashing LongHashFunction]
           [com.google.common.primitives Longs]
           [java.nio ByteBuffer]
           [java.util Base64]))

;; ID algorithm
;; maps
;; A map will have a :type attribute (namespace?)
;; Each type should have registered a list of defining
;; attributes (can leverage spec for validation)
;; the defining attributes will be an ordered list.
;; the expectation is that all are required.
;; Object types must be namespaced. They should either
;; be namespaced keywords or IRIs. If an IRI, there
;; should be a curie or mapping defined in names.clj

;; vectors
;; Vectors will be condensed to a single hash used
;; for computing the id of a containing object,
;; when necessary (is it necessary now?) The ID of
;; each (non-primitive)
;; object contained in the vector will be computed,
;; each will be sorted lexographically / ordinally for
;; unordered sets, in-order for ordered vectors.
;; Vectors containing mixed types not supported for ID
;; purposes.

;; primitives
;; The only needed (or allowed) primitive types are
;; Strings, Longs, and IRIs. IRIs are the format for
;; identifiers in the system.

(defonce types (atom {}))

(defn register-type
  "Registers TYPE with the appropriate seq of
  DEFINING-ATTRIBUTES. Order is significant;
  attributes will be added to the byte buffer "
  [type defining-attributes]
  (swap! types assoc type defining-attributes))

;; relying on the vector of required attributes for a type
;; always being the third item in a type description.
;; should explore whether we can always rely on this
(defn defining-attributes-for-type [t]
  (get @types t))

(defn defining-attributes [o]
  (conj (mapv #(get o %) (defining-attributes-for-type (:type o)))
        (:type o)))

(defprotocol HashablePrimitive
  (attr->hashable-primitive [attr]))

(defn hash-buffer-length [hashable-attrs]
  (reduce (fn [a attr]
            (+ a (if (instance? Long attr)
                   Long/BYTES
                   (count (.getBytes attr)))))
          0
          hashable-attrs))

(defn attrs->hash [attrs]
  (let [bb (ByteBuffer/allocate (hash-buffer-length attrs))]
    (run! #(if (instance? Long %)
             (.putLong bb %)
             (.put bb (.getBytes %)))
          attrs)
    (.hashBytes (LongHashFunction/xx3) (.array bb))))

(defn hash->id [h]
  (.encodeToString
   (.withoutPadding (Base64/getUrlEncoder))
   (Longs/toByteArray h)))

(defn iri [o]
  (->> (defining-attributes o)
       (mapv attr->hashable-primitive)
       attrs->hash
       hash->id
       (str "https://genegraph.clingen.app/")))

(extend-type java.lang.String
  HashablePrimitive
  (attr->hashable-primitive [attr] attr))

(extend-type java.lang.Long
  HashablePrimitive
  (attr->hashable-primitive [attr] attr))

(extend-type clojure.lang.Keyword
  HashablePrimitive
  (attr->hashable-primitive [attr] (names/kw->iri attr)))

(extend-type clojure.lang.PersistentVector
  HashablePrimitive
  (attr->hashable-primitive [attr]
    (attrs->hash (mapv attr->hashable-primitive attr))))

(extend-type clojure.lang.IPersistentMap
  HashablePrimitive
  (attr->hashable-primitive [attr]
    (or (:iri attr)
        (iri attr))))



(comment

  (register-type :ga4gh/SequenceLocation
                 [:ga4gh/sequenceReference :ga4gh/start :ga4gh/end])

  (register-type :ga4gh/CopyNumberChange
                 [:ga4gh/location :ga4gh/copyChange])
  
  (names/add-prefixes {"ga4gh" "https://ga4gh.org/terms/"})

  (names/add-keyword-mappings
   {:efo/copy-number-loss "http://www.ebi.ac.uk/efo/EFO_0030067"
    :efo/copy-number-gain "http://www.ebi.ac.uk/efo/EFO_0030070"})

  (iri {:ga4gh/sequenceReference
        "https://identifiers.org/refseq:NC_000001.10"
        :ga4gh/start [100100 100110]
        :ga4gh/end [200102 200110]
        :type :ga4gh/SequenceLocation})

  (iri {:type :ga4gh/CopyNumberChange
        :ga4gh/copyChange :efo/copy-number-loss
        :ga4gh/location
        {:ga4gh/sequenceReference
         "https://identifiers.org/refseq:NC_000001.10"
         :ga4gh/start [100100 100110]
         :ga4gh/end [200102 200110]
         :type :ga4gh/SequenceLocation}})
  
  (spec/valid? :w3c/iri 3)

  (spec/valid? :ga4gh/SequenceLocation
               {:ga4gh/sequenceReference
                "https://identifiers.org/refseq:NC_000001.10"
                :ga4gh/start [100000 100100]
                :ga4gh/end [200000 200100]
                :type :ga4gh/SequenceLocation})

  (spec/describe :ga4gh/SequenceLocation)

  
  )
