(ns genegraph.clinvar.xml
  (:require [clojure.java.io :as io]
            [clojure.data.xml :as xml]
            [clojure.zip :as zip]
            [clojure.data.zip.xml :as xz])
  (:import [java.util.zip GZIPInputStream]))

(def clinvar-path "/users/tristan/data/clinvar.xml.gz")

(defn id [n]
  (when n
    (str (xz/xml1-> n :ID (xz/attr :Source))
         ":"
         (xz/xml1-> n :ID xz/text))))

(defn citation [n]
  (when n
    (if-let [url (xz/xml1-> n :URL)]
      (xz/text url)
      (id n))))

(defn variation [n]
  {:id (xz/xml1-> n (xz/attr :VariationID))
   :spdi (xz/xml1-> n
                    :InterpretedRecord
                    :SimpleAllele
                    :CanonicalSPDI
                    xz/text)
   :caid (xz/xml1-> n
                    :InterpretedRecord
                    :SimpleAllele
                    :XRefList
                    :XRef
                    (xz/attr= :DB "ClinGen")
                    (xz/attr :ID))
   :hgvs (set
          (xz/xml-> n
                    :InterpretedRecord
                    :SimpleAllele
                    :HGVSlist
                    :HGVS
                    :NucleotideExpression
                    :Expression
                    xz/text))
   :type (xz/xml1-> n
                    :InterpretedRecord
                    :SimpleAllele
                    :VariantType
                    xz/text)})

(defn observed-in [n]
  {:origin (xz/xml1-> n
                      :Sample
                      :Origin
                      xz/text)
   :affected-status (xz/xml1-> n
                      :Sample
                      :AffectedStatus
                      xz/text)
   :method (xz/xml1-> n
                      :Method
                      :MethodType
                      xz/text)
   :description (xz/xml1-> n
                           :ObservedData
                           :Attribute
                           (xz/attr= :Type "Description")
                           xz/text)
   :citation (citation
              (xz/xml1-> n
                         :ObservedData
                         :Citation))})

(defn assertion [n]
  {:id (xz/xml1-> n
                  :ClinVarAccession
                  (xz/attr :Accession))
   :label (xz/xml1-> n
                     :ClinVarSubmissionID
                     (xz/attr :title))
   :submitter-id (xz/xml1-> n
                            :ClinVarAccession
                            (xz/attr :OrgID))
   :submitter-name (xz/xml1-> n
                              :ClinVarAccession
                              (xz/attr :SubmitterName))
   :version (xz/xml1-> n
                       :ClinVarAccession
                       (xz/attr :Version))
   :date-created (xz/xml1-> n
                            :ClinVarAccession
                            (xz/attr :DateCreated))
   :date-updated (xz/xml1-> n
                            :ClinVarAccession
                            (xz/attr :DateUpdated))
   :date-last-evaluated (xz/xml1-> n
                            :Interpretation
                            (xz/attr :DateLastEvaluated))
   :interpretation (xz/xml1-> n
                              :Interpretation
                              :Description
                              xz/text)
   :type (xz/xml1-> n
                    :Assertion
                    xz/text)
   :record-status (xz/xml1-> n
                             :RecordStatus
                             xz/text)
   :review-status (xz/xml1-> n
                            :ReviewStatus
                            xz/text)
   :description (xz/xml1-> n
                           :Interpretation
                           :Comment
                           xz/text)
   :citations (mapv citation
                    (xz/xml-> n :Citation))
   :observed-in (mapv observed-in
                      (xz/xml-> n
                                :ObservedInList
                                :ObservedIn))})

(defn assertions [n]
  (mapv
   assertion 
   (xz/xml-> n
             :InterpretedRecord
             :ClinicalAssertionList
             :ClinicalAssertion)))

(defn vcv [n]
  {:variation (variation n)
   :assertions (assertions n)})

;; ClinVar XML can't be parsed with the default Java security settings
;; default is 5*10^7, increasing to 5*10^8
(-> (System/getProperties) (.setProperty "jdk.xml.totalEntitySizeLimit" "500000000"))

(with-open [clinvar-stream (GZIPInputStream.
                            (io/input-stream clinvar-path))]
  (->> (:content (xml/parse clinvar-stream))
       (take (* 1000))
       (map zip/xml-zip)
       (map vcv)
       (take 1)
       (into [])))

