(ns genegraph.clinvar.xml
  (:require [clojure.java.io :as io]
            [clojure.data.xml :as xml]
            [clojure.zip :as zip]
            [clojure.data.zip.xml :as xz]
            
            [genegraph.framework.storage :as storage]
            [genegraph.framework.storage.rocksdb :as rocksdb])
  (:import [java.util.zip GZIPInputStream]))

;; TODO haplotypes

;; https://blog.korny.info/2014/03/08/xml-for-fun-and-profit.html

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

(defn sequence-location [n]
  {:for-display (some-> (xz/xml1-> n (xz/attr :forDisplay)) Boolean/parseBoolean)
   :assembly(xz/xml1-> n (xz/attr :Assembly))
   :chr (xz/xml1-> n (xz/attr :Chr))
   :accession (xz/xml1-> n (xz/attr :Accession))
   :outer-start (some-> (xz/xml1-> n (xz/attr :outerStart)) Integer/parseInt)
   :inner-start (some-> (xz/xml1-> n (xz/attr :innerStart)) Integer/parseInt)
   :start (some-> (xz/xml1-> n (xz/attr :start)) Integer/parseInt)
   :stop (some-> (xz/xml1-> n (xz/attr :stop)) Integer/parseInt)
   :inner-stop (some-> (xz/xml1-> n (xz/attr :innerStop)) Integer/parseInt)
   :outer-stop (some-> (xz/xml1-> n (xz/attr :outerStop)) Integer/parseInt)
   :display-start (some-> (xz/xml1-> n (xz/attr :display_start)) Integer/parseInt)
   :display-stop (some-> (xz/xml1-> n (xz/attr :display_stop)) Integer/parseInt)
   :strand (xz/xml1-> n (xz/attr :Strand))
   :variant-length (some-> (xz/xml1-> n (xz/attr :variantLength)) Integer/parseInt)
   :reference-allele (xz/xml1-> n (xz/attr :referenceAllele))
   :alternate-allele (xz/xml1-> n (xz/attr :alternateAllele))
   :assembly-accession-version (xz/xml1-> n (xz/attr :AssemblyAccessionVersion))
   :assembly-status (xz/xml1-> n (xz/attr :AssemblyStatus))
   :position-vcf (some-> (xz/xml1-> n (xz/attr :positionVCF)) Integer/parseInt)
   :reference-allele-vcf (xz/xml1-> n (xz/attr :referenceAlleleVCF))
   :alternate-allele-vcf (xz/xml1-> n (xz/attr :alternateAlleleVCF))
   :for-display-length (some-> (xz/xml1-> n (xz/attr :forDisplayLength))
                               Integer/parseInt)})

;; IncludedRecord vs InterpretedRecord -- first used for haplotype members
;; https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=clinvar&rettype=vcv&is_variationid&id=1727010
;; Has 'interpreted variation list'

;; Haplotype could be feasible
;; Do we really want anything to do with Genotype

(defn simple-allele [n]
  {:id (xz/xml1-> n :SimpleAllele (xz/attr :VariationID))
   ;; copy number for CNVs only exists in Clinical Assertion, for some reason
   ;; Anticipate only one of these existing, or being relevant
   :copy-number (some-> (xz/xml1-> n
                                   :ClinicalAssertionList
                                   :ClinicalAssertion
                                   :SimpleAllele
                                   :AttributeSet
                                   :Attribute
                                   (xz/attr= :Type "AbsoluteCopyNumber")
                                   xz/text)
                        Integer/parseInt)
   :reference-copy-number (some-> (xz/xml1-> n
                                   :ClinicalAssertionList
                                   :ClinicalAssertion
                                   :SimpleAllele
                                   :AttributeSet
                                   :Attribute
                                   (xz/attr= :Type "ReferenceCopyNumber")
                                   xz/text)
                        Integer/parseInt)
   :name (xz/xml1-> n
                    :SimpleAllele
                    :Name
                    xz/text)
   :spdi (xz/xml1-> n
                    :SimpleAllele
                    :CanonicalSPDI
                    xz/text)
   :caid (xz/xml1-> n
                    :SimpleAllele
                    :XRefList
                    :XRef
                    (xz/attr= :DB "ClinGen")
                    (xz/attr :ID))
   :hgvs (into []
          (xz/xml-> n
                    :SimpleAllele
                    :HGVSlist
                    :HGVS
                    :NucleotideExpression
                    :Expression
                    xz/text))
   :genes (into []
           (xz/xml-> n
                     :SimpleAllele
                     :GeneList
                     :Gene
                     (xz/attr :GeneID)))
   :sequence-location (mapv sequence-location
                           (xz/xml-> n
                                     :SimpleAllele
                                     :Location
                                     :SequenceLocation))
   :type (xz/xml1-> n
                    :SimpleAllele
                    :VariantType
                    xz/text)})

(defn haplotype [n]
  {:type "Haplotype"
   :id (xz/xml1-> n :Haplotype (xz/attr :VariationID))
   :alleles (mapv simple-allele
                  (xz/xml-> n :Haplotype :SimpleAllele))})

(defn genotype [n]
  {:type "Genotype"
   :id (xz/xml1-> n :Genotype (xz/attr :VariationID))})

(defn variation [n]
  (cond (xz/xml1-> n :SimpleAllele) (simple-allele n)
        (xz/xml1-> n :Haplotype) (haplotype n)
        (xz/xml1-> n :Genotype) (genotype n)))


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
  {:variation (variation
               (or (xz/xml1-> n :InterpretedRecord)
                   (xz/xml1-> n :IncludedRecord)))
   :assertions (assertions n)})

;; ClinVar XML can't be parsed with the default Java security settings
;; default is 5*10^7, increasing to 5*10^8
(-> (System/getProperties) (.setProperty "jdk.xml.totalEntitySizeLimit" "500000000"))

(defonce clinrocks (rocksdb/open "/users/tristan/data/clinrocks"))

#_(rocksdb/close clinrocks)

(defn write-clinvar-record-to-rocksdb [db xml-vcv]
  (let [vcv (-> xml-vcv zip/xml-zip vcv)]
    (storage/write db (get-in vcv [:variation :id]) vcv)))

(comment

  (first (rocksdb/entire-db-seq clinrocks))

  
  (def non-spdi-ep-variants
    (->> (rocksdb/entire-db-seq clinrocks)
         #_(take (* 1000 100))
         #_(remove #(get-in % [:variation :type]))
         #_(filter #(= "copy number loss" (get-in % [:variation :type])))
         #_(filter #(= "Duplication" (get-in % [:variation :type])))
         #_(remove #(or (get-in % [:variation :copy-number])
                        (get-in % [:variation :spdi])
                        (not (some-> %
                                     :variation
                                     :sequence-location
                                     first
                                     :reference-allele-vcf))))
         (filter (fn [a] (some #(= "reviewed by expert panel" (:review-status %))
                               (:assertions a))))
         (remove #(get-in % [:variation :spdi]))
         #_(mapcat #(map :review-status (:assertions %)))
         #_first
         #_clojure.pprint/pprint
         #_(map #(get-in % [:variation :copy-number]))
         #_frequencies
         (into [])))

  (->> non-spdi-ep-variants
       #_(map #(get-in % [:variation :type]))
       (map #(some-> % :variation :sequence-location first :variant-length))
       (filter #(and % (< % 100)))
       count
       #_(filter #(= "single nucleotide variant" (get-in % [:variation :type])))
       #_last
       #_clojure.pprint/pprint))

(comment
  (time
   (with-open [clinvar-stream (GZIPInputStream.
                               (io/input-stream clinvar-path))]
     (println "loading clinvar")
     (run! #(write-clinvar-record-to-rocksdb clinrocks %)
           (:content (xml/parse clinvar-stream)))))

  (count (rocksdb/entire-db-seq clinrocks))

  (first (rocksdb/entire-db-seq clinrocks))
  )


#_(first (rocksdb/entire-db-seq clinrocks))

#_(time (count (rocksdb/entire-db-seq clinrocks)))

(comment
  (with-open [clinvar-stream (GZIPInputStream.
                              (io/input-stream clinvar-path))]
    (->> (:content (xml/parse clinvar-stream))
         (take (* 1000 10))
         (map zip/xml-zip)
         (map vcv)
         #_(remove #(get-in % [:variation :spdi]))
         #_(filter #(= "copy number loss" (get-in % [:variation :type])))
         #_(filter #(seq (get-in % [:variation :hgvs])))
         #_(map #(get-in % [:variation :type]))
         #_count
         #_frequencies
         (take 1)
         (into []))))

(comment
  (with-open [clinvar-stream (GZIPInputStream.
                              (io/input-stream clinvar-path))]
    (->> (:content (xml/parse clinvar-stream))
         (take (* 1000 10))
         (map zip/xml-zip)
         #_(filter #(xz/xml1-> %
                               :InterpretedRecord
                               :SimpleAllele
                               :Location
                               :SequenceLocation
                               (xz/attr= :Assembly "GRCh38")))
         (filter #(xz/xml1-> %
                             :InterpretedRecord
                             :Genotype))
         #_(remove #(xz/xml1-> %
                               :InterpretedRecord
                               :SimpleAllele
                               :Location
                               :SequenceLocation
                               (xz/attr= :Assembly "GRCh37")))
         #_(filter #(xz/xml1-> %
                               :InterpretedRecord
                               :SimpleAllele
                               :VariantType
                               (xz/text= "single nucleotide variant")))
         
         #_(map #(xz/xml1-> %
                            :InterpretedRecord
                            :Haplotype
                            (xz/attr :VariationID)))
         (map vcv)
         
         first
         ))
  )

#_res
