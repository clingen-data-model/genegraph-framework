(ns genegraph.clinvar.variation
  (:require [hato.client :as hc]
            [jsonista.core :as j]
            [next.jdbc :as jdbc]
            [clojure.string :as s]
            [clojure.walk :as walk])
  (:import [java.net URLEncoder]
           [java.security MessageDigest]
           [java.util Arrays Base64]))

(def the-var
  {:copy-number nil,
  :name "NM_019032.6(ADAMTSL4):c.794C>T (p.Pro265Leu)",
  :type "single nucleotide variant",
  :sequence-location
  #{{:alternate-allele nil,
     :display-start 150553785,
     :outer-start nil,
     :assembly-status "current",
     :alternate-allele-vcf "T",
     :inner-start nil,
     :for-display "true",
     :variant-length 1,
     :outer-stop nil,
     :strand nil,
     :start 150553785,
     :assembly-accession-version "GCF_000001405.38",
     :position-vcf 150553785,
     :reference-allele nil,
     :reference-allele-vcf "C",
     :for-display-length nil,
     :stop 150553785,
     :accession "NC_000001.11",
     :display-stop 150553785,
     :assembly "GRCh38",
     :inner-stop nil,
     :chr "1"}
    {:alternate-allele nil,
     :display-start 150526261,
     :outer-start nil,
     :assembly-status "previous",
     :alternate-allele-vcf "T",
     :inner-start nil,
     :for-display nil,
     :variant-length 1,
     :outer-stop nil,
     :strand nil,
     :start 150526261,
     :assembly-accession-version "GCF_000001405.25",
     :position-vcf 150526261,
     :reference-allele nil,
     :reference-allele-vcf "C",
     :for-display-length nil,
     :stop 150526261,
     :accession "NC_000001.10",
     :display-stop 150526261,
     :assembly "GRCh37",
     :inner-stop nil,
     :chr "1"}},
  :genes #{"54507" "100289061"},
  :hgvs
  #{"NC_000001.10:g.150526261C>T"
    "NM_001378596.1:c.794C>T"
    "NC_000001.11:g.150553785C>T"
    "NM_025008.5:c.794C>T"
    "NM_019032.6:c.794C>T"
    "NM_001288608.2:c.794C>T"
    "NG_012172.1:g.9364C>T"
    "NM_001288607.2:c.794C>T"},
  :caid nil,
  :id "1484067",
  :reference-copy-number nil,
   :spdi "NC_000001.11:150553784:C:T"})


(def the-cnv
  {:copy-number 1,
   :name "GRCh37/hg19 Xq11.1-28(chrX:61694576-155254881)x1",
   :type "copy number loss",
   :sequence-location
   #{{:alternate-allele nil,
      :display-start 61694576,
      :outer-start nil,
      :assembly-status "previous",
      :alternate-allele-vcf nil,
      :inner-start 61694576,
      :for-display "true",
      :variant-length 93560306,
      :outer-stop nil,
      :strand nil,
      :start nil,
      :assembly-accession-version "GCF_000001405.25",
      :position-vcf nil,
      :reference-allele nil,
      :reference-allele-vcf nil,
      :for-display-length nil,
      :stop nil,
      :accession "NC_000023.10",
      :display-stop 155254881,
      :assembly "GRCh37",
      :inner-stop 155254881,
      :chr "X"}},
   :genes ; truncating this for expediency
   #{"4515"
     "55285"
     "574515"
     "2258"},
   :hgvs #{"NC_000023.10:g.(?_61694576)_(155254881_?)del"},
   :caid nil,
   :id "616920",
   :reference-copy-number nil,
   :spdi nil})

(def the-copy-change
  {:copy-number nil,
  :name "Single allele",
  :type "Duplication",
  :sequence-location
  #{{:alternate-allele nil,
     :display-start 22528723,
     :outer-start nil,
     :assembly-status "previous",
     :alternate-allele-vcf nil,
     :inner-start nil,
     :for-display "true",
     :variant-length 771450,
     :outer-stop nil,
     :strand nil,
     :start 22528723,
     :assembly-accession-version "GCF_000001405.25",
     :position-vcf nil,
     :reference-allele nil,
     :reference-allele-vcf nil,
     :for-display-length nil,
     :stop 23300172,
     :accession "NC_000015.9",
     :display-stop 23300172,
     :assembly "GRCh37",
     :inner-stop nil,
     :chr "15"}},
  :genes #{"23191" "123606" "283767" "114791" "81614"},
  :hgvs #{"NC_000015.9:g.22528723_23300172dup"},
  :caid nil,
  :id "236397",
  :reference-copy-number nil,
  :spdi nil})


(def db {:dbtype "sqlite"
         :dbname "/Users/tristan/data/seqrepo/2021-01-29.4wk6r56e/aliases.sqlite3"
         #_#_:dbname "sqlite test"})

(def ds (jdbc/get-datasource db))

(defn seq-id [alias]
  (some->> (jdbc/execute-one! ds ["select seq_id from seqalias where alias = ? limit 1;" "NM_025008.5"])
           :seqalias/seq_id
           (str "ga4gh:SQ.")))

(def seq-id-memo (memoize seq-id))

(comment
  (time (seq-id-memo "NC_000001.11")))

(defn ^:private sha512t24u
  "Base64-encode the truncated SHA-512 digest of string S."
  [s]
  (-> (MessageDigest/getInstance "SHA-512")
      (.digest (.getBytes s))
      (Arrays/copyOf 24)
      (->> (.encodeToString (Base64/getUrlEncoder)))))

(def vrs-values-for-hash
  {"Allele" [:sequence :start :end :state]
   "CopyNumberCount" [:sequence :start :end :copies]
   "CopyNumberChange" [:sequence :start :end :copy_change]})

(def vrs-type-prefixes
  {"Allele" "VA"
   "CopyNumberCount" "VAB"
   "CopyNumberChange" "VAB"})

(defn value-seq-for-hash [vrs-value]
  (cond
    (string? vrs-value) vrs-value
    (number? vrs-value) (str vrs-value)
    (vector? vrs-value) (mapv value-for-hash vrs-value)
    (map? vrs-value) (reduce
                      (fn [a k]
                        (conj a (value-for-hash (get vrs-value k))))
                      []
                      (vrs-values-for-hash (:type vrs-value)))
    :else "NULL"))

(defn vrs-id [vrs-object]
  (str
   "ga4gh:"
   (vrs-type-prefixes (:type vrs-object))
   "."
   (sha512t24u (s/join " " (value-seq-for-hash vrs-object)))))

(defn spdi->vrs [spdi]
  (let [[sequence position-str deletion insertion] (s/split spdi #":")
        position (Integer/parseInt position-str)
        allele {:type "Allele"
                    :sequence (seq-id sequence)
                    :start position
                    :end (+ position (count deletion))
                    :state insertion}]
    (assoc allele :id (vrs-id allele))))

(-> (:spdi the-var)
    spdi->vrs)

(spdi->vrs "NC_000012.12:110282700::A")


;; TODO fordisplay should probably just be a boolean
(defn select-sequence-location [variant]
  (let [locations (:sequence-location variant)]
    (println locations)
    (or (first (filter :for-display locations))
        (first locations))))

(defn loc->coord-map [loc]
  {:sequence (seq-id (:accession loc))
   :start (or (:start loc)
              [(:outer-start loc)
               (:inner-start loc)])
   :end (or (:stop loc)
            [(:inner-stop loc)
             (:outer-stop loc)])})

(defn cnv->vrs [cnv]
  (assoc (loc->coord-map (select-sequence-location cnv))
         :type "CopyNumberCount"
         :copies (:copy-number cnv)))

(cnv->vrs the-cnv)

;; EFO:0030070 = copy number gain
;; EFO:0030067 = copy number loss

(def type->copy-change
  {"Duplication" "EFO:0030070"
   "Deletion" "EFO:0030067"
   "copy number gain" "EFO:0030070"
   "copy number loss" "EFO:0030067"})

(defn copy-change->vrs [copy-change]
  (assoc (loc->coord-map (select-sequence-location copy-change))
         :type "CopyNumberChange"
         :copy_change (type->copy-change (:type copy-change))))

(defn add-vrs-id [vrs-object]
  (assoc vrs-object :id (vrs-id vrs-object)))

(add-vrs-id (copy-change->vrs the-copy-change))



#_(def allele-registry "http://reg.genome.network/vrAllele?hgvs=NC_000010.11:g.87894077C>T")

#_(def allele-registy-post "http://reg.genome.network/alleles?file=id")

#_(def normalizer "https://normalization.clingen.app/variation/to_canonical_variation?q=NC_000007.14%3A4781212%3AGGAT%3ATGCTGTAAACTGTAACTGTAAA&fmt=spdi&do_liftover=false&hgvs_dup_del_mode=default&untranslatable_returns_text=true")
(comment
  (def hgvs-del "NC_000001.10:g.(?_955533)_(1249188_?)del")

  (def smaller-del "NC_000007.14:g.26196448_26197095del")
  (def another-small-del "NC_000007.14:g.117610519_117610669del")

  (-> (hc/get "https://normalization.clingen.app/variation/to_canonical_variation"
              {:query-params
               {:q another-small-del
                :fmt "hgvs"
                :do_liftover "false"
                :hgvs_dup_del_mode "default"
                :unstranslatable_returns_text "true"}})
      :body
      (j/read-value j/keyword-keys-object-mapper)
      clojure.pprint/pprint))

#_(j/read-value (:body (hc/get normalizer)) j/keyword-keys-object-mapper)

#_(def ar-result (hc/get allele-registry))

#_(def ar-bulk-result (hc/post (str allele-registy-post) {:body "CA215070\n" :accept :json}))

#_(clojure.pprint/pprint
 (j/read-value (:body ar-bulk-result)
               j/keyword-keys-object-mapper))


;;   :id "2011668", longish-del, no SPDI, weird output

