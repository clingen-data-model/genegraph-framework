(ns genegraph.clinvar.variation
  (:require [hato.client :as hc]
            [jsonista.core :as j])
  (:import [java.net URLEncoder]))

(def the-var
  {:id  "2"
   :spdi  "NC_000007.14:4781212:GGAT:TGCTGTAAACTGTAACTGTAAA"
   :caid  "CA215070"
   :hgvs  #{"NM_001364858.1:c.-202_-199delinsTGCTGTAAACTGTAACTGTAAA"
             "LRG_1247:g.10583_10586delinsTGCTGTAAACTGTAACTGTAAA"
             "NM_014855.3:c.80_83delinsTGCTGTAAACTGTAACTGTAAA"
             "NC_000007.14:g.4781213_4781216delinsTGCTGTAAACTGTAACTGTAAA"
             "NR_157345.1:n.173_176delinsTGCTGTAAACTGTAACTGTAAA"
             "NG_028111.p1:g.10583_10586delinsTGCTGTAAACTGTAACTGTAAA"
             "LRG_1247t1:c.80_83delinsTGCTGTAAACTGTAACTGTAAA"
             "NC_000007.13:g.4820844_4820847delinsTGCTGTAAACTGTAACTGTAAA"}
   :type  "Indel"})

(def allele-registry "http://reg.genome.network/vrAllele?hgvs=NC_000010.11:g.87894077C>T")

(def allele-registy-post "http://reg.genome.network/alleles?file=id")

(def normalizer "https://normalization.clingen.app/variation/to_canonical_variation?q=NC_000007.14%3A4781212%3AGGAT%3ATGCTGTAAACTGTAACTGTAAA&fmt=spdi&do_liftover=false&hgvs_dup_del_mode=default&untranslatable_returns_text=true")

(j/read-value (:body (hc/get normalizer)) j/keyword-keys-object-mapper)

(def ar-result (hc/get allele-registry))

(def ar-bulk-result (hc/post (str allele-registy-post) {:body "CA215070\n" :accept :json}))

(clojure.pprint/pprint
 (j/read-value (:body ar-bulk-result)
               j/keyword-keys-object-mapper))
