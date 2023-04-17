(ns genegraph.gene-validity.deserialize
  (:require [genegraph.framework.storage.rdf :as rdf]
            [clojure.string :as s]
            [clojure.walk :refer [postwalk]]
            [clojure.edn :as edn]
            [cheshire.core :as json])
  (:import [java.io ByteArrayInputStream]))

(def base "http://dataexchange.clinicalgenome.org/gci/")
(def legacy-report-base "http://dataexchange.clinicalgenome.org/gci/legacy-report_")
(def affbase "http://dataexchange.clinicalgenome.org/agent/")

(def ns-prefixes {"dx" "http://dataexchange.clinicalgenome.org/"
                  "sepio" "http://purl.obolibrary.org/obo/SEPIO_"
                  "hp" "http://purl.obolibrary.org/obo/HP_"
                  "dc" "http://purl.org/dc/terms/"
                  "rdfs" "http://www.w3.org/2000/01/rdf-schema#"
                  "dxgci" "http://dataexchange.clinicalgenome.org/gci/"
                  "ro" "http://purl.obolibrary.org/obo/RO_"
                  "mondo" "http://purl.obolibrary.org/obo/MONDO_"
                  "car" "http://reg.genome.network/allele/"
                  "cv" "https://www.ncbi.nlm.nih.gov/clinvar/variation/"
                  "hgnc" "https://identifiers.org/hgnc:"
                  "pmid" "https://pubmed.ncbi.nlm.nih.gov/"
                  "geno" "http://purl.obolibrary.org/obo/GENO_"})

(def context
  (s/join ""
        (drop-last
         (json/generate-string
          {"@context" 
           {
            ;; frontmatter
            "@vocab" "http://dataexchange.clinicalgenome.org/gci/"
            "@base" "http://dataexchange.clinicalgenome.org/gci/"

            "PK" "@id"
            "item_type" "@type"
            "uuid" "@id"

            "gci" "http://dataexchange.clinicalgenome.org/gci/"
            "gcixform" "http://dataexchange.clinicalgenome.org/gcixform/"

            ;; ;; common prefixes
            "HGNC" "https://identifiers.org/hgnc:"
            "MONDO" "http://purl.obolibrary.org/obo/MONDO_"
            "SEPIO" "http://purl.obolibrary.org/obo/SEPIO_"
            "GENO" "http://purl.obolibrary.org/obo/GENO_"
            "NCIT" "http://purl.obolibrary.org/obo/NCIT_"
            "HP" {"@id" "http://purl.obolibrary.org/obo/HP_"
                  "@prefix" true}
            ;; ;; declare attributes with @id, @vocab types
            "hgncId" {"@type" "@id"}

            "autoClassification" {"@type" "@vocab"}
            "alteredClassification" {"@type" "@vocab"}
            "hpoIdInDiagnosis" {"@type" "@id"}
            "diseaseId" {"@type" "@id"}
            "caseInfoType" {"@type" "@id"}
            "variantType" {"@type" "@id"}
            "caseControl" {"@type" "@id"}
            "affiliation" {"@type" "@id"}
            ;; "experimental_scored" {"@type" "@id"}
            ;; "caseControl_scored" {"@type" "@id"}
            ;; "variants" {"@type" "@id"}

            "modelSystemsType" {"@type" "@vocab"}
            "evidenceType" {"@type" "@vocab"}
            "functionalAlterationType" {"@type" "@vocab"}
            "rescueType" {"@type" "@vocab"}
            "studyType" {"@type" "@vocab"}
            "sequencingMethod" {"@type" "@vocab"}
            "authors" {"@container" "@list"}
            "recessiveZygosity" {"@type" "@vocab"}
            "sopVersion" {"@type" "@vocab"}
            "sex" {"@type" "@vocab"}
            "ethnicity" {"@type" "@vocab"}
            "ageType" {"@type" "@vocab"}
            "ageUnit" {"@type" "@vocab"}
            "scoreStatus" {"@type" "@vocab"}
            "interactionType" {"@type" "@vocab"}
            "probandIs" {"@type" "@vocab"}
            "genotypingMethods" {"@container" "@list"}

            ;; ;; Category names
            "Model Systems" "gcixform:ModelSystems"
            "Functional Alteration" "gcixform:FunctionalAlteration"
            "Case control" "gcixform:CaseControl"

            ;; Case control
            "Aggregate variant analysis" "gcixform:AggregateVariantAnalysis"
            "Single variant analysis" "gcixform:SingleVariantAnalysis"

            ;; segregation
            ;; "Candidate gene sequencing" "gcixform:CandidateGeneSequencing"
            ;; "Exome/genome or all genes sequenced in linkage region" "gcixform:ExomeSequencing"

            "Candidate gene sequencing" "http://purl.obolibrary.org/obo/SEPIO_0004543"
            "Exome genome or all genes sequenced in linkage region" "http://purl.obolibrary.org/obo/SEPIO_0004541"

            ;; Experimental evidence types
            "Expression" "gcixform:Expression"
            "Biochemical Function" "gcixform:BiochemicalFunction"
            "Protein Interactions" "gcixform:ProteinInteraction"

            ;; rescue
            "Cell culture" "gcixform:CellCulture"
            "Non-human model organism" "gcixform:NonHumanModel"
            "Patient cells" "gcixform:PatientCells"
            "Human" "gcixform:Human"

            ;; model systems
            "Cell culture model" "gcixform:CellCultureModel"

            ;; functional alteration
            "Non-patient cells" "gcixform:NonPatientCells"
            "patient cells" "gcixform:PatientCells"

            ;; ;; evidence strength
            "No Modification" "gcixform:NoModification"
            "Definitive" "http://purl.obolibrary.org/obo/SEPIO_0004504"
            "Strong" "http://purl.obolibrary.org/obo/SEPIO_0004505"
            "Moderate" "http://purl.obolibrary.org/obo/SEPIO_0004506"
            "Limited" "http://purl.obolibrary.org/obo/SEPIO_0004507"
            "No Known Disease Relationship" "http://purl.obolibrary.org/obo/SEPIO_0004508"
            "No Reported Evidence" "http://purl.obolibrary.org/obo/SEPIO_0004508" ;; investigate the use of this
            "Refuted" "http://purl.obolibrary.org/obo/SEPIO_0004510"
            "Disputed" "http://purl.obolibrary.org/obo/SEPIO_0004540"
            "No Classification" "http://purl.obolibrary.org/obo/SEPIO_0004508" ;; Maybe this should not exist in published records?
            ;; "No Classification" "http://purl.obolibrary.org/obo/SEPIO_0004508"

            ;; Zygosity
            "Homozygous" "http://purl.obolibrary.org/obo/GENO_0000136"
            "TwoTrans" "http://purl.obolibrary.org/obo/GENO_0000135"
            "Hemizygous" "http://purl.obolibrary.org/obo/GENO_0000134"

            ;; SOP versions
            "4" "http://purl.obolibrary.org/obo/SEPIO_0004092"
            "5" "http://purl.obolibrary.org/obo/SEPIO_0004093"
            "6" "http://purl.obolibrary.org/obo/SEPIO_0004094"
            "7" "http://purl.obolibrary.org/obo/SEPIO_0004095"
            "8" "http://purl.obolibrary.org/obo/SEPIO_0004096"
            "9" "http://purl.obolibrary.org/obo/SEPIO_0004171"


            ;; Sex
            "Ambiguous" "http://purl.obolibrary.org/obo/SEPIO_0004574"
            "Female" "http://purl.obolibrary.org/obo/SEPIO_0004575"
            "Intersex" "http://purl.obolibrary.org/obo/SEPIO_0004576"
            "Male" "http://purl.obolibrary.org/obo/SEPIO_0004578"
            ;; "Unknown" "http://purl.obolibrary.org/obo/SEPIO_0004570"

            ;; ethnicity
            "Hispanic or Latino" "http://purl.obolibrary.org/obo/SEPIO_0004568"
            "Not Hispanic or Latino" "http://purl.obolibrary.org/obo/SEPIO_0004569"
            "Unknown" "http://purl.obolibrary.org/obo/SEPIO_0004570"

            ;; ageType
            "Death" "http://purl.obolibrary.org/obo/SEPIO_0004562"
            "Diagnosis" "http://purl.obolibrary.org/obo/SEPIO_0004563"
            "Onset" "http://purl.obolibrary.org/obo/SEPIO_0004564"
            "Report" "http://purl.obolibrary.org/obo/SEPIO_0004565"
 
            ;; ageUnit
            "Days" "http://purl.obolibrary.org/obo/SEPIO_0004552"
            "Hours" "http://purl.obolibrary.org/obo/SEPIO_0004553"
            "Months" "http://purl.obolibrary.org/obo/SEPIO_0004554"
            "Weeks" "http://purl.obolibrary.org/obo/SEPIO_0004555"
            "Weeks gestation" "http://purl.obolibrary.org/obo/SEPIO_0004556" 
            "Years" "http://purl.obolibrary.org/obo/SEPIO_0004557"

            ;; scoreStatus
            "Contradicts" "http://purl.obolibrary.org/obo/SEPIO_0004581"
            "Review" "http://purl.obolibrary.org/obo/SEPIO_0004582"
            "Score" "http://purl.obolibrary.org/obo/SEPIO_0004583"
            "Supports" "http://purl.obolibrary.org/obo/SEPIO_0004584"
            "none" "http://purl.obolibrary.org/obo/SEPIO_0004585"

            ;; testingMethods
            "Chromosomal microarray" "http://purl.obolibrary.org/obo/SEPIO_0004591"
            "Denaturing gradient gel" "http://purl.obolibrary.org/obo/SEPIO_0004592"
            "Exome sequencing" "http://purl.obolibrary.org/obo/SEPIO_0004593"
            "Genotyping" "http://purl.obolibrary.org/obo/SEPIO_0004594"
            "High resolution melting" "http://purl.obolibrary.org/obo/SEPIO_0004595"
            "Homozygosity mapping" "http://purl.obolibrary.org/obo/SEPIO_0004596"
            "Linkage analysis" "http://purl.obolibrary.org/obo/SEPIO_0004597"
            "Next generation sequencing panels" "http://purl.obolibrary.org/obo/SEPIO_0004598"
            "Other" "http://purl.obolibrary.org/obo/SEPIO_0004599"
            "PCR" "http://purl.obolibrary.org/obo/SEPIO_0004600"
            "Restriction digest" "http://purl.obolibrary.org/obo/SEPIO_0004601"
            "SSCP" "http://purl.obolibrary.org/obo/SEPIO_0004602"
            "Sanger sequencing" "http://purl.obolibrary.org/obo/SEPIO_0004603"
            "Whole genome shotgun sequencing" "http://purl.obolibrary.org/obo/SEPIO_0004604"

            ;; variantType
            "OTHER_VARIANT_TYPE" "http://purl.obolibrary.org/obo/SEPIO_0004611"
            "PREDICTED_OR_PROVEN_NULL" "http://purl.obolibrary.org/obo/SEPIO_0004612"

            ;; interactionTypes
            "genetic interaction" "gcixform:GeneticInteraction"
            "negative genetic interaction" "gcixform:NegativeGeneticInteraction"
            "physical association" "gcixform:PhysicalAssociation"
            "positive genetic interaction" "gcixform:PositiveGeneticInteraction"

            ;; probandIs
            "Biallelic compound heterozygous" "http://purl.obolibrary.org/obo/GENO_0000402"
            "Biallelic homozygous" "http://purl.obolibrary.org/obo/GENO_0000136"
            "Monoallelic heterozygous"  "http://purl.obolibrary.org/obo/GENO_0000135" }}))))

(defn expand-affiliation-to-iri
  "Expand affiliation when a simple string field, to be an iri"
  [m]
  (if (and (map? m) (get m "affiliation"))
    (update m "affiliation" (fn [affiliation]
                              (if (coll? affiliation)
                                affiliation
                                (str affbase affiliation))))
    m))

(defn fix-hpo-ids [m]
  (if (and (map? m) (get m "hpoIdInDiagnosis"))
    (update m "hpoIdInDiagnosis" (fn [phenotypes]
                                   (mapv #(re-find #"HP:\d{7}" %)
                                         phenotypes)))
    m))

(defn clear-associated-snapshots [m]
  (if (map? m) (dissoc m "associatedClassificationSnapshots") m))

(defn remove-keys-when-empty
  "When element is a map, removes any keys with key names from 'keys' vector that
  has an empty value." 
  [element keys]
  (postwalk (fn [x] (if (map? x)
                      (->> (select-keys x keys)
                           (reduce (fn [coll [k v]]
                                     (if (empty? v) (conj coll k) coll))
                                   [])
                           (apply dissoc x))
                      x))
            element))

(defn preprocess-json
  "Walk GCI JSON prior to parsing as JSON-LD to clean up data."
  [gci-json]
  (->> (json/parse-string gci-json)
       (postwalk #(-> %
                      clear-associated-snapshots
                      fix-hpo-ids
                      expand-affiliation-to-iri
                      (remove-keys-when-empty ["geneWithSameFunctionSameDisease"
                                               "normalExpression"
                                               "scores"
                                               "carId"
                                               "clinvarVariantId"])))
       json/generate-string))

(defn fix-gdm-identifiers [gdm-json]
  (-> gdm-json
      (s/replace #"MONDO_" "http://purl.obolibrary.org/obo/MONDO_")
      ;; New json-ld parser doesn't like '/' or parenthesis in terms 
      (s/replace #"Exome/genome or all genes sequenced in linkage region"
                 "Exome genome or all genes sequenced in linkage region")
      ;; these are the interactionType MI codes only -  MI codes are used
      ;; in at least one other field in the json. Removing the MI code
      ;; completely as we are not preserving the actual interactionType
      (s/replace #" \(MI:0208\)| \(MI:0915\)| \(MI:0933\)| \(MI:0935\)" "")
      (s/replace #"@id" "gciid")))

(defn append-context [gdm-json]
  (str context "," (subs gdm-json 1)))

(defn deserialize [event]
  (assoc event
         :data
         (-> (:value event)
             preprocess-json
             fix-gdm-identifiers
             append-context
             .getBytes
             ByteArrayInputStream.
             (rdf/read-rdf {:format :json-ld}))))

(comment
 (-> "/Users/tristan/data/genegraph/2023-01-17T1950/events/:gci-raw-missing-data/1ec53217-814e-44b3-a7b7-0f18311c20f3.json.edn"
     slurp
     edn/read-string
     (update-keys #(keyword (name %)))
     deserialize
     keys))
