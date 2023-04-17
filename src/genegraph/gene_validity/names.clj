(ns genegraph.gene-validity.names
  (:require [genegraph.framework.storage.rdf.names :as names :refer [add-prefixes add-keyword-mappings]]))

(add-keyword-mappings
 {:sepio/has-evidence "http://purl.obolibrary.org/obo/SEPIO_0000189"
  :sepio/has-subject "http://purl.obolibrary.org/obo/SEPIO_0000388"
  :sepio/has-predicate "http://purl.obolibrary.org/obo/SEPIO_0000389"
  :sepio/has-object "http://purl.obolibrary.org/obo/SEPIO_0000390"
  :sepio/has-qualifier "http://purl.obolibrary.org/obo/SEPIO_0000144"
  :sepio/GeneValidityProposition "http://purl.obolibrary.org/obo/SEPIO_0004001"
  :mondo/Disease "http://purl.obolibrary.org/obo/MONDO_0000001"
  :sepio/Assertion "http://purl.obolibrary.org/obo/SEPIO_0000001"
  :ro/IsCausalGermlineMutationIn "http://purl.obolibrary.org/obo/RO_0004013"})


